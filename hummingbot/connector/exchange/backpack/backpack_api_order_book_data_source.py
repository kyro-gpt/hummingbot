import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils
from hummingbot.connector.exchange.backpack.backpack_order_book import BackpackOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange


class BackpackAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DEPTH_STREAM_ID = 2

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BackpackExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.WS_TRADES_CHANNEL
        self._diff_messages_queue_key = CONSTANTS.WS_DEPTH_CHANNEL
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """
        Fetch the last traded prices for the given trading pairs.

        Args:
            trading_pairs: List of trading pairs to get prices for
            domain: Optional domain parameter

        Returns:
            Dictionary mapping trading pairs to their last traded prices
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        Args:
            trading_pair: The trading pair for which the order book will be retrieved

        Returns:
            The response from the exchange (JSON dictionary)
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {
            "symbol": symbol,
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and depth update events through the provided websocket connection.

        Args:
            ws: The websocket assistant used to connect to the exchange
        """
        try:
            # Subscribe to trade updates for all trading pairs
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

                # Subscribe to trades
                trade_payload = {
                    "method": "SUBSCRIBE",
                    "params": [f"trade.{symbol}"],  # Correct format: trade.SYMBOL
                    "id": 1
                }

                trade_request = WSJSONRequest(payload=trade_payload)
                await ws.send(trade_request)

                # Subscribe to order book depth updates
                depth_payload = {
                    "method": "SUBSCRIBE",
                    "params": [f"depth.{symbol}"],  # Correct format: depth.SYMBOL
                    "id": 2
                }

                depth_request = WSJSONRequest(payload=depth_payload)
                await ws.send(depth_request)

            self.logger().info("Subscribed to public order book and trade channels...")

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book and trade streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates and connects a websocket assistant instance.

        Returns:
            Connected websocket assistant
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.wss_url(CONSTANTS.WS_URL, self._domain),
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
        )
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Retrieves order book snapshot and converts it to OrderBookMessage.

        Args:
            trading_pair: The trading pair to get snapshot for

        Returns:
            OrderBookMessage containing the snapshot data
        """
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = BackpackOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses a trade message from the websocket and puts it in the message queue.

        Args:
            raw_message: Raw message from websocket
            message_queue: Queue to put the parsed message
        """
        try:

            # Backpack uses {"stream": "...", "data": {...}} wrapper structure
            if "stream" in raw_message and "data" in raw_message:
                stream = raw_message["stream"]
                data = raw_message["data"]

                # Check if this is a trade event
                if stream.startswith("trade.") and data.get("e") == "trade":
                    symbol = data["s"]
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                    trade_message = BackpackOrderBook.trade_message_from_exchange(
                        data,
                        {"trading_pair": trading_pair}
                    )
                    message_queue.put_nowait(trade_message)
        except Exception as e:
            self.logger().error(f"Error parsing trade message: {e}", exc_info=True)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses an order book diff message from the websocket and puts it in the message queue.

        Args:
            raw_message: Raw message from websocket
            message_queue: Queue to put the parsed message
        """
        try:

            # Backpack uses {"stream": "...", "data": {...}} wrapper structure
            if "stream" in raw_message and "data" in raw_message:
                stream = raw_message["stream"]
                data = raw_message["data"]

                # Check if this is a depth update event
                if stream.startswith("depth.") and data.get("e") == "depth":
                    symbol = data["s"]
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

                    order_book_message: OrderBookMessage = BackpackOrderBook.diff_message_from_exchange(
                        data,
                        time.time(),
                        {"trading_pair": trading_pair}
                    )
                    message_queue.put_nowait(order_book_message)
        except Exception as e:
            self.logger().error(f"Error parsing order book diff message: {e}", exc_info=True)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Determines which channel/queue the message should be routed to based on its content.

        Args:
            event_message: The websocket message

        Returns:
            The channel identifier for routing the message
        """

        channel = ""

        # Backpack uses {"stream": "...", "data": {...}} wrapper structure
        if "stream" in event_message:
            stream = event_message["stream"]
            if stream.startswith("trade."):
                channel = self._trade_messages_queue_key
            elif stream.startswith("depth."):
                channel = self._diff_messages_queue_key
            else:
                pass  # No other stream types are handled here
        else:
            pass  # No other message types are handled here

        return channel
