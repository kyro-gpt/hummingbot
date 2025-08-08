import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.exchange.backpack.backpack_order_book import BackpackOrderBook
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import (
        BackpackPerpetualDerivative,
    )


class BackpackPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    Order book data source for Backpack perpetual/derivative trading.

    Inherits from PerpetualAPIOrderBookDataSource and adds funding rate support via REST API polling.
    Note: Backpack does not provide WebSocket streams for funding data, so we use periodic REST calls.
    """

    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DEPTH_STREAM_ID = 2

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BackpackPerpetualDerivative',
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
        Fetch the last traded prices for the given derivative trading pairs.

        Args:
            trading_pairs: List of trading pairs to get prices for
            domain: Optional domain parameter

        Returns:
            Dictionary mapping trading pairs to their last traded prices
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """
        Retrieves the funding information for a perpetual trading pair.

        This method fetches current mark price, index price, funding rate, and next funding time
        from Backpack's /api/v1/markPrices endpoint.

        Args:
            trading_pair: The trading pair to get funding info for

        Returns:
            FundingInfo object containing funding rate and price information
        """
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            params = {"symbol": symbol}

            rest_assistant = await self._api_factory.get_rest_assistant()
            response = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.MARK_PRICES_PATH_URL, domain=self._domain),
                params=params,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.MARK_PRICES_PATH_URL,
            )

            # Backpack returns an array, we need the first (and should be only) result for the specific symbol
            if response and len(response) > 0:
                mark_data = response[0]

                # Parse the response according to Backpack's MarkPrice schema
                funding_info = FundingInfo(
                    trading_pair=trading_pair,
                    index_price=Decimal(str(mark_data["indexPrice"])),
                    mark_price=Decimal(str(mark_data["markPrice"])),
                    next_funding_utc_timestamp=int(mark_data["nextFundingTimestamp"]),
                    rate=Decimal(str(mark_data["fundingRate"]))
                )

                return funding_info
            else:
                raise ValueError(f"No funding data returned for {trading_pair}")

        except Exception as e:
            self.logger().error(
                f"Error fetching funding info for {trading_pair}: {e}",
                exc_info=True
            )
            # Return a default FundingInfo to prevent complete failure
            return FundingInfo(
                trading_pair=trading_pair,
                index_price=Decimal("0"),
                mark_price=Decimal("0"),
                next_funding_utc_timestamp=int(time.time() * 1000) + 28800000,  # 8 hours from now
                rate=Decimal("0")
            )

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse funding info message from WebSocket.

        Note: Backpack does not provide funding info via WebSocket, so this method is not used.
        Funding info is retrieved via REST API polling in get_funding_info().

        Args:
            raw_message: Raw message from websocket (unused for Backpack)
            message_queue: Queue to put the parsed message (unused for Backpack)
        """
        # Not implemented for Backpack since no WebSocket funding streams are available
        # Funding data is retrieved via REST API polling only
        pass

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular derivative trading pair.

        Args:
            trading_pair: The derivative trading pair for which the order book will be retrieved

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

        For derivatives, this is identical to spot trading - same WebSocket streams are used.

        Args:
            ws: The websocket assistant used to connect to the exchange
        """
        try:
            # Subscribe to trade updates and order book depth updates for all derivative trading pairs
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

                # Subscribe to trades - same format as spot
                trade_payload = {
                    "method": "SUBSCRIBE",
                    "params": [f"trade.{symbol}"],  # Format: trade.SYMBOL
                    "id": 1
                }

                trade_request = WSJSONRequest(payload=trade_payload)
                await ws.send(trade_request)

                # Subscribe to order book depth updates - same format as spot
                depth_payload = {
                    "method": "SUBSCRIBE",
                    "params": [f"depth.{symbol}"],  # Format: depth.SYMBOL
                    "id": 2
                }

                depth_request = WSJSONRequest(payload=depth_payload)
                await ws.send(depth_request)

            self.logger().info("Subscribed to derivative order book and trade channels...")

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to derivative order book and trade streams...",
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
            trading_pair: The derivative trading pair to get snapshot for

        Returns:
            OrderBookMessage containing the snapshot data
        """
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()

        # Reuse the same BackpackOrderBook logic since order book format is identical for derivatives
        snapshot_msg: OrderBookMessage = BackpackOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses a trade message from the websocket and puts it in the message queue.

        Trade message format is identical for spot and derivative trading pairs.

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

                    # Reuse BackpackOrderBook logic - trade format is identical for derivatives
                    trade_message = BackpackOrderBook.trade_message_from_exchange(
                        data,
                        {"trading_pair": trading_pair}
                    )
                    message_queue.put_nowait(trade_message)
        except Exception as e:
            self.logger().error(f"Error parsing derivative trade message: {e}", exc_info=True)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses an order book diff message from the websocket and puts it in the message queue.

        Order book diff format is identical for spot and derivative trading pairs.

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

                    # Reuse BackpackOrderBook logic - order book format is identical for derivatives
                    order_book_message: OrderBookMessage = BackpackOrderBook.diff_message_from_exchange(
                        data,
                        time.time(),
                        {"trading_pair": trading_pair}
                    )
                    message_queue.put_nowait(order_book_message)
        except Exception as e:
            self.logger().error(f"Error parsing derivative order book diff message: {e}", exc_info=True)

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
            # Note: No funding stream routing since Backpack doesn't provide funding WebSocket streams
            else:
                pass  # No other stream types are handled here
        else:
            pass  # No other message types are handled here

        return channel
