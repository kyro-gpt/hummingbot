import asyncio
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import (
        LighterPerpetualDerivative,
    )


class LighterPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    _bpobds_logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'LighterPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DEFAULT_DOMAIN
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._snapshot_messages_queue_key = "order_book_snapshot"

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """
        Get funding information for a trading pair
        """
        market_id = web_utils.format_trading_pair_to_market_id(trading_pair)

        # Get funding data from /api/v1/fundings endpoint
        params = {"market_id": market_id}
        response = await self._connector._api_get(
            path_url=CONSTANTS.FUNDINGS_PATH_URL,
            params=params
        )

        # Get mark price and index price from order book details
        orderbook_params = {"market_id": market_id}
        orderbook_response = await self._connector._api_get(
            path_url=CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL,
            params=orderbook_params
        )

        # Extract funding info from response
        funding_rate = Decimal("0")  # Default if no funding data
        if response.get("fundings") and len(response["fundings"]) > 0:
            latest_funding = response["fundings"][0]  # Most recent funding
            funding_rate = Decimal(str(latest_funding.get("funding_rate", "0")))

        # Extract prices from orderbook details
        mark_price = Decimal("0")
        index_price = Decimal("0")
        if orderbook_response.get("order_book_details"):
            details = orderbook_response["order_book_details"][0]
            mark_price = Decimal(str(details.get("mark_price", "0")))
            index_price = Decimal(str(details.get("index_price", "0")))

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=index_price,
            mark_price=mark_price,
            next_funding_utc_timestamp=self._next_funding_time(),
            rate=funding_rate,
        )
        return funding_info

    async def listen_for_funding_info(self, output: asyncio.Queue):
        """
        Reads the funding info events queue and updates the local funding info information.
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    funding_info = await self.get_funding_info(trading_pair)
                    funding_info_update = FundingInfoUpdate(
                        trading_pair=trading_pair,
                        index_price=funding_info.index_price,
                        mark_price=funding_info.mark_price,
                        next_funding_utc_timestamp=funding_info.next_funding_utc_timestamp,
                        rate=funding_info.rate,
                    )
                    output.put_nowait(funding_info_update)
                await self._sleep(CONSTANTS.FUNDING_RATE_UPDATE_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public funding info updates from exchange")
                await self._sleep(CONSTANTS.FUNDING_RATE_UPDATE_INTERVAL)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Request order book snapshot from REST API
        """
        market_id = web_utils.format_trading_pair_to_market_id(trading_pair)

        # Use orderBookDetails endpoint for order book snapshot
        params = {"market_id": market_id}
        data = await self._connector._api_get(
            path_url=CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL,
            params=params
        )
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Convert REST API response to OrderBookMessage
        """
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)

        # Extract order book data from response
        order_book_details = snapshot_response.get("order_book_details", [])
        if not order_book_details:
            raise ValueError(f"No order book data found for {trading_pair}")

        order_book_data = order_book_details[0]

        # Convert bids and asks from PriceLevel format
        bids = []
        asks = []

        if "bids" in order_book_data:
            bids = [[float(level["price"]), float(level["size"])] for level in order_book_data["bids"]]

        if "asks" in order_book_data:
            asks = [[float(level["price"]), float(level["size"])] for level in order_book_data["asks"]]

        # Use current timestamp as update_id since Lighter doesn't provide sequence numbers
        timestamp = int(time.time() * 1000)

        snapshot_msg: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair,
            "update_id": timestamp,
            "bids": bids,
            "asks": asks,
        }, timestamp=timestamp)
        return snapshot_msg

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Create and connect WebSocket assistant
        """
        url = web_utils.wss_url(self._domain)
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to order book channels via WebSocket
        Following actual Lighter SDK protocol: individual channel subscriptions
        """
        try:
            # Subscribe to order book updates for each trading pair
            for trading_pair in self._trading_pairs:
                market_id = web_utils.format_trading_pair_to_market_id(trading_pair)
                
                # Lighter WebSocket protocol: {"type": "subscribe", "channel": "order_book/{market_id}"}
                subscribe_payload = {
                    "type": "subscribe",
                    "channel": f"order_book/{market_id}"
                }
                subscribe_request: WSJSONRequest = WSJSONRequest(payload=subscribe_payload)
                await ws.send(subscribe_request)
                
                self.logger().info(f"Subscribed to order book channel: order_book/{market_id}")
            
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book data streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Determine which channel a WebSocket message belongs to
        Based on actual Lighter WebSocket protocol
        """
        channel = ""
        message_type = event_message.get("type", "")
        
        if message_type in ["subscribed/order_book", "update/order_book"]:
            channel = self._snapshot_messages_queue_key
        elif message_type == "update/trades":  # Keep for future trade support
            channel = self._trade_messages_queue_key
        
        return channel

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse WebSocket order book update message
        Based on actual Lighter WebSocket protocol
        """
        try:
            # Extract market_id from channel (format: "order_book:1")
            channel = raw_message.get("channel", "")
            if not channel or ":" not in channel:
                return
            
            market_id = int(channel.split(":")[1])
            
            # Convert market_id back to trading pair
            trading_pair = web_utils.format_market_id_to_trading_pair(market_id)
            
            # Extract order book data from message
            order_book_data = raw_message.get("order_book", {})
            
            # Extract bids and asks (format: [{"price": "X", "size": "Y"}])
            bids = []
            asks = []
            
            if "bids" in order_book_data:
                bids = [[float(level["price"]), float(level["size"])] for level in order_book_data["bids"]]
            
            if "asks" in order_book_data:
                asks = [[float(level["price"]), float(level["size"])] for level in order_book_data["asks"]]
            
            timestamp = int(time.time() * 1000)
            
            order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.DIFF, {
                "trading_pair": trading_pair,
                "update_id": timestamp,
                "bids": bids,
                "asks": asks,
            }, timestamp=timestamp)
            
            message_queue.put_nowait(order_book_message)
            
        except Exception as e:
            self.logger().error(f"Error parsing order book diff message: {e}")

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse WebSocket order book snapshot message
        Based on actual Lighter WebSocket protocol
        """
        try:
            # Extract market_id from channel (format: "order_book:1")
            channel = raw_message.get("channel", "")
            if not channel or ":" not in channel:
                return
            
            market_id = int(channel.split(":")[1])
            
            # Convert market_id back to trading pair
            trading_pair = web_utils.format_market_id_to_trading_pair(market_id)
            
            # Extract order book data from message
            order_book_data = raw_message.get("order_book", {})
            
            # Extract bids and asks (format: [{"price": "X", "size": "Y"}])
            bids = []
            asks = []
            
            if "bids" in order_book_data:
                bids = [[float(level["price"]), float(level["size"])] for level in order_book_data["bids"]]
            
            if "asks" in order_book_data:
                asks = [[float(level["price"]), float(level["size"])] for level in order_book_data["asks"]]
            
            timestamp = int(time.time() * 1000)
            
            order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
                "trading_pair": trading_pair,
                "update_id": timestamp,
                "bids": bids,
                "asks": asks,
            }, timestamp=timestamp)
            
            message_queue.put_nowait(order_book_message)
            
        except Exception as e:
            self.logger().error(f"Error parsing order book snapshot message: {e}")

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse WebSocket trade message
        """
        try:
            data = raw_message.get("data", {})

            # Handle multiple trades in one message
            trades = data if isinstance(data, list) else [data]

            for trade_data in trades:
                market_id = trade_data.get("market_id")
                if market_id is None:
                    continue

                trading_pair = web_utils.format_market_id_to_trading_pair(market_id)

                # Determine trade type based on trade data
                # In Lighter, we need to infer from the trade structure
                trade_type = TradeType.BUY  # Default, will be determined by price movement or other indicators

                trade_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.TRADE, {
                    "trading_pair": trading_pair,
                    "trade_type": float(trade_type.value),
                    "trade_id": str(trade_data.get("trade_id", "")),
                    "price": float(trade_data.get("price", "0")),
                    "amount": float(trade_data.get("size", "0"))
                }, timestamp=int(time.time() * 1000))

                message_queue.put_nowait(trade_message)

        except Exception as e:
            self.logger().error(f"Error parsing trade message: {e}")

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse funding info message (placeholder for future implementation)
        """
        pass

    def _next_funding_time(self) -> int:
        """
        Calculate next funding time
        Lighter funding occurs every 8 hours (similar to most perpetual exchanges)
        """
        # Funding at 00:00, 08:00, 16:00 UTC
        current_time = int(time.time())
        hours_since_epoch = current_time // 3600
        next_funding_hour = ((hours_since_epoch // 8) + 1) * 8
        return next_funding_hour * 3600

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Get a new OrderBook instance with current snapshot data
        Required by base class OrderBookTrackerDataSource
        """
        snapshot_msg = await self._order_book_snapshot(trading_pair)
        order_book = OrderBook()
        
        # Convert bid/ask data to OrderBookRow objects
        bids = snapshot_msg.content.get("bids", [])
        asks = snapshot_msg.content.get("asks", [])
        
        bid_rows = [OrderBookRow(price=Decimal(str(bid[0])), amount=Decimal(str(bid[1])), update_id=snapshot_msg.update_id) for bid in bids]
        ask_rows = [OrderBookRow(price=Decimal(str(ask[0])), amount=Decimal(str(ask[1])), update_id=snapshot_msg.update_id) for ask in asks]
        
        # Apply snapshot to order book
        order_book.apply_snapshot(bid_rows, ask_rows, snapshot_msg.update_id)
        return order_book

    async def _request_complete_funding_info(self, trading_pair: str) -> List:
        """
        Request complete funding information for a trading pair
        Returns raw API response data (similar to Hyperliquid pattern)
        """
        market_id = web_utils.format_trading_pair_to_market_id(trading_pair)
        
        # Get funding data from /api/v1/fundings endpoint
        params = {"market_id": market_id}
        funding_response = await self._connector._api_get(
            path_url=CONSTANTS.FUNDINGS_PATH_URL,
            params=params
        )
        
        # Get order book details for mark/index prices
        orderbook_params = {"market_id": market_id}
        orderbook_response = await self._connector._api_get(
            path_url=CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL,
            params=orderbook_params
        )
        
        # Return combined data as list (similar to Hyperliquid format)
        return [funding_response, orderbook_response]
