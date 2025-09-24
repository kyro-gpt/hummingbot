import asyncio
import logging
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative


class AsterPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    _apobds_logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'AsterPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._trade_messages_queue_key = CONSTANTS.TRADE_STREAM_ID
        self._diff_messages_queue_key = CONSTANTS.DIFF_STREAM_ID
        self._funding_info_messages_queue_key = CONSTANTS.FUNDING_INFO_STREAM_ID
        self._snapshot_messages_queue_key = "order_book_snapshot"

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._apobds_logger is None:
            cls._apobds_logger = logging.getLogger(__name__)
        return cls._apobds_logger

    @property
    def exchange_name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        url = web_utils.wss_url(CONSTANTS.PUBLIC_WS_ENDPOINT, domain=self._domain)
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)

        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        NOTE: Using @depth@100ms for 2.5x faster order book updates than Binance default
        FALLBACK: If rate limiting issues, change WS_DEPTH_CHANNEL to "@depth" (250ms default)

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            stream_id_channel_pairs = [
                (CONSTANTS.DIFF_STREAM_ID, CONSTANTS.WS_DEPTH_CHANNEL),    # @depth@100ms (optimized)
                (CONSTANTS.TRADE_STREAM_ID, CONSTANTS.WS_TRADE_CHANNEL),   # @aggTrade
                (CONSTANTS.FUNDING_INFO_STREAM_ID, CONSTANTS.WS_FUNDING_CHANNEL),  # @markPrice
            ]
            for stream_id, channel in stream_id_channel_pairs:
                params = []
                for trading_pair in self._trading_pairs:
                    symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                    params.append(f"{symbol.lower()}{channel}")
                payload = {
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": stream_id,
                }
                subscribe_request: WSJSONRequest = WSJSONRequest(payload)
                await ws.send(subscribe_request)
            self.logger().info("Subscribed to public order book (100ms), trade and funding info channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "result" not in event_message:
            stream_name = event_message.get("stream")
            if "@depth" in stream_name:  # Handles both @depth and @depth@100ms
                channel = self._diff_messages_queue_key
            elif "@aggTrade" in stream_name:
                channel = self._trade_messages_queue_key
            elif "@markPrice" in stream_name:
                channel = self._funding_info_messages_queue_key
        return channel

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse order book diff messages from WebSocket

        NOTE: Handles @depth@100ms format (faster than Binance default)
        Message format should be same as Binance since Aster is 90% compatible
        """
        diff_data: Dict[str, Any] = raw_message["data"]
        timestamp: float = time.time()
        update_id: int = diff_data["u"]

        order_book_message_content = {
            "trading_pair": await self._connector.trading_pair_associated_to_exchange_symbol(symbol=diff_data["s"]),
            "update_id": update_id,
            "bids": [(Decimal(bid[0]), Decimal(bid[1])) for bid in diff_data["b"]],
            "asks": [(Decimal(ask[0]), Decimal(ask[1])) for ask in diff_data["a"]],
        }
        order_book_message: OrderBookMessage = OrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=order_book_message_content,
            timestamp=timestamp,
        )

        message_queue.put_nowait(order_book_message)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse trade messages from WebSocket - Same format as Binance
        """
        trade_data: Dict[str, Any] = raw_message["data"]
        timestamp: float = time.time()

        order_book_message_content = {
            "trading_pair": await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["s"]),
            "trade_type": float(TradeType.BUY.value) if trade_data["m"] is False else float(TradeType.SELL.value),
            "trade_id": trade_data["a"],
            "update_id": trade_data["a"],
            "price": Decimal(trade_data["p"]),
            "amount": Decimal(trade_data["q"]),
        }
        order_book_message: OrderBookMessage = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=order_book_message_content,
            timestamp=timestamp,
        )

        message_queue.put_nowait(order_book_message)

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse funding info messages from WebSocket - Same format as Binance
        """
        funding_data: Dict[str, Any] = raw_message["data"]

        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=funding_data["s"])
        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(funding_data["i"]),
            mark_price=Decimal(funding_data["p"]),
            next_funding_utc_timestamp=int(funding_data["T"] / 1e3),
            rate=Decimal(funding_data["r"]),
        )

        funding_info_update: FundingInfoUpdate = FundingInfoUpdate(
            trading_pair=trading_pair,
            index_price=funding_info.index_price,
            mark_price=funding_info.mark_price,
            next_funding_utc_timestamp=funding_info.next_funding_utc_timestamp,
            rate=funding_info.rate,
        )

        message_queue.put_nowait(funding_info_update)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Retrieve order book snapshot from REST API
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": 1000  # Maximum depth for snapshot
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.SNAPSHOT_REST_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_REST_URL,
        )

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": data["lastUpdateId"],
            "bids": [(Decimal(bid[0]), Decimal(bid[1])) for bid in data["bids"]],
            "asks": [(Decimal(ask[0]), Decimal(ask[1])) for ask in data["asks"]],
        }

        order_book_message: OrderBookMessage = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=order_book_message_content,
            timestamp=time.time(),
        )

        return order_book_message

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": 1000
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.SNAPSHOT_REST_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_REST_URL,
        )

        return data

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """
        Retrieves the funding info for the specified trading pair.

        :param trading_pair: the trading pair for which the funding info will be retrieved
        :return: The funding info for the trading pair
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        funding_response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.MARK_PRICE_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.MARK_PRICE_URL,
        )

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(funding_response["indexPrice"]),
            mark_price=Decimal(funding_response["markPrice"]),
            next_funding_utc_timestamp=int(funding_response["nextFundingTime"]),
            rate=Decimal(funding_response["lastFundingRate"]),
        )

        return funding_info
