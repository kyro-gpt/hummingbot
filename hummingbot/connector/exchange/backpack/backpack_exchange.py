import asyncio
import hashlib
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils
from hummingbot.connector.exchange.backpack.backpack_api_order_book_data_source import BackpackAPIOrderBookDataSource
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class BackpackExchange(ExchangePyBase):
    """
    Backpack Exchange connector for Hummingbot.

    This class implements the interface between Hummingbot and the Backpack cryptocurrency exchange,
    providing functionality for market data, order management, and account operations.
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 backpack_api_key: str,
                 backpack_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """
        Initialize the Backpack exchange connector.

        Args:
            client_config_map: Client configuration
            backpack_api_key: The API key for authentication
            backpack_secret_key: The API secret for authentication
            trading_pairs: List of trading pairs to track
            trading_required: Whether trading functionality is required
            domain: The domain to use for API calls
        """
        self.api_key = backpack_api_key
        self.secret_key = backpack_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_backpack_timestamp = 1.0
        self._trading_pair_symbol_map: Optional[bidict] = None
        self._mapping_initialization_lock = asyncio.Lock()

        super().__init__(client_config_map)

    @staticmethod
    def backpack_order_type(order_type: OrderType) -> str:
        """Convert Hummingbot order type to Backpack order type."""
        return CONSTANTS.BACKPACK_ORDER_TYPE[order_type]

    @staticmethod
    def to_hb_order_type(backpack_type: str) -> OrderType:
        """Convert Backpack order type to Hummingbot order type."""
        return CONSTANTS.HB_ORDER_TYPE[backpack_type]

    @property
    def authenticator(self):
        """Returns the authenticator instance for API requests."""
        return BackpackAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer
        )

    @property
    def name(self) -> str:
        """Returns the name of this exchange connector."""
        return CONSTANTS.EXCHANGE_NAME

    @property
    def rate_limits_rules(self):
        """Returns the rate limits configuration."""
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        """Returns the domain for this exchange."""
        return self._domain

    @property
    def client_order_id_max_length(self):
        """Returns the maximum length for client order IDs."""
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        """Returns the prefix used for client order IDs."""
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        """Returns the API path for trading rules."""
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        """Returns the API path for trading pairs."""
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def check_network_request_path(self):
        """Returns the API path for network connectivity checks."""
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        """Returns the list of trading pairs."""
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        """Returns whether order cancellation is synchronous."""
        return True

    @property
    def is_trading_required(self) -> bool:
        """Returns whether trading functionality is required."""
        return self._trading_required

    def supported_order_types(self):
        """Returns the list of supported order types."""
        return CONSTANTS.SUPPORTED_ORDER_TYPES

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        """Check if the exception is related to time synchronization issues."""
        error_description = str(request_exception)
        # Backpack specific time sync error patterns - to be updated based on actual API errors
        is_time_synchronizer_related = ("timestamp" in error_description.lower() and
                                        ("invalid" in error_description.lower() or
                                        "expired" in error_description.lower()))
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """Check if the exception indicates an order was not found during status update."""
        error_description = str(status_update_exception).lower()
        return "order not found" in error_description or "unknown order" in error_description

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """Check if the exception indicates an order was not found during cancellation."""
        error_description = str(cancelation_exception).lower()
        return "order not found" in error_description or "unknown order" in error_description

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """Creates the web assistants factory for API calls."""
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """Creates the order book data source."""
        return BackpackAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """Creates the user stream data source."""
        from hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source import (
            BackpackAPIUserStreamDataSource,
        )
        return BackpackAPIUserStreamDataSource(
            auth=self.authenticator,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = Decimal("NaN"),
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculate trading fees for the given order.

        Args:
            base_currency: Base currency of the trading pair
            quote_currency: Quote currency of the trading pair
            order_type: Order type (limit, market, etc.)
            order_side: Order side (buy/sell)
            amount: Order amount
            price: Order price
            is_maker: Whether this is a maker order

        Returns:
            TradeFeeBase object with fee information
        """
        # Determine if this is a maker order
        is_maker_order = order_type is OrderType.LIMIT_MAKER or (is_maker if is_maker is not None else False)

        return build_trade_fee(
            exchange=self.name,
            is_maker=is_maker_order,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price
        )

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """Get the last traded price for a trading pair."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {"symbol": symbol}

        resp_json = await self._api_get(
            path_url=CONSTANTS.TICKER_PATH_URL,
            params=params,
        )

        return float(resp_json.get("lastPrice", 0))

    async def _make_trading_rules_request(self) -> Any:
        """Request trading rules from the exchange."""
        exchange_info = await self._api_get(path_url=CONSTANTS.MARKETS_PATH_URL)
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        """Request available trading pairs from the exchange."""
        exchange_info = await self._api_get(path_url=CONSTANTS.MARKETS_PATH_URL)
        return exchange_info

    async def _get_trading_pair_from_exchange_symbol_map(self):
        """Build the mapping between exchange symbols and trading pairs."""
        async with self._mapping_initialization_lock:
            if self._trading_pair_symbol_map is None:
                exchange_info = await self._make_trading_pairs_request()
                self._trading_pair_symbol_map = await self._init_trading_pair_symbols(exchange_info)

    async def _init_trading_pair_symbols(self, exchange_info: Dict[str, Any]):
        """Initialize the trading pair to symbol mapping."""
        mapping = bidict()

        # Backpack returns market info with symbols like "SOL_USDC"
        for market_info in exchange_info:
            exchange_symbol = market_info["symbol"]
            base = market_info["baseSymbol"]
            quote = market_info["quoteSymbol"]
            trading_pair = f"{base}-{quote}"

            if trading_pair in self._trading_pairs:
                mapping[trading_pair] = exchange_symbol

        return mapping

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """Convert trading pair to exchange symbol."""
        if self._trading_pair_symbol_map is None:
            await self._get_trading_pair_from_exchange_symbol_map()
        return self._trading_pair_symbol_map[trading_pair]

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        """Convert exchange symbol to trading pair."""
        if self._trading_pair_symbol_map is None:
            await self._get_trading_pair_from_exchange_symbol_map()
        return self._trading_pair_symbol_map.inverse[symbol]

    def _get_order_book_tracker(self):
        """Create and return order book tracker."""
        if self._order_book_tracker is None:
            self._order_book_tracker = self._create_order_book_tracker()
        return self._order_book_tracker

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """Get last traded prices for multiple trading pairs."""
        results = {}
        for trading_pair in trading_pairs:
            try:
                price = await self._get_last_traded_price(trading_pair)
                results[trading_pair] = price
            except Exception as e:
                self.logger().error(f"Error fetching last price for {trading_pair}: {e}")
                results[trading_pair] = 0.0
        return results

    # ============================================================
    # ABSTRACT METHOD IMPLEMENTATIONS
    # ============================================================

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        """
        Place an order on Backpack exchange.

        Args:
            order_id: Hummingbot's client order ID
            trading_pair: Trading pair in Hummingbot format (e.g., "SOL-USDC")
            amount: Order amount
            trade_type: BUY or SELL
            order_type: LIMIT, MARKET, etc.
            price: Order price

        Returns:
            Tuple of (exchange_order_id, timestamp)
        """
        try:
            # Convert trading pair to exchange format
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

            # Generate client ID from Hummingbot order ID
            # Backpack expects 32-bit unsigned integer, so we hash the order_id
            client_id = int(hashlib.md5(order_id.encode()).hexdigest()[:8], 16) % (2**32 - 1)

            # Convert order parameters to Backpack format
            side = "Bid" if trade_type == TradeType.BUY else "Ask"

            # Map order types
            if order_type == OrderType.LIMIT:
                order_type_str = "Limit"
            elif order_type == OrderType.MARKET:
                order_type_str = "Market"
            else:
                raise ValueError(f"Unsupported order type: {order_type}")

            # Build order data
            order_data = {
                "symbol": symbol,
                "side": side,
                "orderType": order_type_str,
                "quantity": str(amount),
                "clientId": client_id
            }

            # Add price for limit orders
            if order_type == OrderType.LIMIT:
                order_data["price"] = str(price)
                order_data["timeInForce"] = "GTC"  # Good Till Cancelled

            # Place order via REST API
            response = await self._api_post(
                path_url=CONSTANTS.ORDER_PATH_URL,
                data=order_data,
                is_auth_required=True
            )

            # Extract exchange order ID and timestamp
            exchange_order_id = str(response["id"])
            timestamp = response["createdAt"] / 1000.0  # Convert milliseconds to seconds

            return exchange_order_id, timestamp

        except Exception as e:
            self.logger().error(f"Failed to place order {order_id}: {e}")
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel an order on Backpack exchange.

        Args:
            order_id: Hummingbot's client order ID
            tracked_order: InFlightOrder object containing order details

        Returns:
            bool: True if cancellation was successful, False otherwise
        """
        try:
            # Convert trading pair to exchange format
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

            # Cancel using exchange order ID (stored in tracked_order.exchange_order_id)
            cancel_data = {
                "symbol": symbol,
                "orderId": tracked_order.exchange_order_id
            }

            # Cancel order via REST API
            response = await self._api_delete(
                path_url=CONSTANTS.ORDER_PATH_URL,
                data=cancel_data,
                is_auth_required=True
            )

            # Check if cancellation was successful
            is_cancelled = response.get("status") == "Cancelled"

            if is_cancelled:
                self.logger().info(f"Successfully cancelled order {order_id} (exchange ID: {tracked_order.exchange_order_id})")
            else:
                self.logger().warning(f"Order cancellation may have failed for {order_id}. Response: {response}")

            return is_cancelled

        except Exception as e:
            self.logger().error(f"Failed to cancel order {order_id}: {e}")
            return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """Format trading rules from exchange info."""
        # TODO: Implement based on Backpack's market info format
        # For now, return empty list - this will be implemented when we have market data
        return []

    async def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """Initialize trading pair symbols from exchange info."""
        # Use our existing symbol mapping logic
        self._trading_pair_symbol_map = await self._init_trading_pair_symbols(exchange_info)

    async def _update_balances(self):
        """Update account balances."""
        # TODO: Implement in Phase 3 - Private API
        # This will use GET /api/v1/capital endpoint
        pass

    async def _update_trading_fees(self):
        """Update trading fees information."""
        # TODO: Implement when we have account-specific fee information
        # For now, use default fees from constants
        pass

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """Get all trade updates for an order."""
        # TODO: Implement in Phase 3 - Private API
        # This will use GET /wapi/v1/history/fills endpoint
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from Backpack exchange.

        Args:
            tracked_order: InFlightOrder object to check status for

        Returns:
            OrderUpdate with current order status
        """
        try:
            # Convert trading pair to exchange format
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

            # Query order status using exchange order ID
            params = {
                "symbol": symbol,
                "orderId": tracked_order.exchange_order_id
            }

            # Get order status via REST API
            order_data = await self._api_get(
                path_url=CONSTANTS.ORDER_PATH_URL,
                params=params,
                is_auth_required=True
            )

            # Map Backpack status to Hummingbot OrderState
            backpack_status = order_data["status"]
            new_state = CONSTANTS.ORDER_STATE.get(backpack_status, tracked_order.current_state)

            # Create order update
            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=order_data.get("createdAt", self._time_synchronizer.time()) / 1000.0,  # Convert milliseconds to seconds
                new_state=new_state,
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(order_data["id"]),
            )

            return order_update

        except Exception as e:
            # Handle 404 error - order not found (likely cancelled)
            if "404" in str(e) and "RESOURCE_NOT_FOUND" in str(e):
                self.logger().info(f"Order {tracked_order.client_order_id} not found - likely cancelled")
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self._time_synchronizer.time(),
                    new_state=CONSTANTS.ORDER_STATE.get("Cancelled", tracked_order.current_state),
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )
            else:
                self.logger().error(f"Failed to request order status for {tracked_order.client_order_id}: {e}")
                # Return current state if request fails
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self._time_synchronizer.time(),
                    new_state=tracked_order.current_state,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )

    async def _user_stream_event_listener(self):
        """Listen to user stream events."""
        # TODO: Implement in Phase 3 - Private API
        # This will process WebSocket events for orders, trades, balances
        pass
