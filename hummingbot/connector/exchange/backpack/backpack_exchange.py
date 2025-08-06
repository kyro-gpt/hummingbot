import asyncio
import hashlib
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.exchange.backpack import (
    backpack_constants as CONSTANTS,
    backpack_utils,
    backpack_web_utils as web_utils,
)
from hummingbot.connector.exchange.backpack.backpack_api_order_book_data_source import BackpackAPIOrderBookDataSource
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
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

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """
        Returns True if the exception indicates that the order was not found during status update.

        Args:
            status_update_exception: Exception raised during order status update

        Returns:
            True if the order was not found, False otherwise
        """
        error_description = str(status_update_exception)
        # Check for typical 404 error patterns from Backpack
        is_not_found = (
            "404" in error_description or
            "Order not found" in error_description or
            "not found" in error_description.lower()
        )
        return is_not_found

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """
        Returns True if the exception indicates that the order was not found during cancelation.

        Args:
            cancelation_exception: Exception raised during order cancelation

        Returns:
            True if the order was not found, False otherwise
        """
        error_description = str(cancelation_exception)
        # Check for typical 404 error patterns from Backpack
        is_not_found = (
            "404" in error_description or
            "Order not found" in error_description or
            "not found" in error_description.lower()
        )
        return is_not_found

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
        error_description = str(status_update_exception)
        # Check for typical error patterns from Backpack that indicate order not found
        is_not_found = (
            "404" in error_description or  # Not Found
            "401" in error_description or  # Unauthorized (may indicate order doesn't exist)
            "Order not found" in error_description or
            "not found" in error_description.lower()
        )
        return is_not_found



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

    async def _make_network_check_request(self):
        """
        Override network check to handle Backpack's plain text 'pong' response.

        Backpack's /api/v1/ping returns plain text "pong", not JSON.
        We need to use the REST assistant directly to avoid JSON parsing.
        """
        try:
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            url = self.web_utils.public_rest_url(path_url=self.check_network_request_path, domain=self._domain)

            # Use execute_request_and_get_response to get RESTResponse object, then call .text()
            rest_response = await rest_assistant.execute_request_and_get_response(
                url=url,
                method=RESTMethod.GET,
                throttler_limit_id=self.check_network_request_path
            )

            # Get the plain text response (not JSON)
            response_text = await rest_response.text()

            # Check if response is the expected "pong"
            if response_text.strip().lower() == "pong":
                self.logger().debug("Network check successful: received 'pong' response")
                return response_text
            else:
                self.logger().warning(f"Unexpected ping response: {response_text}")
                return response_text

        except Exception as e:
            self.logger().error(f"Network check request failed: {e}")
            raise

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

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        """
        Initialize trading pair symbol mapping from exchange info.
        Required abstract method from ExchangePyBase.
        """
        mapping = bidict()

        # Backpack returns market info with symbols like "SOL_USDC"
        for market_info in exchange_info:
            try:
                if backpack_utils.is_exchange_information_valid(market_info):
                    exchange_symbol = market_info["symbol"]
                    base = market_info["baseSymbol"]
                    quote = market_info["quoteSymbol"]
                    trading_pair = f"{base}-{quote}"

                    if self._trading_pairs is None or trading_pair in self._trading_pairs:
                        mapping[exchange_symbol] = trading_pair
            except Exception as e:
                self.logger().error(f"Error parsing trading pair {market_info}: {e}")

        self._set_trading_pair_symbol_map(mapping)

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

            # Cancel using exchange order ID (per API spec: either orderId OR clientId, not both)
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
            # Log the error and re-raise for base class handling
            self.logger().error(f"Failed to cancel order {order_id}: {e}")
            raise

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from Backpack's market info.

        Actual exchange_info_dict format from Backpack /api/v1/markets endpoint:
        [
            {
                "symbol": "SOL_USDC",
                "baseSymbol": "SOL",
                "quoteSymbol": "USDC",
                "marketType": "SPOT",
                "orderBookState": "Open",
                "filters": {
                    "price": {
                        "minPrice": "0.01",
                        "maxPrice": null,
                        "tickSize": "0.01"
                    },
                    "quantity": {
                        "minQuantity": "0.01",
                        "maxQuantity": null,
                        "stepSize": "0.01"
                    }
                }
            }
        ]

        Note: Backpack doesn't provide minNotional filter, so we use a sensible default.
        """
        try:
            trading_rules = []
            markets_data = exchange_info_dict if isinstance(exchange_info_dict, list) else exchange_info_dict.get("markets", [])

            # Import the validation function
            from hummingbot.connector.exchange.backpack import backpack_utils

            for market_info in filter(backpack_utils.is_exchange_information_valid, markets_data):
                try:
                    # Extract symbol information
                    exchange_symbol = market_info.get("symbol")
                    if not exchange_symbol:
                        self.logger().debug(f"Skipping market with no symbol: {market_info}")
                        continue

                    # Market validation is already done by the filter function

                    # Convert to Hummingbot trading pair format using base and quote symbols
                    base_symbol = market_info.get("baseSymbol", "")
                    quote_symbol = market_info.get("quoteSymbol", "")

                    if not base_symbol or not quote_symbol:
                        self.logger().warning(f"Missing base/quote symbols for {exchange_symbol}")
                        continue

                    # Create trading pair in Hummingbot format: BASE-QUOTE
                    trading_pair = f"{base_symbol.upper()}-{quote_symbol.upper()}"

                    # Extract filter information from Backpack's nested dict format
                    # {"price": {"tickSize": "0.01", "minPrice": "0.01"}, "quantity": {"minQuantity": "0.01", "stepSize": "0.01"}}
                    filters = market_info.get("filters", {})

                    price_filter = filters.get("price", {})
                    quantity_filter = filters.get("quantity", {})
                    # Backpack doesn't provide minNotional filter, so this will be empty
                    min_notional_filter = filters.get("minNotional", {})

                    # Extract values from Backpack's actual API response
                    min_price_increment = Decimal(str(price_filter.get("tickSize", "0.01")))
                    min_order_size = Decimal(str(quantity_filter.get("minQuantity", "0.001")))

                    # Handle maxQuantity - Backpack often returns null, so provide a reasonable default
                    max_quantity_value = quantity_filter.get("maxQuantity")
                    if max_quantity_value is not None:
                        max_order_size = Decimal(str(max_quantity_value))
                    else:
                        # Use a very large default when maxQuantity is null
                        max_order_size = Decimal("1000000")

                    min_base_amount_increment = Decimal(str(quantity_filter.get("stepSize", "0.001")))

                    # Handle min notional size
                    min_notional_value = min_notional_filter.get("minNotional") or min_notional_filter.get("minNotionalSize")
                    if min_notional_value:
                        min_notional_size = Decimal(str(min_notional_value))
                    else:
                        # Only use default if no notional filter is provided by the API
                        min_notional_size = Decimal("1.0")

                    # Create trading rule
                    trading_rule = TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_order_size,
                        max_order_size=max_order_size,
                        min_price_increment=min_price_increment,
                        min_base_amount_increment=min_base_amount_increment,
                        min_notional_size=min_notional_size
                    )

                    trading_rules.append(trading_rule)
                    self.logger().debug(f"Created trading rule for {trading_pair} ({exchange_symbol}): "
                                        f"min_order_size={min_order_size}, "
                                        f"min_price_increment={min_price_increment}, "
                                        f"min_notional_size={min_notional_size}")

                except Exception as e:
                    # More detailed error logging
                    symbol = market_info.get("symbol", "unknown")
                    self.logger().debug(f"Error parsing trading rule for market {symbol}: {e}")
                    self.logger().debug(f"Market data: {market_info}")
                    continue

            self.logger().info(f"Successfully parsed {len(trading_rules)} trading rules from {len(markets_data)} markets")
            return trading_rules

        except Exception as e:
            self.logger().error(f"Failed to format trading rules: {e}")
            import traceback
            self.logger().debug(f"Full traceback: {traceback.format_exc()}")
            # Return empty list on error - don't block connector initialization
            return []

    async def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """Initialize trading pair symbols from exchange info."""
        # Use our existing symbol mapping logic
        self._trading_pair_symbol_map = await self._init_trading_pair_symbols(exchange_info)

    async def _update_balances(self):
        """
        Update account balances by fetching from Backpack's /capital endpoint.

        Expected response format:
        {
            "USDC": {"available": "46.157501635", "locked": "0", "staked": "0"},
            "SOL": {"available": "0.019109", "locked": "0", "staked": "0"},
            "POINTS": {"available": "5", "locked": "0", "staked": "0"}
        }
        """
        try:
            local_asset_names = set(self._account_balances.keys())
            remote_asset_names = set()

            # Fetch account balances from Backpack
            balance_info = await self._api_get(
                path_url=CONSTANTS.CAPITAL_PATH_URL,
                is_auth_required=True
            )

            # Process each asset balance
            for asset_name, balance_data in balance_info.items():
                # Skip non-tradeable assets like POINTS
                if asset_name in ["POINTS"]:
                    continue

                # Extract available and locked balances
                available_balance = Decimal(balance_data.get("available", "0"))
                locked_balance = Decimal(balance_data.get("locked", "0"))
                staked_balance = Decimal(balance_data.get("staked", "0"))

                # Total balance includes available + locked + staked
                total_balance = available_balance + locked_balance + staked_balance

                # Update balance tracking
                self._account_available_balances[asset_name] = available_balance
                self._account_balances[asset_name] = total_balance
                remote_asset_names.add(asset_name)

                self.logger().debug(f"Updated balance for {asset_name}: "
                                    f"available={available_balance}, total={total_balance}")

            # Remove assets that are no longer in the remote response
            asset_names_to_remove = local_asset_names.difference(remote_asset_names)
            for asset_name in asset_names_to_remove:
                if asset_name in self._account_available_balances:
                    del self._account_available_balances[asset_name]
                if asset_name in self._account_balances:
                    del self._account_balances[asset_name]
                self.logger().debug(f"Removed balance for {asset_name}")

            self.logger().info(f"Successfully updated balances for {len(remote_asset_names)} assets")

        except Exception as e:
            self.logger().error(f"Failed to update balances: {e}")
            # Don't raise exception - balance updates should be non-blocking

    async def _update_trading_fees(self):
        """
        Update trading fees information.

        Note: Backpack may not have a specific endpoint for account-level trading fees.
        For now, we'll use reasonable default values and update this when we have
        more information about Backpack's fee structure.
        """
        try:
            # Default fee structure for Backpack (these are typical values)
            # Update these values if Backpack provides account-specific fee information
            default_maker_fee = Decimal("0.0020")  # 0.20% (20 basis points)
            default_taker_fee = Decimal("0.0025")  # 0.25% (25 basis points)

            # Set fees for all trading pairs
            for trading_pair in self._trading_pairs or []:
                self._trading_fees[trading_pair] = {
                    "maker": default_maker_fee,
                    "taker": default_taker_fee
                }

            self.logger().info(f"Updated trading fees: maker={default_maker_fee}, taker={default_taker_fee}")
            self.logger().debug(f"Applied fees to {len(self._trading_fees)} trading pairs")

        except Exception as e:
            self.logger().error(f"Failed to update trading fees: {e}")
            # Don't raise exception - fee updates should be non-blocking

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Get all trade updates for a specific order from Backpack.

        Uses the GET /history/fills endpoint to fetch trade history for the order.

        Args:
            order: InFlightOrder object containing order details

        Returns:
            List of TradeUpdate objects representing all fills for this order
        """
        trade_updates = []

        if order.exchange_order_id is None:
            self.logger().debug(f"Order {order.client_order_id} has no exchange order ID")
            return trade_updates

        try:
            # Convert trading pair to exchange format
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)

            # Query fills for this specific order
            params = {
                "symbol": symbol,
                "orderId": order.exchange_order_id
            }

            # Get fills from Backpack API
            fills_response = await self._api_get(
                path_url=CONSTANTS.HISTORY_FILLS_PATH_URL,
                params=params,
                is_auth_required=True,
                limit_id=CONSTANTS.HISTORY_FILLS_PATH_URL
            )

            # Process each fill
            for fill in fills_response:
                try:
                    # Extract fill information
                    trade_id = str(fill.get("id", ""))
                    fill_timestamp = float(fill.get("timestamp", self._time_synchronizer.time() * 1000)) / 1000.0
                    fill_price = Decimal(str(fill.get("price", "0")))
                    fill_quantity = Decimal(str(fill.get("quantity", "0")))

                    # Calculate fill amounts
                    fill_base_amount = fill_quantity
                    fill_quote_amount = fill_base_amount * fill_price

                    # Extract fee information if available
                    fee_amount = Decimal(str(fill.get("fee", "0")))
                    fee_asset = fill.get("feeSymbol", order.quote_asset)

                    # Create fee object
                    if fee_amount > 0:
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=order.trade_type,
                            percent_token=fee_asset,
                            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)]
                        )
                    else:
                        # Use default fee calculation if no fee info provided
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=order.trade_type,
                            percent_token=order.quote_asset,
                        )

                    # Create trade update
                    trade_update = TradeUpdate(
                        trade_id=trade_id,
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_base_amount,
                        fill_quote_amount=fill_quote_amount,
                        fill_price=fill_price,
                        fill_timestamp=fill_timestamp,
                    )

                    trade_updates.append(trade_update)
                    self.logger().debug(f"Retrieved trade update for order {order.client_order_id}: "
                                        f"{fill_base_amount} @ {fill_price} (ID: {trade_id})")

                except Exception as e:
                    self.logger().error(f"Error processing fill {fill}: {e}")
                    continue

            self.logger().info(f"Retrieved {len(trade_updates)} trade updates for order {order.client_order_id}")

        except Exception as e:
            # Handle 404 or other errors gracefully
            if "404" in str(e) or "NOT_FOUND" in str(e):
                self.logger().debug(f"No fills found for order {order.client_order_id}")
            else:
                self.logger().error(f"Failed to get trade updates for order {order.client_order_id}: {e}")

        return trade_updates

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
            # Log the error and re-raise for base class handling
            self.logger().error(f"Failed to request order status for {tracked_order.client_order_id}: {e}")
            raise

    async def _user_stream_event_listener(self):
        """
        Process user stream events from Backpack WebSocket.

        This function runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are order updates, position updates, and trade events.

        Backpack WebSocket event format:
        - orderAccepted/orderCancelled events with stream: 'account.orderUpdate'
        - Position updates with stream: 'account.positionUpdate'
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_stream = event_message.get("stream", "")
                event_data = event_message.get("data", {})

                if not event_data:
                    continue

                # Handle order updates (account.orderUpdate and symbol-specific variants)
                if "orderUpdate" in event_stream:
                    await self._process_order_event(event_data)

                # Handle position updates (account.positionUpdate and symbol-specific variants)
                elif "positionUpdate" in event_stream:
                    await self._process_position_event(event_data)

                # Handle RFQ updates (if needed in future)
                elif "rfqUpdate" in event_stream:
                    self.logger().debug(f"Received RFQ update: {event_data}")
                    # RFQ updates are not needed for spot trading, but we log them for debugging

                else:
                    self.logger().debug(f"Unhandled user stream event: {event_message}")

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _process_order_event(self, event_data: Dict[str, Any]):
        """
        Process order-related events from Backpack WebSocket.

        Event format:
        {
            "E": 1748288615134547,    # Event timestamp
            "O": "USER",              # Order origin
            "S": "Ask",               # Side (Ask = Sell, Bid = Buy)
            "T": 1748288615133255,    # Transaction timestamp
            "V": "RejectTaker",       # Self trade prevention
            "X": "New",               # Order status (New, Cancelled, Filled, PartiallyFilled)
            "Z": "0",                 # Cumulative filled quantity (base)
            "c": 123456789,           # Client order ID (32-bit integer)
            "e": "orderAccepted",     # Event type (orderAccepted, orderCancelled, orderFilled)
            "f": "GTC",               # Time in force
            "i": "114575842681290753", # Exchange order ID
            "o": "LIMIT",             # Order type (LIMIT, MARKET)
            "p": "178.15",            # Price
            "q": "20.03",             # Quantity
            "r": false,               # Reduce only flag
            "s": "SOL_USDC",          # Symbol
            "t": null,                # Trade ID (filled when trade occurs)
            "z": "0"                  # Last filled quantity
        }
        """
        try:
            # Extract order information using real Backpack API format
            exchange_order_id = str(event_data.get("i", ""))       # Exchange order ID
            symbol = event_data.get("s", "")                       # Symbol
            order_status = event_data.get("X", "")                 # Order status
            event_type = event_data.get("e", "")                   # Event type
            client_order_id_raw = event_data.get("c")              # Client order ID (32-bit integer)

            if not exchange_order_id or not symbol:
                self.logger().debug(f"Missing order ID or symbol in event: {event_data}")
                return

            # Convert symbol to trading pair format
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except Exception as e:
                self.logger().warning(f"Could not convert symbol {symbol} to trading pair: {e}")
                return

            # Find client order ID from our tracked orders using exchange order ID
            client_order_id = None
            tracked_order = None

            # First try to find by exchange order ID
            for order_id, order in self._order_tracker.all_updatable_orders.items():
                if order.exchange_order_id == exchange_order_id:
                    client_order_id = order_id
                    tracked_order = order
                    break

            # If we have a client_order_id from the event, verify it matches
            if client_order_id_raw and tracked_order:
                # Convert Hummingbot order ID back to 32-bit integer for comparison
                expected_client_id = int(hashlib.md5(client_order_id.encode()).hexdigest()[:8], 16) % (2**32 - 1)
                if expected_client_id != client_order_id_raw:
                    self.logger().warning(f"Client ID mismatch: expected {expected_client_id}, got {client_order_id_raw}")

            if not tracked_order:
                self.logger().debug(f"Order {exchange_order_id} not found in tracked orders")
                return

            # Process trade events (fills)
            if event_type in ["orderFilled", "orderAccepted"] and event_data.get("t") is not None:
                await self._process_trade_fill(event_data, tracked_order)

            # Process order status updates
            if order_status in CONSTANTS.ORDER_STATE:
                timestamp = int(event_data.get("E", self._time_synchronizer.time() * 1000)) / 1000.0

                order_update = OrderUpdate(
                    trading_pair=trading_pair,
                    update_timestamp=timestamp,
                    new_state=CONSTANTS.ORDER_STATE[order_status],
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id,
                )

                self._order_tracker.process_order_update(order_update=order_update)
                self.logger().info(f"Processed order update: {client_order_id} -> {order_status}")

        except Exception as e:
            self.logger().error(f"Error processing order event: {e}", exc_info=True)

    async def _process_trade_fill(self, event_data: Dict[str, Any], tracked_order: InFlightOrder):
        """Process trade fill events from order updates."""
        try:
            trade_id = str(event_data.get("t", ""))
            if not trade_id or trade_id == "null":
                return  # No actual trade occurred

            fill_price = Decimal(str(event_data.get("p", "0")))
            last_fill_qty = Decimal(str(event_data.get("z", "0")))

            if last_fill_qty == 0:
                return  # No fill quantity

            # Calculate fill amounts
            fill_base_amount = last_fill_qty
            fill_quote_amount = fill_base_amount * fill_price

            # Extract fee information from WebSocket event (available in real Backpack format)
            fee_amount = Decimal(str(event_data.get("n", "0")))      # Fee amount
            fee_asset = event_data.get("N", tracked_order.quote_asset)  # Fee symbol

            if fee_amount > 0:
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=tracked_order.trade_type,
                    percent_token=fee_asset,
                    flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)]
                )
            else:
                # Fallback to default fee calculation if no fee info provided
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=tracked_order.trade_type,
                    percent_token=tracked_order.quote_asset,
                )

            # Create trade update
            trade_update = TradeUpdate(
                trade_id=trade_id,
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                fee=fee,
                fill_base_amount=fill_base_amount,
                fill_quote_amount=fill_quote_amount,
                fill_price=fill_price,
                fill_timestamp=int(event_data.get("T", self._time_synchronizer.time() * 1000)) / 1000.0,
            )

            self._order_tracker.process_trade_update(trade_update)
            self.logger().info(f"Processed trade fill: {tracked_order.client_order_id} - {fill_base_amount} @ {fill_price}")

        except Exception as e:
            self.logger().error(f"Error processing trade fill: {e}", exc_info=True)

    async def _process_position_event(self, event_data: Dict[str, Any]):
        """
        Process position/balance update events from Backpack WebSocket.

        Based on real Backpack position update events, this appears to be perpetual futures position data.
        For spot trading, we may not need to process these events for balance updates,
        as balance updates come through REST API calls.

        Real Backpack position event format:
        {
            "B": "164.59",            # Unknown field B
            "E": 1754373041730003,    # Event timestamp
            "M": "168.16672645",      # Mark price or similar
            "P": "0.035767",          # PnL or percentage
            "Q": "0.01",              # Quantity
            "T": 1754373041730004,    # Transaction timestamp
            "b": "164.6098",          # Bid or balance related
            "f": "0.02",              # Fee or funding
            "i": 4681711685,          # Position/instrument ID
            "l": "0",                 # Leverage or locked
            "m": "0.0125",            # Margin or similar
            "n": "1.6816672645",      # Notional value
            "p": "0",                 # Price or position
            "q": "0.01",              # Quantity (duplicate?)
            "s": "SOL_USDC_PERP"      # Symbol (note: PERP not SPOT)
        }
        """
        try:
            symbol = event_data.get("s", "")

            # Check if this is a perpetual futures position update (contains "_PERP")
            if "_PERP" in symbol:
                self.logger().debug(f"Received perpetual futures position update for {symbol}: {event_data}")
                # For spot trading connector, we typically don't process perpetual positions
                # These events are for futures trading, not spot balance updates
                return

            # For spot trading, balance updates usually come through different event types
            # or through REST API polling. The position events we're seeing appear to be
            # perpetual futures related.

            # If in the future Backpack sends spot balance updates through position events,
            # we can implement balance parsing here following the Binance pattern:
            #
            # Example implementation for balance updates (when available):
            # if "balances" in event_data:  # Hypothetical balance array
            #     for balance_info in event_data["balances"]:
            #         asset = balance_info.get("asset", "")
            #         available = Decimal(str(balance_info.get("available", "0")))
            #         locked = Decimal(str(balance_info.get("locked", "0")))
            #         total = available + locked
            #         self._account_available_balances[asset] = available
            #         self._account_balances[asset] = total
            #         self.logger().info(f"Updated balance for {asset}: available={available}, total={total}")

            self.logger().debug(f"Processed position event for {symbol} (no balance updates for spot trading)")

        except Exception as e:
            self.logger().error(f"Error processing position event: {e}", exc_info=True)
