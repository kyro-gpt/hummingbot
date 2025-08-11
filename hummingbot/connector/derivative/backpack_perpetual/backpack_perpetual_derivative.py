"""
Backpack Perpetual Derivative Connector

This module implements the main connector class for Backpack perpetual/futures trading.
It inherits from PerpetualDerivativePyBase and implements all required abstract methods
for position management, funding, and leverage handling.

Based on Backpack Exchange API with derivative-specific features:
- Position tracking and management
- Funding rate collection
- Leverage and margin management
- Enhanced order management with position actions
"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_order_book_data_source import (
    BackpackPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_user_stream_data_source import (
    BackpackPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class BackpackPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Backpack Perpetual Derivative Exchange Connector

    Implements perpetual/futures trading for Backpack Exchange with:
    - Position management and tracking
    - Funding rate collection
    - Leverage and margin management
    - Enhanced order management with position actions
    - Real-time position updates via WebSocket
    """

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        backpack_perpetual_api_key: str,
        backpack_perpetual_secret_key: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """
        Initialize the Backpack Perpetual Derivative connector.

        Args:
            client_config_map: Client configuration map
            backpack_perpetual_api_key: API key for authentication
            backpack_perpetual_secret_key: Secret key for Ed25519 signing
            trading_pairs: List of trading pairs to trade
            trading_required: Whether trading is required
            domain: Domain for the connector
        """
        self._api_key = backpack_perpetual_api_key
        self._secret_key = backpack_perpetual_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._last_position_update_timestamp = 0
        self._position_update_interval = 60  # Update positions every 60 seconds

        # Initialize authentication
        self._auth = BackpackPerpetualAuth(
            api_key=self._api_key,
            secret_key=self._secret_key
        )

        # Initialize parent class
        super().__init__(client_config_map)

        # Initialize web assistant factory
        self._web_assistants_factory = WebAssistantsFactory(
            auth=self._auth,
            throttler=self._throttler
        )

        # Initialize data structures
        self._trading_rules: Dict[str, TradingRule] = {}
        self._trading_pairs_exchanged_symbols = bidict()
        # Note: _trading_pair_symbol_map is inherited from base class - don't shadow it
        self._backup_symbol_map: Optional[bidict] = None  # Backup in case mapping gets reset
        self._last_traded_prices: Dict[str, float] = {}
        self._account_positions: Dict[str, Position] = {}  # Store position data by trading pair
        self._order_book_tracker = None  # Initialize order book tracker

    @property
    def name(self) -> str:
        """The name of the connector"""
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> BackpackPerpetualAuth:
        """The authenticator for this connector"""
        return self._auth

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        """Rate limit rules for the connector"""
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        """The domain of the connector"""
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        """Maximum length for client order IDs"""
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        """Prefix for client order IDs"""
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        """Path for trading rules request"""
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        """Path for trading pairs request"""
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        """Path for network connectivity check"""
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        """List of trading pairs"""
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        """Whether cancel requests are synchronous"""
        return True

    @property
    def is_trading_required(self) -> bool:
        """Whether trading is required"""
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        """Interval for polling funding fees in seconds"""
        return CONSTANTS.FUNDING_FEE_POLL_INTERVAL

    def supported_order_types(self) -> List[OrderType]:
        """List of supported order types"""
        return CONSTANTS.SUPPORTED_ORDER_TYPES

    def supported_position_modes(self) -> List[PositionMode]:
        """List of supported position modes"""
        return CONSTANTS.SUPPORTED_POSITION_MODES





    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Get the collateral token for buy orders"""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Get the collateral token for sell orders"""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        """Check if request exception is related to time synchronization"""
        error_description = str(request_exception)
        return "timestamp" in error_description.lower() or "time" in error_description.lower()

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """Check if status update exception indicates order not found"""
        return "404" in str(status_update_exception) or "not found" in str(status_update_exception).lower()



    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """Create web assistants factory with authentication"""
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth
        )

    def _get_order_book_tracker(self):
        """Create and return order book tracker."""
        if self._order_book_tracker is None:
            self._order_book_tracker = self._create_order_book_tracker()
        return self._order_book_tracker

    def _create_order_book_tracker(self):
        """Create order book tracker for derivative trading"""
        from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
        
        data_source = self._create_order_book_data_source()
        
        tracker = OrderBookTracker(
            data_source=data_source,
            trading_pairs=self._trading_pairs
        )
        return tracker

    def _create_order_book_data_source(self) -> PerpetualAPIOrderBookDataSource:
        """Create order book data source for perpetual trading"""
        return BackpackPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> BackpackPerpetualAPIUserStreamDataSource:
        """Create user stream data source"""
        return BackpackPerpetualAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = Decimal("NaN"),
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        """Calculate trading fees"""
        is_maker = is_maker or order_type is OrderType.LIMIT_MAKER
        trading_pair = combine_to_hb_trading_pair(base=base_currency, quote=quote_currency)

        if trading_pair in self._trading_fees:
            fees_data = self._trading_fees[trading_pair]
            fee_value = Decimal(fees_data.maker_percent_fee_decimal if is_maker else fees_data.taker_percent_fee_decimal)
        else:
            fee_value = Decimal(CONSTANTS.DEFAULT_FEES["maker"] if is_maker else CONSTANTS.DEFAULT_FEES["taker"])

        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema,
            position_action=PositionAction.OPEN,  # Default - will be updated during actual trading
            percent_token=quote_currency,
            flat_fees=[],
            percent=fee_value
        )
        return fee

    async def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        """Format trading rules from raw market information"""
        trading_rules = []

        for market_info in raw_trading_pair_info:
            try:
                # Only process perpetual/future markets for derivatives
                market_type = market_info.get("marketType", "").lower()
                if market_type not in ["future", "perpetual", "perp"]:
                    continue

                symbol = market_info["symbol"]

                # Convert symbol format
                if symbol.endswith("_PERP"):
                    base_pair = symbol[:-5]  # Remove "_PERP"
                    trading_pair = base_pair.replace("_", "-")
                else:
                    # Skip non-perpetual symbols
                    continue

                # Extract trading rule parameters
                filters = market_info.get("filters", {})
                price_filter = filters.get("price", {})
                quantity_filter = filters.get("quantity", {})

                # Get min/max values with proper defaults and safe conversion
                def safe_decimal(value, default):
                    try:
                        if value is None:
                            return Decimal(str(default))
                        return Decimal(str(value))
                    except (ValueError, TypeError, Exception):
                        return Decimal(str(default))

                min_order_size = safe_decimal(quantity_filter.get("minQuantity"), "0.001")
                max_order_size = safe_decimal(quantity_filter.get("maxQuantity"), "1000000")
                step_size = safe_decimal(quantity_filter.get("stepSize"), "0.001")

                min_price_increment = safe_decimal(price_filter.get("tickSize"), "0.01")
                min_notional_size = safe_decimal(price_filter.get("minPrice"), "1.0")
                
                # Extract quote asset for collateral token
                base_symbol = market_info.get("baseSymbol", "")
                quote_symbol = market_info.get("quoteSymbol", "")
                collateral_token = quote_symbol  # For Backpack perpetuals, collateral is the quote asset

                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    max_order_size=max_order_size,
                    min_price_increment=min_price_increment,
                    min_base_amount_increment=step_size,
                    min_notional_size=min_notional_size,
                    buy_order_collateral_token=collateral_token,
                    sell_order_collateral_token=collateral_token,
                )

                trading_rules.append(trading_rule)

                # Store the symbol mapping
                self._trading_pairs_exchanged_symbols[trading_pair] = symbol

            except Exception as e:
                self.logger().error(f"Error parsing trading rule for {market_info.get('symbol', 'unknown')}: {e}")
                continue

        return trading_rules

    async def _update_trading_rules(self):
        """
        Update trading rules without resetting symbol mapping.
        
        CRITICAL: Unlike the base class, we don't call _initialize_trading_pair_symbols_from_exchange_info()
        here because we already initialized it in start_network() and don't want to reset the mapping.
        """
        try:
            # Get trading rules data using same endpoint as symbol mapping
            exchange_info = await self._make_trading_pairs_request()
            trading_rules_list = await self._format_trading_rules(exchange_info)
            
            # Clear and update trading rules
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule
                

            
        except Exception as e:
            self.logger().error(f"Error updating trading rules: {e}", exc_info=True)
            # Don't raise exception to avoid blocking connector initialization

    async def _update_trading_fees(self):
        """Update trading fees - using default values for derivatives"""
        pass

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
                    # RFQ updates are not needed for derivative trading, but we log them for debugging

                else:
                    self.logger().debug(f"Unhandled user stream event: {event_message}")

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _process_order_event(self, event_data: Dict[str, Any]):
        """Process order update events from user stream"""
        try:
            # Extract order information from Backpack event
            client_order_id = str(event_data.get("c", ""))  # Client order ID (32-bit integer as string)
            exchange_order_id = event_data.get("i", "")  # Exchange order ID
            event_type = event_data.get("e", "")  # Event type (orderAccepted, orderFilled, etc.)
            order_state = event_data.get("X", "")  # Order state (New, Filled, Cancelled, etc.)

            # Find tracked order
            tracked_order = None
            if client_order_id:
                tracked_order = self._order_tracker.fetch_order(client_order_id=client_order_id)

            if not tracked_order and exchange_order_id:
                tracked_order = self._order_tracker.fetch_order(exchange_order_id=exchange_order_id)

            if not tracked_order:
                self.logger().debug(f"Order not tracked, ignoring event: {event_data}")
                return

            # Create order update
            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=CONSTANTS.ORDER_STATE.get(order_state, OrderState.OPEN),
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=exchange_order_id,
            )

            # Process trade fills if applicable
            if event_type == "orderFilled" or order_state == "Filled":
                await self._process_trade_fill_event(event_data, tracked_order)

            # Update order tracker
            self._order_tracker.process_order_update(order_update)

        except Exception as e:
            self.logger().error(f"Error processing order event: {e}", exc_info=True)

    async def _process_position_event(self, event_data: Dict[str, Any]):
        """Process position update events from user stream"""
        try:            
            # Extract position information from Backpack event
            # Reference format from user stream data source:
            # {
            #     "B": "164.59",            # Entry price
            #     "E": 1754462601563677,    # Event time in microseconds
            #     "M": "163.81034805",      # Mark price
            #     "P": "-0.007796",         # PnL unrealized
            #     "Q": "0.01",              # Net exposure quantity
            #     "T": 1754462601563678,    # Engine timestamp in microseconds
            #     "b": "164.6398",          # Break even price
            #     "f": "0.02",              # Initial margin fraction
            #     "i": 4681711685,          # Position ID
            #     "l": "0",                 # Estimated liquidation price
            #     "m": "0.0125",            # Maintenance margin fraction
            #     "n": "1.6381034805",      # Net exposure notional
            #     "p": "0",                 # PnL realized
            #     "q": "0.01",              # Net quantity
            #     "s": "SOL_USDC_PERP"      # Symbol
            # }

            symbol = event_data.get("s", "")
            if not symbol:
                return

            # Convert symbol to trading pair
            trading_pair = web_utils.convert_from_exchange_trading_pair(symbol)

            amount = Decimal(str(event_data.get("q", "0")))  # Net quantity
            entry_price = Decimal(str(event_data.get("B", "0")))  # Entry price
            mark_price = Decimal(str(event_data.get("M", "0")))  # Mark price
            pnl_unrealized = Decimal(str(event_data.get("P", "0")))  # PnL unrealized

            # Determine position side
            position_side = PositionSide.LONG if amount > 0 else PositionSide.SHORT if amount < 0 else None

            if position_side is not None and amount != 0:
                # Use configured leverage (5x) for now - IMF calculation was causing issues
                leverage = Decimal("5")
                
                # Create or update position
                position = Position(
                    trading_pair=trading_pair,
                    position_side=position_side,
                    unrealized_pnl=pnl_unrealized,
                    entry_price=entry_price,
                    amount=abs(amount),
                    leverage=leverage,
                )

                # Update position tracker
                self.set_position(trading_pair, position)

                self.logger().debug(f"Updated position: {trading_pair} {position_side} {amount} @ {entry_price} (PnL: {pnl_unrealized})")
            else:
                # Position closed
                self.set_position(trading_pair, None)
                self.logger().debug(f"Position closed: {trading_pair}")

        except Exception as e:
            self.logger().error(f"Error processing position event: {e}", exc_info=True)

    async def _process_trade_fill_event(self, event_data: Dict[str, Any], tracked_order: InFlightOrder):
        """Process trade fill events"""
        try:
            # Extract trade information
            trade_id = event_data.get("t", "")
            fill_quantity = Decimal(str(event_data.get("z", "0")))  # Last filled quantity
            fill_price = Decimal(str(event_data.get("p", "0")))  # Price
            fee_amount = Decimal(str(event_data.get("n", "0")))  # Fee amount
            fee_asset = event_data.get("N", "")  # Fee symbol

            if fill_quantity > 0:
                # Create trade update
                trade_update = TradeUpdate(
                    trade_id=trade_id,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                    trading_pair=tracked_order.trading_pair,
                    fee=DeductedFromReturnsTradeFee([TokenAmount(amount=fee_amount, token=fee_asset)]),
                    fill_base_amount=fill_quantity,
                    fill_quote_amount=fill_quantity * fill_price,
                    fill_price=fill_price,
                    fill_timestamp=time.time(),
                )

                # Update order tracker
                self._order_tracker.process_trade_update(trade_update)

        except Exception as e:
            self.logger().error(f"Error processing trade fill event: {e}", exc_info=True)

    # Abstract methods from PerpetualDerivativePyBase

    async def _update_positions(self):
        """Update positions from REST API"""
        try:
            # Rate limit check
            current_time = time.time()
            if current_time - self._last_position_update_timestamp < self._position_update_interval:
                return

            # Request position data
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()

            url = web_utils.get_rest_url_for_endpoint(
                endpoint=CONSTANTS.POSITION_PATH_URL,
                domain=self._domain
            )

            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.POSITION_PATH_URL,
                is_auth_required=True,
            )

            # Process position data
            if isinstance(response, list):
                for position_data in response:
                    await self._process_position_data(position_data)
            else:
                self.logger().warning(f"Unexpected position response format: {type(response)}")

            self._last_position_update_timestamp = time.time()

        except Exception as e:
            self.logger().error(f"Error updating positions: {e}", exc_info=True)

    async def _process_position_data(self, position_data: Dict[str, Any]):
        """Process individual position data from API response"""
        try:
            # Extract position information from Backpack API response
            # Based on FuturePositionWithMargin schema:
            symbol = position_data.get("symbol", "")
            net_quantity = Decimal(str(position_data.get("netQuantity", "0")))
            entry_price = Decimal(str(position_data.get("entryPrice", "0")))
            break_even_price = Decimal(str(position_data.get("breakEvenPrice", "0")))
            pnl_unrealized = Decimal(str(position_data.get("pnlUnrealized", "0")))
            pnl_realized = Decimal(str(position_data.get("pnlRealized", "0")))
            mark_price = Decimal(str(position_data.get("markPrice", "0")))
            liquidation_price = Decimal(str(position_data.get("estLiquidationPrice", "0")))
            
            # Convert symbol to trading pair
            trading_pair = web_utils.convert_from_exchange_trading_pair(symbol)

            # Determine position side
            if net_quantity > 0:
                position_side = PositionSide.LONG
                amount = net_quantity
            elif net_quantity < 0:
                position_side = PositionSide.SHORT
                amount = abs(net_quantity)
            else:
                # No position
                self.set_position(trading_pair, None)
                return

            # Use configured leverage (5x) 
            leverage = Decimal("5")

            # Create position object
            position = Position(
                trading_pair=trading_pair,
                position_side=position_side,
                unrealized_pnl=pnl_unrealized,
                entry_price=entry_price,
                amount=amount,
                leverage=leverage,
            )

            # Update position tracker
            self.set_position(trading_pair, position)

            self.logger().debug(f"Updated position from API: {trading_pair} {position_side} {amount} @ {entry_price}")

        except Exception as e:
            self.logger().error(f"Error processing position data: {e}", exc_info=True)

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """Set leverage for a trading pair"""
        try:
            # Backpack leverage setting is not yet implemented
            # Return success to avoid error logging, but inform user via message
            # TODO: Implement actual leverage setting API when available
            return True, f"Leverage setting not yet implemented for Backpack (requested: {leverage})"
        except Exception as e:
            error_msg = f"Error setting leverage for {trading_pair}: {e}"
            self.logger().error(error_msg, exc_info=True)
            return False, error_msg

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        """Set position mode for a trading pair"""
        try:
            # Backpack position mode setting is not yet implemented
            # Return success to avoid error logging, but inform user via message  
            # TODO: Implement actual position mode setting API when available
            return True, f"Position mode setting not yet implemented for Backpack (requested: {mode})"
        except Exception as e:
            error_msg = f"Error setting position mode for {trading_pair}: {e}"
            self.logger().error(error_msg, exc_info=True)
            return False, error_msg

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        """Fetch the last funding fee payment"""
        try:
            # Request funding payment history
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()

            url = web_utils.get_rest_url_for_endpoint(
                endpoint=CONSTANTS.HISTORY_FUNDING_PATH_URL,
                domain=self._domain
            )

            # Convert trading pair to exchange symbol
            symbol = web_utils.convert_to_exchange_trading_pair(trading_pair)

            params = {
                "symbol": symbol,
                "limit": 1,  # Get only the latest payment
            }

            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                params=params,
                throttler_limit_id=CONSTANTS.HISTORY_FUNDING_PATH_URL,
                is_auth_required=True,
            )

            if isinstance(response, list) and len(response) > 0:
                payment_data = response[0]
                timestamp = int(payment_data.get("intervalEndTimestamp", "0"))
                funding_rate = Decimal(str(payment_data.get("fundingRate", "0")))
                payment_amount = Decimal(str(payment_data.get("quantity", "0")))

                return timestamp, funding_rate, payment_amount
            else:
                # No funding payments found
                return 0, Decimal("0"), Decimal("0")

        except Exception as e:
            self.logger().error(f"Error fetching funding payment for {trading_pair}: {e}", exc_info=True)
            return 0, Decimal("0"), Decimal("0")

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """Get the last traded price for a derivative trading pair."""
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            params = {"symbol": symbol}

            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            url = web_utils.get_rest_url_for_endpoint(
                endpoint=CONSTANTS.TICKER_PATH_URL,
                domain=self._domain
            )

            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                params=params,
                throttler_limit_id=CONSTANTS.TICKER_PATH_URL,
            )

            # For derivative markets, use mark price if available, otherwise last price
            if "markPrice" in response:
                return float(response["markPrice"])
            else:
                return float(response.get("lastPrice", 0))

        except Exception as e:
            self.logger().error(f"Error fetching last traded price for {trading_pair}: {e}", exc_info=True)
            return 0.0

    # Order management methods (inherited and enhanced)

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.OPEN,
        **kwargs,
    ) -> Tuple[str, float]:
        """Place order with position action support"""
        

        
        # Convert trading pair to exchange symbol
        symbol = web_utils.convert_to_exchange_trading_pair(trading_pair)

        # Build order data
        order_data = {
            "symbol": symbol,
            "side": CONSTANTS.BACKPACK_ORDER_SIDE[trade_type.name],
            "orderType": CONSTANTS.BACKPACK_ORDER_TYPE[order_type],
            "quantity": str(amount),
            "clientId": self._generate_client_order_id(order_id),
        }

        # Add price for limit orders
        if order_type != OrderType.MARKET:
            order_data["price"] = str(price)

        # Add time in force
        order_data["timeInForce"] = "GTC"  # Default to Good Till Canceled

        # Execute request
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()

        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.ORDER_PATH_URL,
            domain=self._domain
        )

        response = await rest_assistant.execute_request(
            url=url,
            method=RESTMethod.POST,
            data=order_data,
            throttler_limit_id=CONSTANTS.ORDER_PATH_URL,
            is_auth_required=True,
        )

        # Extract exchange order ID
        exchange_order_id = response.get("id", "")

        return exchange_order_id, time.time()

    def _generate_client_order_id(self, order_id: str) -> int:
        """Generate a 32-bit client order ID from Hummingbot order ID"""
        import hashlib

        # Create MD5 hash of order ID and convert to 32-bit integer
        hash_bytes = hashlib.md5(order_id.encode()).digest()
        client_id = int.from_bytes(hash_bytes[:4], byteorder='big')
        return client_id

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """Check if the cancellation exception indicates that the order was not found"""
        error_str = str(cancelation_exception)
        # Check for Backpack-specific order not found patterns
        # Exception format: "Error executing request DELETE... Error: {"code":"INVALID_CLIENT_REQUEST","message":"Order not found"}"
        return (CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE in error_str and 
                CONSTANTS.ORDER_NOT_FOUND_MESSAGE in error_str)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            "clientId": self._generate_client_order_id(tracked_order.client_order_id),
            "symbol": symbol
        }
        try:
            cancel_result = await self._api_delete(
                path_url=CONSTANTS.ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)
            return True
        except Exception as e:
            error_str = str(e)
            # Check if this is a "Order not found" error in the exception message
            if (CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE in error_str and
                CONSTANTS.ORDER_NOT_FOUND_MESSAGE in error_str):
                self.logger().debug(f"The order {order_id} does not exist on Backpack Perpetuals. "
                                    f"No cancelation needed.")
                await self._order_tracker.process_order_not_found(order_id)
                # Reconstruct error message similar to Binance pattern
                raise IOError(f"{CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE} - {CONSTANTS.ORDER_NOT_FOUND_MESSAGE}")
            # Re-raise other exceptions
            raise

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """Request order status from exchange"""
        try:
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()

            url = web_utils.get_rest_url_for_endpoint(
                endpoint=CONSTANTS.ORDER_PATH_URL,
                domain=self._domain
            )

            # Query by client order ID and symbol (required for derivative API)
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
            params = {
                "clientId": self._generate_client_order_id(tracked_order.client_order_id),
                "symbol": symbol
            }

            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                params=params,
                throttler_limit_id=CONSTANTS.ORDER_PATH_URL,
                is_auth_required=True,
            )

            # Parse response
            order_state = CONSTANTS.ORDER_STATE.get(response.get("status", ""), OrderState.OPEN)
            exchange_order_id = response.get("id", tracked_order.exchange_order_id)

            return OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=order_state,
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=exchange_order_id,
            )

        except Exception as e:
            if "404" in str(e):
                # Order not found - likely cancelled or filled
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=time.time(),
                    new_state=OrderState.CANCELED,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )
            else:
                self.logger().error(f"Error requesting order status for {tracked_order.client_order_id}: {e}")
                raise

    async def _request_account_balances(self):
        """Request account balances from Backpack API (for debugging)"""
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()

        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.CAPITAL_PATH_URL,
            domain=self._domain
        )

        response = await rest_assistant.execute_request(
            url=url,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.CAPITAL_PATH_URL,
            is_auth_required=True,
        )
        
        return response

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

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """Get all trade updates for a specific order"""
        try:
            # Query trade history for this order
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()

            url = web_utils.get_rest_url_for_endpoint(
                endpoint=CONSTANTS.HISTORY_FILLS_PATH_URL,
                domain=self._domain
            )

            # Convert trading pair to exchange symbol
            symbol = web_utils.convert_to_exchange_trading_pair(order.trading_pair)

            params = {
                "symbol": symbol,
                "orderId": order.exchange_order_id,  # Query by exchange order ID
            }

            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                params=params,
                throttler_limit_id=CONSTANTS.HISTORY_FILLS_PATH_URL,
                is_auth_required=True,
            )

            trade_updates = []
            if isinstance(response, list):
                for fill_data in response:
                    trade_id = fill_data.get("id", "")
                    fill_quantity = Decimal(str(fill_data.get("quantity", "0")))
                    fill_price = Decimal(str(fill_data.get("price", "0")))
                    fee_amount = Decimal(str(fill_data.get("fee", "0")))
                    fee_asset = fill_data.get("feeSymbol", "")
                    fill_timestamp = int(fill_data.get("timestamp", 0)) / 1000  # Convert to seconds

                    trade_update = TradeUpdate(
                        trade_id=trade_id,
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        fee=DeductedFromReturnsTradeFee([TokenAmount(amount=fee_amount, token=fee_asset)]),
                        fill_base_amount=fill_quantity,
                        fill_quote_amount=fill_quantity * fill_price,
                        fill_price=fill_price,
                        fill_timestamp=fill_timestamp,
                    )
                    trade_updates.append(trade_update)

            return trade_updates

        except Exception as e:
            self.logger().error(f"Error fetching trade updates for order {order.client_order_id}: {e}", exc_info=True)
            return []

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """
        Get exchange symbol for trading pair with backup fallback.
        
        If the main symbol map is empty (due to reset), use the backup.
        """
        try:
            # First try the base class method
            return await super().exchange_symbol_associated_to_pair(trading_pair)
        except KeyError:
            # If that fails and we have a backup, use it
            if self._backup_symbol_map and trading_pair in self._backup_symbol_map.inverse:
                symbol = self._backup_symbol_map.inverse[trading_pair]
                print(f"🔧 DEBUG: Used backup mapping: {trading_pair} -> {symbol}")  # TODO: Remove after debugging
                return symbol
            else:
                backup_size = len(self._backup_symbol_map) if self._backup_symbol_map else 0
                print(f"🔧 DEBUG: No backup mapping for {trading_pair}. Backup: {backup_size}")  # TODO: Remove after debugging
                raise KeyError(f"Trading pair {trading_pair} not found in symbol mapping")

    async def start_network(self):
        """
        Start all required tasks to update the status of the connector.
        This method is critical for proper order book initialization.
        """
        # CRITICAL: Initialize trading pair symbol mapping FIRST
        await self._initialize_trading_pair_symbol_map()
        
        # Verify symbol mapping is ready before proceeding
        if not self.trading_pair_symbol_map_ready():
            raise ValueError("Trading pair symbol mapping not initialized properly")
            

        
        # Then initialize trading rules (depends on symbol mapping)
        await self._update_trading_rules()
        
        # Finally start the parent network (creates order book tracker)
        await super().start_network()

    async def _make_network_check_request(self):
        """
        Override network check to handle Backpack's plain text 'pong' response.

        Backpack's /api/v1/ping returns plain text "pong", not JSON.
        We need to use the REST assistant directly to avoid JSON parsing.
        """
        try:
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            url = web_utils.public_rest_url(path_url=self.check_network_request_path, domain=self._domain)

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

    async def _make_trading_pairs_request(self) -> Any:
        """Make request to get trading pairs information"""
        try:
            # Use the markets endpoint to get all perpetual markets
            exchange_info = await self._api_get(
                path_url=CONSTANTS.MARKETS_PATH_URL,
                is_auth_required=False
            )
            return exchange_info
        except Exception as e:
            self.logger().error(f"Error making trading pairs request: {e}", exc_info=True)
            raise

    async def _initialize_trading_pair_symbol_map(self):
        """Initialize trading pair symbol mapping"""
        try:
            print(f"🔧 DEBUG: _initialize_trading_pair_symbol_map() called")  # TODO: Remove after debugging
            exchange_info = await self._make_trading_pairs_request()
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
            # Symbol mapping completed successfully
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        """Initialize trading pair symbols from exchange info - simplified like Binance"""
        mapping = bidict()
        
        # Filter for valid perpetual markets and add ALL to mapping (no filtering by _trading_pairs)
        for market_info in filter(self._is_valid_perpetual_market, exchange_info):
            exchange_symbol = market_info["symbol"]  # e.g., "SOL_USDC_PERP"
            base = market_info["baseSymbol"]  # e.g., "SOL"
            quote = market_info["quoteSymbol"]  # e.g., "USDC"
            trading_pair = f"{base}-{quote}"  # e.g., "SOL-USDC"
            
            # Add to mapping without any filtering (like Binance Perpetual)
            mapping[exchange_symbol] = trading_pair
            
        print(f"🔧 DEBUG: Created mapping with {len(mapping)} perpetual pairs")  # TODO: Remove after debugging
        if len(mapping) > 0:
            sample_items = dict(list(mapping.items())[:3])
            print(f"🔧 DEBUG: Sample mappings: {sample_items}")  # TODO: Remove after debugging
        
        # Set the mapping using base class method (just like Backpack spot)
        self._set_trading_pair_symbol_map(mapping)
        self._backup_symbol_map = mapping.copy()  # Create backup

    def _is_valid_perpetual_market(self, market_info: Dict[str, Any]) -> bool:
        """Check if market info represents a valid perpetual market"""
        try:
            symbol = market_info.get("symbol", "unknown")
            
            # Must have all required fields
            required_fields = ["symbol", "baseSymbol", "quoteSymbol", "marketType", "orderBookState"]
            if not all(field in market_info for field in required_fields):
                missing_fields = [f for f in required_fields if f not in market_info]
                print(f"🔧 DEBUG: Market {symbol} missing fields: {missing_fields}")  # TODO: Remove after debugging
                return False
            
            # Must be a perpetual market
            market_type = market_info.get("marketType", "")
            if market_type.upper() not in ["PERP", "PERPETUAL", "FUTURE"]:
                # print(f"🔧 DEBUG: Market {symbol} has invalid market type: '{market_type}'")  # TODO: Remove after debugging
                return False
                
            # Must have _PERP suffix
            if not symbol.endswith("_PERP"):
                print(f"🔧 DEBUG: Market {symbol} does not end with _PERP")  # TODO: Remove after debugging
                return False
                
            # Must be open for trading
            order_book_state = market_info.get("orderBookState", "")
            if order_book_state.upper() != "OPEN":
                print(f"🔧 DEBUG: Market {symbol} is not open: '{order_book_state}'")  # TODO: Remove after debugging
                return False
                
            # print(f"🔧 DEBUG: Market {symbol} is VALID perpetual market")  # TODO: Remove after debugging
            return True
        except Exception as e:
            print(f"🔧 DEBUG: Error validating market {market_info.get('symbol', 'unknown')}: {e}")  # TODO: Remove after debugging
            return False

    def get_position(self, trading_pair: str) -> Optional[Position]:
        """Get position for a trading pair"""
        return self._account_positions.get(trading_pair)

    def set_position(self, trading_pair: str, position: Optional[Position]):
        """Set position for a trading pair"""
        if position is None or position.amount == 0:
            # Remove closed positions
            self._account_positions.pop(trading_pair, None)
            # Also remove from perpetual trading object
            pos_key = self._perpetual_trading.position_key(trading_pair, PositionSide.LONG)
            self._perpetual_trading.remove_position(pos_key)
            pos_key = self._perpetual_trading.position_key(trading_pair, PositionSide.SHORT)
            self._perpetual_trading.remove_position(pos_key)
        else:
            # Store in both our local dict and the perpetual trading object
            self._account_positions[trading_pair] = position
            pos_key = self._perpetual_trading.position_key(trading_pair, position.position_side)
            self._perpetual_trading.set_position(pos_key, position)
