"""
Lighter Perpetual Derivative Connector

This module implements the main connector class for Lighter perpetual/futures trading.
It inherits from PerpetualDerivativePyBase and implements all required abstract methods
for position management, funding, and leverage handling.

Based on Lighter Exchange API (Layer-2 Ethereum) with derivative-specific features:
- Private key authentication (similar to Hyperliquid)
- Account index system
- Position tracking and management
- Funding rate collection
- Transaction-based order management
- Enhanced order management with position actions
"""

import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_0, s_decimal_NaN
from hummingbot.connector.derivative.lighter_perpetual import (
    lighter_perpetual_constants as CONSTANTS,
    lighter_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_order_book_data_source import (
    LighterPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_user_stream_data_source import (
    LighterPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_auth import LighterPerpetualAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class LighterPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Lighter Perpetual Derivative Exchange Connector

    Implements perpetual/futures trading for Lighter Exchange (Layer-2 Ethereum) with:
    - Private key authentication and account index system
    - Position management and tracking
    - Funding rate collection
    - Transaction-based order management
    - Real-time position updates via WebSocket
    - Enhanced error handling and retry logic
    """

    web_utils = web_utils
    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 120.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        lighter_perpetual_private_key: str,
        lighter_perpetual_account_index: int,
        lighter_perpetual_api_key_index: int = 0,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """
        Initialize the Lighter Perpetual Derivative connector.

        Args:
            client_config_map: Client configuration map
            lighter_perpetual_private_key: Ethereum private key for signing
            lighter_perpetual_account_index: Account index on Lighter
            lighter_perpetual_api_key_index: API key index (default: 0)
            trading_pairs: List of trading pairs to trade
            trading_required: Whether trading is required
            domain: Domain for the connector
        """
        self._private_key = lighter_perpetual_private_key
        self._account_index = lighter_perpetual_account_index
        self._api_key_index = lighter_perpetual_api_key_index
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        
        # Position and order management
        self._last_position_update_timestamp = 0
        self._position_update_interval = 60  # Update positions every 60 seconds
        self._last_trade_history_timestamp = None

        # Initialize authentication
        self._auth = LighterPerpetualAuth(
            private_key=self._private_key,
            account_index=self._account_index,
            api_key_index=self._api_key_index
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
        self._last_traded_prices: Dict[str, float] = {}
        self._account_positions: Dict[str, Position] = {}  # Store position data by trading pair
        self._account_balances: Dict[str, Decimal] = {}  # Store account balances
        
        # Market data mapping (market_id to trading_pair)
        self._market_id_to_trading_pair: Dict[int, str] = {}
        self._trading_pair_to_market_id: Dict[str, int] = {}

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def name(self) -> str:
        """The name of the connector"""
        return self._domain

    @property
    def authenticator(self) -> LighterPerpetualAuth:
        """Returns the authenticator for API requests"""
        return self._auth

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        """Returns the rate limits for the exchange"""
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        """Returns the domain"""
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        """Maximum length for client order IDs"""
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        """Prefix for client order IDs"""
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        """Path for trading rules request"""
        return CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        """Path for trading pairs request"""
        return CONSTANTS.ORDER_BOOKS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        """Path for network check request"""
        return CONSTANTS.INFO_PATH_URL

    @property
    def trading_pairs(self):
        """Returns the trading pairs"""
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
        """Funding fee polling interval in seconds"""
        return 120

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        """
        This method needs to be overridden to provide the accurate information depending on the exchange.
        """
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Returns the collateral token for buy orders"""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Returns the collateral token for sell orders"""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        """Check if request exception is related to time synchronization"""
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """Create web assistants factory"""
        return WebAssistantsFactory(
            auth=self._auth,
            throttler=self._throttler
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """Create order book data source"""
        return LighterPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """Create user stream data source"""
        return LighterPerpetualAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    async def _make_network_check_request(self):
        """Make network check request"""
        await self._api_get(path_url=self.check_network_request_path)

    async def _status_polling_loop_fetch_updates(self):
        """Fetch updates in status polling loop"""
        await safe_gather(
            self._update_trade_history(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_order_status(self):
        """Update order status"""
        # TODO: Implement order status updates
        pass

    async def _update_lost_orders_status(self):
        """Update lost orders status"""
        # TODO: Implement lost orders status updates
        pass

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        """
        Place an order on the exchange using Lighter's sendTx API
        
        :param order_id: Client order ID
        :param trading_pair: Trading pair
        :param amount: Order amount
        :param trade_type: BUY or SELL
        :param order_type: Order type
        :param price: Order price
        :param position_action: Position action
        :return: Tuple of exchange order ID and timestamp
        """
        try:
            # Get market ID for trading pair
            market_id = self._get_market_id_for_trading_pair(trading_pair)
            
            # Convert client order ID to integer (Lighter uses integer client_order_index)
            client_order_index = self._auth.get_client_order_index(order_id)
            
            # Convert amounts to micro units (Lighter uses 1e6 precision)
            base_amount_micro = int(amount * Decimal("1e6"))
            price_micro = int(price * Decimal("1e6")) if order_type != OrderType.MARKET else 0
            
            # Map order type to Lighter format
            lighter_order_type = 0 if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER] else 1  # LIMIT = 0, MARKET = 1
            
            # Prepare transaction info
            tx_info = {
                "market_index": market_id,
                "client_order_index": client_order_index,
                "base_amount": base_amount_micro,
                "price": price_micro,
                "is_ask": trade_type == TradeType.SELL,
                "order_type": lighter_order_type,
                "time_in_force": 3,  # GTT = 3 (Good Till Time)
                "reduce_only": 0,    # Not reduce only by default
                "trigger_price": 0   # No trigger price for regular orders
            }
            
            # Send transaction using authentication
            result = await self._auth.send_tx(
                tx_type=CONSTANTS.TX_TYPE_CREATE_ORDER,
                tx_info=tx_info
            )
            
            # Extract exchange order ID and timestamp from response
            exchange_order_id = result.get("order_id", str(client_order_index))
            timestamp = time.time()
            
            self.logger().info(f"Order placed successfully: {order_id} -> {exchange_order_id}")
            
            return exchange_order_id, timestamp
            
        except Exception as e:
            self.logger().error(f"Failed to place order {order_id}: {e}")
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel an order on the exchange using Lighter's sendTx API
        
        :param order_id: Client order ID
        :param tracked_order: Tracked order object
        """
        try:
            # Get market ID for trading pair
            market_id = self._get_market_id_for_trading_pair(tracked_order.trading_pair)
            
            # Get client order index
            client_order_index = self._auth.get_client_order_index(order_id)
            
            # Prepare transaction info for cancellation
            tx_info = {
                "market_index": market_id,
                "client_order_index": client_order_index
            }
            
            # Send cancellation transaction
            result = await self._auth.send_tx(
                tx_type=CONSTANTS.TX_TYPE_CANCEL_ORDER,
                tx_info=tx_info
            )
            
            self.logger().info(f"Order cancellation sent: {order_id} (client_order_index: {client_order_index})")
            
            return result
            
        except Exception as e:
            self.logger().error(f"Failed to cancel order {order_id}: {e}")
            raise

    async def _update_positions(self):
        """
        Update account positions parsing position data from account response
        """
        try:
            # Get account information from Lighter API
            account_info = await self._api_get(
                path_url=CONSTANTS.ACCOUNT_PATH_URL,
                params={"account_index": self._account_index}
            )
            
            # Parse position information
            if "positions" in account_info:
                positions_data = account_info["positions"]
                
                # Clear existing positions
                self._account_positions.clear()
                
                # Process each position
                for position_data in positions_data:
                    market_id = position_data.get("market_index")
                    if market_id is None:
                        continue
                        
                    # Get trading pair for market ID
                    trading_pair = self._get_trading_pair_for_market_id(market_id)
                    
                    # Extract position details
                    base_amount = Decimal(str(position_data.get("base_amount", "0"))) / Decimal("1e6")  # Convert from micro units
                    quote_amount = Decimal(str(position_data.get("quote_amount", "0"))) / Decimal("1e6")
                    
                    # Skip zero positions
                    if base_amount == 0:
                        continue
                    
                    # Determine position side
                    position_side = PositionSide.LONG if base_amount > 0 else PositionSide.SHORT
                    
                    # Create position object
                    position = Position(
                        trading_pair=trading_pair,
                        position_side=position_side,
                        unrealized_pnl=Decimal(str(position_data.get("unrealized_pnl", "0"))) / Decimal("1e6"),
                        entry_price=Decimal(str(position_data.get("entry_price", "0"))) / Decimal("1e6"),
                        amount=abs(base_amount),
                        leverage=Decimal("1")  # Lighter handles leverage internally
                    )
                    
                    self._account_positions[trading_pair] = position
                    
                self.logger().debug(f"Updated {len(self._account_positions)} positions")
                
            else:
                self.logger().debug("No positions data found in account response")
                
        except Exception as e:
            self.logger().error(f"Failed to update positions: {e}")
            # Don't raise exception to avoid breaking the polling loop

    async def _trading_pair_position_mode_set(
        self, mode: PositionMode, trading_pair: str
    ) -> Tuple[bool, str]:
        """
        Set position mode for trading pair
        
        :param mode: Position mode to set
        :param trading_pair: Trading pair
        :return: Tuple of success boolean and error message
        """
        # Lighter only supports ONEWAY mode
        if mode == PositionMode.ONEWAY:
            return True, ""
        else:
            return False, f"Position mode {mode} not supported. Only ONEWAY mode is supported."

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Set leverage for trading pair
        
        :param trading_pair: Trading pair
        :param leverage: Leverage to set
        :return: Tuple of success boolean and error message
        """
        # TODO: Implement leverage setting if supported by Lighter
        # For now, return success as leverage might be handled automatically
        return True, ""

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        """
        Fetch the last funding fee payment
        
        :param trading_pair: Trading pair
        :return: Tuple of timestamp, funding rate, and payment amount
        """
        # TODO: Implement funding payment fetching from Lighter API
        # Return default values for now
        return 0.0, s_decimal_0, s_decimal_0

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculate trading fee for an order
        
        :param base_currency: Base currency
        :param quote_currency: Quote currency
        :param order_type: Order type
        :param order_side: Order side (BUY/SELL)
        :param position_action: Position action
        :param amount: Order amount
        :param price: Order price
        :param is_maker: Whether the order is a maker order
        :return: Trading fee
        """
        # Use default fee structure for now
        # TODO: Get actual fee rates from Lighter API
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        fee_rate = Decimal("0.0005") if is_maker else Decimal("0.001")  # 0.05% maker, 0.1% taker
        
        return DeductedFromReturnsTradeFee(
            percent=fee_rate,
            flat_fees=[TokenAmount(amount=s_decimal_0, token=quote_currency)]
        )

    # Helper methods for market ID mapping
    def _get_market_id_for_trading_pair(self, trading_pair: str) -> int:
        """Get market ID for trading pair"""
        # First try internal mapping, fallback to web_utils
        if trading_pair in self._trading_pair_to_market_id:
            return self._trading_pair_to_market_id[trading_pair]
        return web_utils.format_trading_pair_to_market_id(trading_pair)

    def _get_trading_pair_for_market_id(self, market_id: int) -> str:
        """Get trading pair for market ID"""
        # First try internal mapping, fallback to web_utils
        if market_id in self._market_id_to_trading_pair:
            return self._market_id_to_trading_pair[market_id]
        return web_utils.format_market_id_to_trading_pair(market_id)

    async def _initialize_trading_pair_symbol_map(self):
        """Initialize trading pair symbol mapping"""
        try:
            # Get market information from Lighter API
            response = await self._api_get(path_url=self.trading_pairs_request_path)
            
            # TODO: Parse response and build symbol mapping
            # This will depend on the actual API response format
            pass
            
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    async def _update_trading_rules(self):
        """Update trading rules from exchange"""
        try:
            # Get trading rules from Lighter API
            # TODO: Implement trading rules fetching
            pass
            
        except Exception:
            self.logger().exception("There was an error requesting trading rules.")

    # Account and position management methods
    @property
    def account_index(self) -> int:
        """Returns the account index"""
        return self._account_index

    def get_connection_health(self) -> Dict[str, Any]:
        """
        Get comprehensive connection health information
        
        :return: Dictionary with connection status and statistics
        """
        health_info = {
            "connector_name": self.name,
            "account_index": self.account_index,
            "domain": self.domain,
            "trading_required": self.is_trading_required,
            "trading_pairs_count": len(self._trading_pairs),
            "positions_count": len(self._account_positions),
        }
        
        # Add data source health if available
        if hasattr(self._user_stream_tracker, '_data_source'):
            user_stream_ds = self._user_stream_tracker._data_source
            if hasattr(user_stream_ds, 'get_connection_health'):
                health_info["user_stream_health"] = user_stream_ds.get_connection_health()
        
        return health_info

    # Abstract methods from base classes - stub implementations for now
    
    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Get all trade updates for a specific order
        
        :param order: The order to get trade updates for
        :return: List of trade updates
        """
        # TODO: Implement trade updates fetching from Lighter API
        return []

    def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        Format trading rules from exchange info (orderBookDetails response)
        
        :param exchange_info_dict: Exchange information dictionary from orderBookDetails
        :return: Dictionary of trading rules
        """
        trading_rules = {}
        
        try:
            # Parse orderBookDetails response to extract trading rules
            if "markets" in exchange_info_dict:
                markets_data = exchange_info_dict["markets"]
                
                for market_data in markets_data:
                    market_id = market_data.get("market_id")
                    if market_id is None:
                        continue
                    
                    # Get trading pair for market ID
                    trading_pair = self._get_trading_pair_for_market_id(market_id)
                    
                    # Extract trading rule parameters
                    min_base_amount = Decimal(str(market_data.get("min_base_amount", "0.001"))) / Decimal("1e6")
                    min_quote_amount = Decimal(str(market_data.get("min_quote_amount", "1"))) / Decimal("1e6")
                    max_base_amount = Decimal(str(market_data.get("max_base_amount", "1000000"))) / Decimal("1e6")
                    
                    # Price and amount precision (typically 6 decimal places for Lighter)
                    base_precision = 6
                    quote_precision = 6
                    
                    # Create trading rule
                    trading_rule = TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_base_amount,
                        max_order_size=max_base_amount,
                        min_price_increment=Decimal("0.01"),  # Default price increment
                        min_base_amount_increment=Decimal("0.000001"),  # 1e-6 precision
                        min_quote_amount_increment=Decimal("0.000001"),
                        min_notional_size=min_quote_amount,
                        buy_order_collateral_token="USDC",
                        sell_order_collateral_token="USDC"
                    )
                    
                    trading_rules[trading_pair] = trading_rule
                    
            self.logger().info(f"Loaded {len(trading_rules)} trading rules")
            
        except Exception as e:
            self.logger().error(f"Failed to format trading rules: {e}")
            
        return trading_rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize trading pair symbols from exchange info (orderBooks response)
        
        :param exchange_info: Exchange information dictionary from orderBooks
        """
        try:
            # Parse orderBooks response to build trading_pair <-> market_id mapping
            if "markets" in exchange_info:
                markets_data = exchange_info["markets"]
                
                # Clear existing mappings
                self._market_id_to_trading_pair.clear()
                self._trading_pair_to_market_id.clear()
                
                for market_data in markets_data:
                    market_id = market_data.get("market_id")
                    symbol = market_data.get("symbol")  # e.g., "ETH-USDC"
                    
                    if market_id is not None and symbol:
                        # Convert symbol to Hummingbot format if needed
                        trading_pair = symbol.replace("_", "-").replace("/", "-")
                        
                        # Build bidirectional mapping
                        self._market_id_to_trading_pair[market_id] = trading_pair
                        self._trading_pair_to_market_id[trading_pair] = market_id
                        
                        # Also update the bidict for exchange symbols
                        self._trading_pairs_exchanged_symbols[trading_pair] = symbol
                
                self.logger().info(f"Initialized {len(self._market_id_to_trading_pair)} trading pair mappings")
                
            else:
                self.logger().warning("No markets data found in exchange info")
                
        except Exception as e:
            self.logger().error(f"Failed to initialize trading pair symbols: {e}")

    def _is_order_not_found_during_cancelation_error(self, response_code: int, response_text: str) -> bool:
        """
        Check if error indicates order not found during cancellation
        
        :param response_code: HTTP response code
        :param response_text: Response text
        :return: True if order not found error
        """
        # Enhanced Lighter-specific error patterns
        return (response_code == 404 or
                "order not found" in response_text.lower() or
                "invalid order" in response_text.lower() or
                "order does not exist" in response_text.lower() or
                "client_order_index not found" in response_text.lower())

    def _is_order_not_found_during_status_update_error(self, response_code: int, response_text: str) -> bool:
        """
        Check if error indicates order not found during status update
        
        :param response_code: HTTP response code
        :param response_text: Response text
        :return: True if order not found error
        """
        # Enhanced Lighter-specific error patterns
        return (response_code == 404 or
                "order not found" in response_text.lower() or
                "invalid order" in response_text.lower() or
                "order does not exist" in response_text.lower() or
                "client_order_index not found" in response_text.lower())

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from exchange
        
        :param tracked_order: The order to get status for
        :return: Order update
        """
        # TODO: Implement order status request from Lighter API
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=time.time(),
            new_state=OrderState.OPEN,  # Default state
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
        )

    async def _update_balances(self):
        """
        Update account balances using /api/v1/account endpoint
        """
        try:
            # Get account information from Lighter API
            account_info = await self._api_get(
                path_url=CONSTANTS.ACCOUNT_PATH_URL,
                params={"account_index": self._account_index}
            )
            
            # Parse balance information
            if "account" in account_info:
                account_data = account_info["account"]
                
                # Extract collateral balance (USDC)
                collateral_balance = Decimal(str(account_data.get("collateral", "0")))
                
                # Update balance in the connector
                self._account_balances["USDC"] = collateral_balance
                
                # Log balance update
                self.logger().debug(f"Updated balances: USDC = {collateral_balance}")
                
            else:
                self.logger().warning("No account data found in balance response")
                
        except Exception as e:
            self.logger().error(f"Failed to update balances: {e}")
            # Don't raise exception to avoid breaking the polling loop

    async def _update_trading_fees(self):
        """
        Update trading fees
        """
        # TODO: Implement trading fees update from Lighter API
        pass

    async def _user_stream_event_listener(self):
        """
        Listen to user stream events and process account updates
        """
        async for event_message in self._iter_user_event_queue():
            try:
                await self._process_user_stream_event(event_message)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error processing user stream event: {e}")

    async def _process_user_stream_event(self, event_message: Dict[str, Any]):
        """
        Process individual user stream events
        
        :param event_message: Event message from user stream
        """
        try:
            message_type = event_message.get("type")
            
            if message_type == "update/account_all":
                # Process account updates (balances, positions, orders)
                await self._process_account_update(event_message)
                
            elif message_type == "subscribed/account_all":
                # Initial account state - process as account update
                await self._process_account_update(event_message)
                
            else:
                self.logger().debug(f"Unhandled user stream message type: {message_type}")
                
        except Exception as e:
            self.logger().error(f"Error processing user stream event {event_message.get('type', 'unknown')}: {e}")

    async def _process_account_update(self, event_message: Dict[str, Any]):
        """
        Process account update messages from user stream
        
        :param event_message: Account update message
        """
        try:
            account_data = event_message.get("account", {})
            
            # Update balances
            if "collateral" in account_data:
                collateral_balance = Decimal(str(account_data["collateral"]))
                self._account_balances["USDC"] = collateral_balance
                self.logger().debug(f"Updated balance from user stream: USDC = {collateral_balance}")
            
            # Update positions
            if "positions" in event_message:
                positions_data = event_message["positions"]
                
                # Clear existing positions
                self._account_positions.clear()
                
                # Process each position
                for position_data in positions_data:
                    market_id = position_data.get("market_index")
                    if market_id is None:
                        continue
                        
                    # Get trading pair for market ID
                    trading_pair = self._get_trading_pair_for_market_id(market_id)
                    
                    # Extract position details
                    base_amount = Decimal(str(position_data.get("base_amount", "0"))) / Decimal("1e6")
                    
                    # Skip zero positions
                    if base_amount == 0:
                        continue
                    
                    # Determine position side
                    position_side = PositionSide.LONG if base_amount > 0 else PositionSide.SHORT
                    
                    # Create position object
                    position = Position(
                        trading_pair=trading_pair,
                        position_side=position_side,
                        unrealized_pnl=Decimal(str(position_data.get("unrealized_pnl", "0"))) / Decimal("1e6"),
                        entry_price=Decimal(str(position_data.get("entry_price", "0"))) / Decimal("1e6"),
                        amount=abs(base_amount),
                        leverage=Decimal("1")  # Lighter handles leverage internally
                    )
                    
                    self._account_positions[trading_pair] = position
                
                self.logger().debug(f"Updated {len(self._account_positions)} positions from user stream")
            
            # Process order updates if present
            if "orders" in event_message:
                orders_data = event_message["orders"]
                await self._process_order_updates_from_stream(orders_data)
                
        except Exception as e:
            self.logger().error(f"Error processing account update: {e}")

    async def _process_order_updates_from_stream(self, orders_data: List[Dict[str, Any]]):
        """
        Process order updates from user stream
        
        :param orders_data: List of order data from stream
        """
        try:
            for order_data in orders_data:
                client_order_index = order_data.get("client_order_index")
                if client_order_index is None:
                    continue
                
                # Find the corresponding tracked order
                tracked_order = None
                for order in self._order_tracker.all_orders.values():
                    if self._auth.get_client_order_index(order.client_order_id) == client_order_index:
                        tracked_order = order
                        break
                
                if tracked_order is None:
                    continue
                
                # Extract order status
                order_status = order_data.get("status", "unknown")
                
                # Map Lighter status to Hummingbot OrderState
                if order_status in CONSTANTS.ORDER_STATE:
                    new_state = CONSTANTS.ORDER_STATE[order_status]
                else:
                    self.logger().warning(f"Unknown order status: {order_status}")
                    continue
                
                # Create order update
                order_update = OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=time.time(),
                    new_state=new_state,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )
                
                # Process the order update
                self._order_tracker.process_order_update(order_update)
                
        except Exception as e:
            self.logger().error(f"Error processing order updates from stream: {e}")
