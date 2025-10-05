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
        Place an order on the exchange
        
        :param order_id: Client order ID
        :param trading_pair: Trading pair
        :param amount: Order amount
        :param trade_type: BUY or SELL
        :param order_type: Order type
        :param price: Order price
        :param position_action: Position action
        :return: Tuple of exchange order ID and timestamp
        """
        # TODO: Implement order placement using Lighter's sendTx API
        # This will involve creating the appropriate transaction structure
        # and signing it with the private key
        raise NotImplementedError("Order placement not yet implemented")

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel an order on the exchange
        
        :param order_id: Client order ID
        :param tracked_order: Tracked order object
        """
        # TODO: Implement order cancellation using Lighter's sendTx API
        raise NotImplementedError("Order cancellation not yet implemented")

    async def _update_positions(self):
        """Update account positions"""
        # TODO: Implement position updates from account API
        pass

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
        return web_utils.format_trading_pair_to_market_id(trading_pair)

    def _get_trading_pair_for_market_id(self, market_id: int) -> str:
        """Get trading pair for market ID"""
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
        Format trading rules from exchange info
        
        :param exchange_info_dict: Exchange information dictionary
        :return: Dictionary of trading rules
        """
        # TODO: Implement trading rules formatting from Lighter API response
        return {}

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize trading pair symbols from exchange info
        
        :param exchange_info: Exchange information dictionary
        """
        # TODO: Implement symbol mapping initialization from Lighter API response
        pass

    def _is_order_not_found_during_cancelation_error(self, response_code: int, response_text: str) -> bool:
        """
        Check if error indicates order not found during cancellation
        
        :param response_code: HTTP response code
        :param response_text: Response text
        :return: True if order not found error
        """
        # TODO: Implement Lighter-specific error detection
        return response_code == 404 or "not found" in response_text.lower()

    def _is_order_not_found_during_status_update_error(self, response_code: int, response_text: str) -> bool:
        """
        Check if error indicates order not found during status update
        
        :param response_code: HTTP response code
        :param response_text: Response text
        :return: True if order not found error
        """
        # TODO: Implement Lighter-specific error detection
        return response_code == 404 or "not found" in response_text.lower()

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
        Update account balances
        """
        # TODO: Implement balance updates from Lighter account API
        pass

    async def _update_trading_fees(self):
        """
        Update trading fees
        """
        # TODO: Implement trading fees update from Lighter API
        pass

    async def _user_stream_event_listener(self):
        """
        Listen to user stream events
        """
        # TODO: Implement user stream event processing
        pass
