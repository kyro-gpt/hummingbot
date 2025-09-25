import asyncio  # noqa: F401
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.aster_perpetual import (
    aster_perpetual_constants as CONSTANTS,
    aster_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_api_order_book_data_source import (
    AsterPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_api_user_stream_data_source import (
    AsterPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_auth import AsterPerpetualAuth
from hummingbot.connector.derivative.position import Position  # noqa: F401
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate  # noqa: F401
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

apm_logger = None


class AsterPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Aster Perpetual Derivative connector based on Binance Perpetual structure

    Key differences from Binance:
    - Web3 ECDSA authentication instead of HMAC-SHA256
    - API v3 endpoints instead of v1/v2
    - Optimized @depth@100ms WebSocket updates
    - Wallet-based configuration instead of API keys
    """
    web_utils = web_utils
    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0

    def __init__(
            self,
            client_config_map: "ClientConfigAdapter",
            aster_perpetual_user_wallet: str = None,
            aster_perpetual_api_key: str = None,
            aster_perpetual_secret_key: str = None,
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
            domain: str = CONSTANTS.DOMAIN,
    ):
        # Unified authentication parameters (supports both v1 HMAC and v3 Web3)
        self.aster_perpetual_user_wallet = aster_perpetual_user_wallet
        self.aster_perpetual_api_key = aster_perpetual_api_key
        self.aster_perpetual_secret_key = aster_perpetual_secret_key

        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._position_mode = None
        self._last_trade_history_timestamp = None
        super().__init__(client_config_map)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> AsterPerpetualAuth:
        """
        Returns unified authenticator (uses default version internally)
        """
        return AsterPerpetualAuth(
            user_wallet=self.aster_perpetual_user_wallet,
            api_key=self.aster_perpetual_api_key,
            secret_key=self.aster_perpetual_secret_key,
            api_version=CONSTANTS.DEFAULT_API_VERSION
        )

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 600

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def supported_position_modes(self):
        """
        This method needs to be overridden to provide the accurate information depending on the exchange.
        """
        return [PositionMode.ONEWAY, PositionMode.HEDGE]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        """
        Check if request exception is related to time synchronization

        NOTE: Error codes may differ from Binance - needs verification with Aster API
        """
        error_description = str(request_exception)
        # TODO: Verify Aster's time synchronization error codes
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for this request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(
            status_update_exception
        ) and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(
            cancelation_exception
        ) and CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self.authenticator)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return AsterPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return AsterPerpetualUserStreamDataSource(
            auth=self.authenticator,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculate trading fee - same logic as Binance
        """
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        trading_pair = combine_to_hb_trading_pair(base=base_currency, quote=quote_currency)
        if trading_pair in self._trading_fees:
            trading_rule: TradingRule = self._trading_rules[trading_pair]
            fee_base_percent = trading_rule.maker_percent_fee_decimal if is_maker else trading_rule.taker_percent_fee_decimal
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self._trading_fees[trading_pair],
                position_action=PositionAction.OPEN,
                percent_token=fee_base_percent,
                flat_fees=[TokenAmount(amount=Decimal("0"), token=trading_rule.buy_order_collateral_token)]
            )
        else:
            fee = build_trade_fee(
                self.name,
                is_maker,
                base_currency=base_currency,
                quote_currency=quote_currency,
                order_type=order_type,
                order_side=order_side,
                amount=amount,
                price=price,
            )
        return fee

    # === Abstract methods that must be implemented ===

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
        Place order on Aster exchange

        NOTE: Similar to Binance but using v3 API endpoints and Web3 authentication
        """
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        api_params = {
            "symbol": symbol,
            "side": "BUY" if trade_type is TradeType.BUY else "SELL",
            "quantity": amount_str,
            "type": "MARKET" if order_type is OrderType.MARKET else "LIMIT",
            "newClientOrderId": order_id
        }

        if order_type.is_limit_type():
            api_params["price"] = price_str
        if order_type == OrderType.LIMIT:
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC
        if order_type == OrderType.LIMIT_MAKER:
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTX

        if self.position_mode == PositionMode.HEDGE:
            if position_action == PositionAction.OPEN:
                api_params["positionSide"] = "LONG" if trade_type is TradeType.BUY else "SHORT"
            else:
                api_params["positionSide"] = "SHORT" if trade_type is TradeType.BUY else "LONG"

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_URL,
                data=api_params,
                is_auth_required=True
            )
            o_id = str(order_result["orderId"])
            transact_time = order_result["updateTime"] * 1e-3
        except IOError as e:
            error_description = str(e)
            is_server_overloaded = ("status is 503" in error_description
                                    and "Unknown error, please check your request or try again later." in error_description)
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = time.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel order on Aster exchange

        NOTE: Similar to Binance but using v3 API endpoints and Web3 authentication
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            "origClientOrderId": order_id,
            "symbol": symbol,
        }

        cancel_result = await self._api_delete(
            path_url=CONSTANTS.ORDER_URL,
            params=api_params,
            is_auth_required=True
        )

        if cancel_result.get("code") == CONSTANTS.UNKNOWN_ORDER_ERROR_CODE and CONSTANTS.UNKNOWN_ORDER_MESSAGE in cancel_result.get("msg", ""):
            self.logger().debug(f"The order {order_id} does not exist on Aster Perpetual. No cancelation needed.")
            await self._order_tracker.process_order_not_found(order_id)
            raise IOError(f"{cancel_result.get('code')} - {cancel_result['msg']}")

        if cancel_result.get("status") == "CANCELED":
            return True
        return False

    async def _update_positions(self):
        """
        Update position information from Aster

        NOTE: Same logic as Binance but using v3 endpoints
        """
        position_info = await self._api_get(
            path_url=CONSTANTS.POSITION_INFORMATION_URL,
            is_auth_required=True
        )

        for position_data in position_info:
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=position_data["symbol"])
                position = self._perpetual_trading.get_position(trading_pair)
                if position is not None:
                    position.update_position(
                        position_side=PositionSide[position_data["positionSide"]],
                        unrealized_pnl=Decimal(position_data["unRealizedProfit"]),
                        entry_price=Decimal(position_data["entryPrice"]),
                        amount=Decimal(position_data["positionAmt"]),
                    )
            except KeyError:
                # Skip symbols that aren't in our configured trading pairs
                self.logger().debug(f"Skipping position update for unknown symbol: {position_data['symbol']}")
                continue

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Set leverage for trading pair on Aster

        NOTE: Same API as Binance but using v3 endpoints
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {
            "symbol": symbol,
            "leverage": leverage
        }

        try:
            await self._api_post(
                path_url=CONSTANTS.SET_LEVERAGE_URL,
                data=api_params,
                is_auth_required=True,
                throttler_limit_id=CONSTANTS.SET_LEVERAGE_URL
            )
            return True, ""
        except Exception as e:
            return False, str(e)

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        """
        Fetch last funding fee payment for trading pair

        NOTE: Same logic as Binance but using v3 endpoints
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        try:
            response = await self._api_get(
                path_url=CONSTANTS.GET_INCOME_HISTORY_URL,
                params={
                    "symbol": symbol,
                    "incomeType": "FUNDING_FEE",
                    "limit": 1
                },
                is_auth_required=True
            )

            if response:
                payment = response[0]
                return (
                    payment["time"] / 1000.0,  # Convert to seconds
                    Decimal("0"),  # Funding rate (not available in income history)
                    Decimal(str(payment["income"]))  # Payment amount as Decimal
                )
        except Exception as e:
            self.logger().warning(f"Failed to fetch funding payment for {trading_pair}: {e}")

        return 0, Decimal("-1"), Decimal("-1")

    async def _trading_pair_position_mode_set(
        self, mode: PositionMode, trading_pair: str
    ) -> Tuple[bool, str]:
        """
        Set position mode for trading pair

        NOTE: Same API as Binance but using v3 endpoints
        """
        try:
            dual_side_position = mode == PositionMode.HEDGE
            await self._api_post(
                path_url=CONSTANTS.CHANGE_POSITION_MODE_URL,
                data={"dualSidePosition": str(dual_side_position).lower()},
                is_auth_required=True
            )
            return True, ""
        except Exception as e:
            return False, str(e)

    # === Additional abstract methods from base classes ===

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Get all trade updates for an order - adapted from Binance for Aster v3 API
        """
        trade_updates = []
        try:
            # Try to get exchange order ID with a shorter timeout
            try:
                exchange_order_id = await asyncio.wait_for(order.get_exchange_order_id(), timeout=1.0)
            except asyncio.TimeoutError:
                # If we can't get the exchange order ID, skip this update
                self.logger().debug(f"Skipping trade updates for {order.client_order_id} - exchange order ID not available yet")
                return trade_updates
            
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            all_fills_response = await self._api_get(
                path_url=CONSTANTS.ACCOUNT_TRADE_LIST_URL,
                params={
                    "symbol": trading_pair,
                },
                is_auth_required=True,
                throttler_limit_id=CONSTANTS.ACCOUNT_TRADE_LIST_URL)

            for trade in all_fills_response:
                order_id = str(trade.get("orderId"))
                if order_id == exchange_order_id:
                    position_side = trade["positionSide"]
                    position_action = (PositionAction.OPEN
                                       if (order.trade_type is TradeType.BUY and position_side == "LONG"
                                           or order.trade_type is TradeType.SELL and position_side == "SHORT")
                                       else PositionAction.CLOSE)
                    fee = TradeFeeBase.new_perpetual_fee(
                        fee_schema=self.trade_fee_schema(),
                        position_action=position_action,
                        percent_token=trade["commissionAsset"],
                        flat_fees=[TokenAmount(amount=Decimal(trade["commission"]), token=trade["commissionAsset"])]
                    )
                    trade_update: TradeUpdate = TradeUpdate(
                        trade_id=str(trade["id"]),
                        client_order_id=order.client_order_id,
                        exchange_order_id=trade["orderId"],
                        trading_pair=order.trading_pair,
                        fill_timestamp=trade["time"] * 1e-3,
                        fill_price=Decimal(trade["price"]),
                        fill_base_amount=Decimal(trade["qty"]),
                        fill_quote_amount=Decimal(trade["quoteQty"]),
                        fee=fee,
                    )
                    trade_updates.append(trade_update)

        except Exception as e:
            self.logger().debug(f"Error fetching trade updates for {order.client_order_id}: {e}")

        return trade_updates

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from exchange info - adapted from Binance for Aster v3 API
        """
        trading_rules = []
        if "symbols" in exchange_info_dict:
            for rule in exchange_info_dict["symbols"]:
                if web_utils.is_exchange_information_valid(rule):
                    try:
                        trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule["symbol"])
                        
                        # Extract precision from filters (like Binance)
                        filters = rule.get("filters", [])
                        filt_dict = {fil["filterType"]: fil for fil in filters}
                        
                        # Get LOT_SIZE filter for quantity precision
                        lot_size_filter = filt_dict.get("LOT_SIZE", {})
                        min_order_size = Decimal(lot_size_filter.get("minQty", "0.001"))
                        max_order_size = Decimal(lot_size_filter.get("maxQty", "1000000"))
                        step_size = Decimal(lot_size_filter.get("stepSize", "0.001"))
                        
                        # Get PRICE_FILTER for price precision
                        price_filter = filt_dict.get("PRICE_FILTER", {})
                        tick_size = Decimal(price_filter.get("tickSize", "0.01"))
                        
                        trading_rule = TradingRule(
                            trading_pair=trading_pair,
                            min_order_size=min_order_size,
                            max_order_size=max_order_size,
                            min_price_increment=tick_size,
                            min_base_amount_increment=step_size,
                        )
                        trading_rules.append(trading_rule)
                    except KeyError:
                        # Skip symbols that aren't in our configured trading pairs
                        self.logger().debug(f"Skipping trading rule for unknown symbol: {rule['symbol']}")
                        continue
        return trading_rules

    async def _update_trading_rules(self):
        """Override to handle errors gracefully"""
        try:
            await super()._update_trading_rules()
        except (IOError, OSError) as e:
            self.logger().error(f"Invalid symbol.")
            self.logger().network(
                f"Error fetching trading rules: {e}",
                app_warning_msg=f"Could not fetch trading rules from {self.name_cap}. Check network connection."
            )

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize trading pair symbols from exchange info - adapted from Binance
        """
        mapping = bidict()
        # Handle error responses gracefully
        if "symbols" not in exchange_info:
            self.logger().error(f"Invalid exchange info response: {exchange_info}")
            return
        for symbol_data in filter(web_utils.is_exchange_information_valid, exchange_info["symbols"]):
            mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(
                base=symbol_data["baseAsset"], quote=symbol_data["quoteAsset"]
            )
        self._set_trading_pair_symbol_map(mapping)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from exchange - adapted from Binance for Aster v3 API
        """
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        order_update = await self._api_get(
            path_url=CONSTANTS.ORDER_URL,
            params={
                "symbol": trading_pair,
                "origClientOrderId": tracked_order.client_order_id,
            },
            is_auth_required=True,
            throttler_limit_id=CONSTANTS.ORDER_URL
        )

        order_state = CONSTANTS.ORDER_STATE[order_update["status"]]

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order_update["orderId"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=order_update["updateTime"] * 1e-3,
            new_state=order_state,
        )

    async def _update_balances(self):
        """
        Update account balances - adapted from Binance for Aster v3 API
        """
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_INFO_URL,
            is_auth_required=True)

        for balance_entry in account_info["assets"]:
            asset_name = balance_entry["asset"]
            free_balance = Decimal(balance_entry["availableBalance"])
            total_balance = Decimal(balance_entry["walletBalance"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _update_trading_fees(self):
        """
        Update trading fees information from the exchange
        """
        pass  # Same as Binance - not implemented

    async def _status_polling_loop_fetch_updates(self):
        """
        Called by status polling loop to fetch updates - same as Binance
        """
        await safe_gather(
            self._update_order_fills_from_trades(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_order_fills_from_trades(self):
        """
        This is intended to be a backup measure to get filled events with trade ID for orders,
        in case Aster's user stream events are not working.
        Adapted from Binance perpetual implementation.
        """
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        if current_tick > last_tick and len(self._order_tracker.active_orders) > 0:
            trading_pairs_to_order_map: Dict[str, Dict[str, Any]] = {}
            for order in self._order_tracker.active_orders.values():
                # Skip orders without exchange order ID
                if not hasattr(order, 'exchange_order_id') or order.exchange_order_id is None:
                    continue
                if order.trading_pair not in trading_pairs_to_order_map:
                    trading_pairs_to_order_map[order.trading_pair] = {}
                trading_pairs_to_order_map[order.trading_pair][order.exchange_order_id] = order
            
            trading_pairs = list(trading_pairs_to_order_map.keys())
            tasks = [
                self._api_get(
                    path_url=CONSTANTS.ACCOUNT_TRADE_LIST_URL,
                    params={"symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)},
                    is_auth_required=True,
                )
                for trading_pair in trading_pairs
            ]
            self.logger().debug(f"Polling for order fills of {len(tasks)} trading_pairs.")
            results = await safe_gather(*tasks, return_exceptions=True)
            
            for trades, trading_pair in zip(results, trading_pairs):
                order_map = trading_pairs_to_order_map.get(trading_pair)
                if isinstance(trades, Exception):
                    self.logger().network(
                        f"Error fetching trades update for the order {trading_pair}: {trades}.",
                        app_warning_msg=f"Failed to fetch trade update for {trading_pair}."
                    )
                    continue
                    
                for trade in trades:
                    order_id = str(trade.get("orderId"))
                    if order_id in order_map:
                        tracked_order: InFlightOrder = order_map.get(order_id)
                        position_side = trade["positionSide"]
                        position_action = (PositionAction.OPEN
                                         if (tracked_order.trade_type is TradeType.BUY and position_side == "LONG")
                                         or (tracked_order.trade_type is TradeType.SELL and position_side == "SHORT")
                                         else PositionAction.CLOSE)
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=tracked_order.trade_type,
                            percent_token=trade["commissionAsset"],
                            flat_fees=[TokenAmount(amount=Decimal(trade["commission"]), token=trade["commissionAsset"])]
                        )
                        trade_update = TradeUpdate(
                            trade_id=str(trade["id"]),
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=order_id,
                            trading_pair=tracked_order.trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(trade["qty"]),
                            fill_quote_amount=Decimal(trade["quoteQty"]),
                            fill_price=Decimal(trade["price"]),
                            fill_timestamp=trade["time"] * 1e-3,
                            position_side=PositionSide[position_side],
                            position_action=position_action,
                        )
                        self._order_tracker.process_trade_update(trade_update)

    async def _user_stream_event_listener(self):
        """
        Listen for user stream events - adapted from Binance pattern
        """
        async for stream_message in self._iter_user_event_queue():
            try:
                event_type = stream_message.get("e")
                if event_type == "ORDER_TRADE_UPDATE":
                    order_message = stream_message.get("o")
                    client_order_id = order_message.get("c")
                    tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                    
                    if tracked_order is not None:
                        trade_id: str = str(order_message.get("t", "0"))
                        
                        if trade_id != "0":  # Indicates that there has been a trade
                            fee_asset = order_message.get("N", tracked_order.quote_asset)
                            fee_amount = Decimal(order_message.get("n", "0"))
                            position_side = order_message.get("ps", "LONG")
                            position_action = (PositionAction.OPEN
                                               if (tracked_order.trade_type is TradeType.BUY and position_side == "LONG"
                                                   or tracked_order.trade_type is TradeType.SELL and position_side == "SHORT")
                                               else PositionAction.CLOSE)
                            flat_fees = [] if fee_amount == Decimal("0") else [TokenAmount(amount=fee_amount, token=fee_asset)]

                            fee = AddedToCostTradeFee(
                                percent_token=fee_asset,
                                flat_fees=flat_fees,
                            )

                            trade_update: TradeUpdate = TradeUpdate(
                                trade_id=trade_id,
                                client_order_id=client_order_id,
                                exchange_order_id=str(order_message["i"]),
                                trading_pair=tracked_order.trading_pair,
                                fill_timestamp=stream_message["E"] * 1e-3,
                                fill_price=Decimal(order_message["L"]),
                                fill_base_amount=Decimal(order_message["l"]),
                                fill_quote_amount=Decimal(order_message["L"]) * Decimal(order_message["l"]),
                                fee=fee,
                            )
                            self._order_tracker.process_trade_update(trade_update)

                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update: OrderUpdate = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=stream_message["E"] * 1e-3,
                            new_state=CONSTANTS.ORDER_STATE[order_message["X"]],
                            client_order_id=client_order_id,
                            exchange_order_id=str(order_message["i"]),
                        )
                        self._order_tracker.process_order_update(order_update)
                elif event_type == "ACCOUNT_UPDATE":
                    # Handle balance and position updates - adapted from Binance
                    update_data = stream_message.get("a", {})
                    # Update balances
                    for asset in update_data.get("B", []):
                        asset_name = asset["a"]
                        self._account_balances[asset_name] = Decimal(asset["wb"])
                        self._account_available_balances[asset_name] = Decimal(asset["cw"])
                    
                    # Update positions
                    for asset in update_data.get("P", []):
                        trading_pair = asset["s"]
                        try:
                            hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(trading_pair)
                        except KeyError:
                            # Ignore results for which their symbols is not tracked by the connector
                            continue

                        side = PositionSide[asset['ps']]
                        position = self._perpetual_trading.get_position(hb_trading_pair, side)
                        if position is not None:
                            amount = Decimal(asset["pa"])
                            if amount == Decimal("0"):
                                pos_key = self._perpetual_trading.position_key(hb_trading_pair, side)
                                self._perpetual_trading.remove_position(pos_key)
                            else:
                                position.update_position(position_side=PositionSide[asset["ps"]],
                                                         unrealized_pnl=Decimal(asset["up"]),
                                                         entry_price=Decimal(asset["ep"]),
                                                         amount=Decimal(asset["pa"]))
                        else:
                            amount = Decimal(asset["pa"])
                            if amount != Decimal("0"):
                                leverage = self._perpetual_trading.get_leverage(hb_trading_pair)
                                position = Position(
                                    trading_pair=hb_trading_pair,
                                    position_side=PositionSide[asset["ps"]],
                                    unrealized_pnl=Decimal(asset["up"]),
                                    entry_price=Decimal(asset["ep"]),
                                    amount=Decimal(asset["pa"]),
                                    leverage=leverage
                                )
                                self._perpetual_trading.set_position(hb_trading_pair, position)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Get last traded price for trading pair - adapted from Binance for Aster v3 API
        """
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params = {"symbol": exchange_symbol}
        response = await self._api_get(
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_URL,
            params=params)
        price = float(response["lastPrice"])
        return price
