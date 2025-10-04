from decimal import Decimal
from typing import List, Optional

import pandas_ta as ta
from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig


class AvellanedaMMControllerConfig(MarketMakingControllerConfigBase):
    """
    Configuration for Avellaneda-Stoikov Market Making Controller.
    Implements the Avellaneda-Stoikov optimal market making strategy with risk management.
    """
    controller_name: str = "avellaneda_mm"

    # Candles configuration for volatility calculation
    candles_config: List[CandlesConfig] = []
    candles_connector: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the connector for candles data (leave empty to use same as trading connector): ",
            "prompt_on_new": True})
    candles_trading_pair: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the trading pair for candles data (leave empty to use same as trading pair): ",
            "prompt_on_new": True})
    interval: str = Field(
        default="1m",
        json_schema_extra={
            "prompt": "Enter the candle interval for volatility calculation (e.g., 1m, 5m): ",
            "prompt_on_new": True})

    # Avellaneda-Stoikov specific parameters
    risk_factor: Decimal = Field(
        default=Decimal("1.0"),
        gt=0,
        json_schema_extra={
            "prompt": "Enter the risk factor (gamma) - controls risk aversion (e.g., 1.0): ",
            "prompt_on_new": True, "is_updatable": True})

    order_amount: Decimal = Field(
        default=Decimal("100"),
        gt=0,
        json_schema_extra={
            "prompt": "Enter the order amount per level: ",
            "prompt_on_new": True, "is_updatable": True})

    inventory_target_base_pct: Decimal = Field(
        default=Decimal("50"),
        ge=0, le=100,
        json_schema_extra={
            "prompt": "Enter the target base asset inventory percentage (0-100): ",
            "prompt_on_new": True, "is_updatable": True})

    order_amount_shape_factor: Decimal = Field(
        default=Decimal("0.0"),
        ge=0, le=1,
        json_schema_extra={
            "prompt": "Enter the order amount shape factor (eta) for inventory skewing (0-1): ",
            "prompt_on_new": True, "is_updatable": True})

    min_spread_pct: Decimal = Field(
        default=Decimal("0.1"),
        ge=0,
        json_schema_extra={
            "prompt": "Enter the minimum spread percentage (e.g., 0.1 for 0.1%): ",
            "prompt_on_new": True, "is_updatable": True})

    volatility_buffer_size: int = Field(
        default=200,
        ge=10, le=1000,
        json_schema_extra={
            "prompt": "Enter the number of price samples for volatility calculation (10-1000): ",
            "prompt_on_new": True, "is_updatable": True})

    # Override spreads to use dynamic calculation
    buy_spreads: List[float] = Field(default=[], json_schema_extra={"exclude_from_prompt": True})
    sell_spreads: List[float] = Field(default=[], json_schema_extra={"exclude_from_prompt": True})

    @field_validator("candles_connector", mode="before")
    @classmethod
    def set_candles_connector(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("connector_name")
        return v

    @field_validator("candles_trading_pair", mode="before")
    @classmethod
    def set_candles_trading_pair(cls, v, validation_info: ValidationInfo):
        if v is None or v == "":
            return validation_info.data.get("trading_pair")
        return v


class AvellanedaMMController(MarketMakingControllerBase):
    """
    Avellaneda-Stoikov Market Making Controller

    Implements the optimal market making strategy from "High-frequency trading in a limit order book"
    by Avellaneda & Stoikov. The strategy dynamically adjusts bid/ask spreads based on:
    - Volatility estimation
    - Inventory position relative to target
    - Risk aversion parameters
    """

    def __init__(self, config: AvellanedaMMControllerConfig, *args, **kwargs):
        self.config = config

        # Set up candles config for volatility calculation
        if len(self.config.candles_config) == 0:
            self.config.candles_config = [CandlesConfig(
                connector=config.candles_connector,
                trading_pair=config.candles_trading_pair,
                interval=config.interval,
                max_records=config.volatility_buffer_size + 50  # Extra buffer for calculations
            )]

        # Initialize processed data storage
        self.processed_data = {
            "volatility": Decimal("0"),
            "reservation_price": Decimal("0"),
            "optimal_spread": Decimal("0"),
            "optimal_bid": Decimal("0"),
            "optimal_ask": Decimal("0"),
            "inventory_ratio": Decimal("0")
        }

        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        """
        Calculate Avellaneda-Stoikov parameters:
        1. Estimate volatility from price data
        2. Calculate inventory position relative to target
        3. Compute reservation price and optimal spread
        4. Determine optimal bid/ask prices
        """
        try:
            # Get market data
            mid_price = await self.get_processed_price()
            if mid_price is None or mid_price <= 0:
                return

            # Calculate volatility from candles
            volatility = await self._calculate_volatility()

            # Calculate inventory metrics
            inventory_ratio = await self._calculate_inventory_ratio()

            # Apply Avellaneda-Stoikov formulas
            reservation_price, optimal_spread = self._calculate_reservation_price_and_spread(
                mid_price, volatility, inventory_ratio
            )

            # Calculate optimal bid/ask with minimum spread constraint
            min_spread = mid_price * self.config.min_spread_pct / Decimal("100")
            effective_spread = max(optimal_spread, min_spread)

            optimal_ask = reservation_price + effective_spread / 2
            optimal_bid = reservation_price - effective_spread / 2

            # Store results
            self.processed_data.update({
                "volatility": volatility,
                "reservation_price": reservation_price,
                "optimal_spread": effective_spread,
                "optimal_bid": optimal_bid,
                "optimal_ask": optimal_ask,
                "inventory_ratio": inventory_ratio,
                "mid_price": mid_price
            })

            # Update spreads for the base controller
            if mid_price > 0:
                buy_spread = float((mid_price - optimal_bid) / mid_price)
                sell_spread = float((optimal_ask - mid_price) / mid_price)
                self.config.buy_spreads = [max(buy_spread, 0.0001)]  # Minimum 0.01%
                self.config.sell_spreads = [max(sell_spread, 0.0001)]

        except Exception as e:
            self.logger().error(f"Error in update_processed_data: {e}")

    async def _calculate_volatility(self) -> Decimal:
        """Calculate volatility from price candles using returns standard deviation"""
        try:
            candles_df = self.market_data_provider.get_candles_df(
                connector_name=self.config.candles_connector,
                trading_pair=self.config.candles_trading_pair,
                interval=self.config.interval,
                max_records=self.config.volatility_buffer_size
            )

            if len(candles_df) < 2:
                return Decimal("0.01")  # Default volatility

            # Calculate log returns
            prices = candles_df["close"]
            log_returns = (prices / prices.shift(1)).apply(lambda x: x.ln() if x > 0 else 0)

            # Calculate volatility (standard deviation of returns)
            volatility = float(log_returns.std())
            return Decimal(str(max(volatility, 0.001)))  # Minimum volatility threshold

        except Exception as e:
            self.logger().error(f"Error calculating volatility: {e}")
            return Decimal("0.01")

    async def _calculate_inventory_ratio(self) -> Decimal:
        """Calculate current inventory ratio relative to target"""
        try:
            balances = self.market_data_provider.get_balance_df(connector_name=self.config.connector_name)
            if balances.empty:
                return Decimal("0")

            base_asset = self.config.trading_pair.split("-")[0]
            quote_asset = self.config.trading_pair.split("-")[1]

            base_balance = Decimal(str(balances.get(base_asset, 0)))
            quote_balance = Decimal(str(balances.get(quote_asset, 0)))

            mid_price = await self.get_processed_price()
            if mid_price is None or mid_price <= 0:
                return Decimal("0")

            # Calculate total portfolio value in quote terms
            total_value = base_balance * mid_price + quote_balance
            if total_value <= 0:
                return Decimal("0")

            # Current base asset percentage
            current_base_pct = (base_balance * mid_price) / total_value * 100

            # Target base asset percentage
            target_base_pct = self.config.inventory_target_base_pct

            # Return inventory deviation from target (normalized)
            inventory_deviation = (current_base_pct - target_base_pct) / 100
            return inventory_deviation

        except Exception as e:
            self.logger().error(f"Error calculating inventory ratio: {e}")
            return Decimal("0")

    def _calculate_reservation_price_and_spread(
        self,
        mid_price: Decimal,
        volatility: Decimal,
        inventory_ratio: Decimal
    ) -> tuple[Decimal, Decimal]:
        """
        Apply Avellaneda-Stoikov formulas for reservation price and optimal spread

        reservation_price = mid_price - inventory_ratio * gamma * volatility
        optimal_spread = gamma * volatility + (2/gamma) * ln(1 + gamma/kappa)

        Simplified version assuming infinite time horizon and approximated liquidity parameters
        """
        try:
            gamma = self.config.risk_factor

            # Reservation price: adjust mid price based on inventory imbalance
            reservation_price = mid_price - inventory_ratio * gamma * volatility * mid_price

            # Optimal spread: base spread from volatility and risk aversion
            # Simplified formula for infinite horizon case
            optimal_spread = gamma * volatility * mid_price * 2  # Factor of 2 for bid-ask spread

            return reservation_price, optimal_spread

        except Exception as e:
            self.logger().error(f"Error calculating reservation price and spread: {e}")
            return mid_price, mid_price * Decimal("0.002")  # 0.2% default spread

    def get_executor_config(self, level_id: str, price: Decimal, amount: Decimal) -> PositionExecutorConfig:
        """Create executor config with inventory-adjusted amounts"""
        trade_type = self.get_trade_type_from_level_id(level_id)

        # Apply inventory skewing using order_amount_shape_factor (eta)
        if self.config.order_amount_shape_factor > 0:
            inventory_ratio = self.processed_data.get("inventory_ratio", Decimal("0"))
            eta = self.config.order_amount_shape_factor

            if trade_type.name == "BUY" and inventory_ratio > 0:
                # Reduce buy amount when we have excess base inventory
                skew_factor = (-eta * inventory_ratio).exp()
                amount = amount * skew_factor
            elif trade_type.name == "SELL" and inventory_ratio < 0:
                # Reduce sell amount when we have deficit base inventory
                skew_factor = (eta * inventory_ratio).exp()
                amount = amount * skew_factor

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            level_id=level_id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_price=price,
            amount=max(amount, Decimal("0.001")),  # Minimum amount threshold
            triple_barrier_config=self.config.triple_barrier_config,
            leverage=self.config.leverage,
            side=trade_type,
        )

    def to_format_status(self) -> list[str]:
        """Display Avellaneda-Stoikov strategy status"""
        lines = []
        try:
            lines.append("Strategy: Avellaneda-Stoikov Market Making")
            lines.append(f"Risk Factor (γ): {self.config.risk_factor}")
            lines.append(f"Shape Factor (η): {self.config.order_amount_shape_factor}")
            lines.append(f"Target Base %: {self.config.inventory_target_base_pct}%")

            if self.processed_data:
                lines.append("--- Current State ---")
                lines.append(f"Volatility: {self.processed_data.get('volatility', 0):.4f}")
                lines.append(f"Inventory Ratio: {self.processed_data.get('inventory_ratio', 0):.4f}")
                lines.append(f"Reservation Price: {self.processed_data.get('reservation_price', 0):.4f}")
                lines.append(f"Optimal Spread: {self.processed_data.get('optimal_spread', 0):.4f}")

        except Exception as e:
            lines.append(f"Error displaying status: {e}")

        return lines
