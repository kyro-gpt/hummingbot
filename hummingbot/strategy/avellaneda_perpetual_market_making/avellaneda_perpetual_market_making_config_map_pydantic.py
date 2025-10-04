# Avellaneda Perpetual Market Making Strategy Configuration
# This file is adapted from the spot Avellaneda strategy with modifications for perpetual trading:
#
# KEY DIFFERENCES FROM SPOT VERSION:
# 1. Uses 'derivative' field instead of 'exchange' for derivative connector validation
# 2. Adds 'leverage' field for perpetual trading with margin
# 3. Strategy name changed to "avellaneda_perpetual_market_making"
# 4. Updated prompts to reflect derivative trading context
# 5. All core Avellaneda-Stoikov parameters remain identical (risk_factor, volatility calculation, etc.)

from datetime import datetime, time
from decimal import Decimal
from typing import Dict, Optional, Union

from pydantic import ConfigDict, Field, field_validator, model_validator

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.config_validators import (
    validate_derivative,  # DIFFERENCE: Use derivative validator instead of exchange
)
from hummingbot.client.config.config_validators import (
    validate_bool,
    validate_datetime_iso_string,
    validate_decimal,
    validate_int,
    validate_market_trading_pair,
    validate_time_iso_string,
)
from hummingbot.client.config.strategy_config_data_types import BaseStrategyConfigMap
from hummingbot.client.settings import required_exchanges
from hummingbot.connector.utils import split_hb_trading_pair

# === Execution Timeframe Models (identical to spot version) ===

class InfiniteModel(BaseClientModel):
    model_config = ConfigDict(title="infinite")


class FromDateToDateModel(BaseClientModel):
    start_datetime: datetime = Field(
        default=...,
        description="The start date and time for date-to-date execution timeframe.",
        json_schema_extra={
            "prompt": "Please enter the start date and time (YYYY-MM-DD HH:MM:SS)", "prompt_on_new": True
        }
    )
    end_datetime: datetime = Field(
        default=...,
        description="The end date and time for date-to-date execution timeframe.",
        json_schema_extra={
            "prompt": "Please enter the end date and time (YYYY-MM-DD HH:MM:SS)", "prompt_on_new": True
        }
    )
    model_config = ConfigDict(title="from_date_to_date")

    @field_validator("start_datetime", "end_datetime", mode="before")
    @classmethod
    def validate_execution_time(cls, v: Union[str, datetime]) -> Optional[str]:
        if not isinstance(v, str):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        ret = validate_datetime_iso_string(v)
        if ret is not None:
            raise ValueError(ret)
        return v


class DailyBetweenTimesModel(BaseClientModel):
    start_time: time = Field(
        default=...,
        description="The start time for daily-between-times execution timeframe.",
        json_schema_extra={"prompt": "Please enter the start time (HH:MM:SS)", "prompt_on_new": True},
    )
    end_time: time = Field(
        default=...,
        description="The end time for daily-between-times execution timeframe.",
        json_schema_extra={"prompt": "Please enter the end time (HH:MM:SS)", "prompt_on_new": True},
    )
    model_config = ConfigDict(title="daily_between_times")

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def validate_execution_time(cls, v: Union[str, datetime]) -> Optional[str]:
        if not isinstance(v, str):
            v = v.strftime("%H:%M:%S")
        ret = validate_time_iso_string(v)
        if ret is not None:
            raise ValueError(ret)
        return v


EXECUTION_TIMEFRAME_MODELS = {
    InfiniteModel.model_config["title"]: InfiniteModel,
    FromDateToDateModel.model_config["title"]: FromDateToDateModel,
    DailyBetweenTimesModel.model_config["title"]: DailyBetweenTimesModel,
}


# === Order Level Models (identical to spot version) ===

class SingleOrderLevelModel(BaseClientModel):
    model_config = ConfigDict(title="single_order_level")


class MultiOrderLevelModel(BaseClientModel):
    order_levels: int = Field(
        default=2,
        description="The number of orders placed on either side of the order book.",
        ge=2,
        json_schema_extra={"prompt": "How many orders do you want to place on both sides?", "prompt_on_new": True},
    )
    level_distances: Decimal = Field(
        default=Decimal("0"),
        description="The spread between order levels, expressed in % of optimal spread.",
        ge=0,
        json_schema_extra={"prompt": "How far apart in % of optimal spread should orders on one side be?", "prompt_on_new": True},
    )
    model_config = ConfigDict(title="multi_order_level")


ORDER_LEVEL_MODELS = {
    SingleOrderLevelModel.model_config["title"]: SingleOrderLevelModel,
    MultiOrderLevelModel.model_config["title"]: MultiOrderLevelModel,
}


# === Hanging Orders Models (identical to spot version) ===

class IgnoreHangingOrdersModel(BaseClientModel):
    model_config = ConfigDict(title="ignore_hanging_orders")


class TrackHangingOrdersModel(BaseClientModel):
    hanging_orders_cancel_pct: Decimal = Field(
        default=Decimal("10"),
        description="The percentage change in price that will trigger the cancellation of hanging orders.",
        ge=0,
        le=100,
        json_schema_extra={"prompt": "At what percentage change in price would you like to cancel hanging orders?"},
    )
    model_config = ConfigDict(title="track_hanging_orders")


HANGING_ORDER_MODELS = {
    TrackHangingOrdersModel.model_config["title"]: TrackHangingOrdersModel,
    IgnoreHangingOrdersModel.model_config["title"]: IgnoreHangingOrdersModel,
}


# === Main Configuration Class ===

class AvellanedaPerpetualMarketMakingConfigMap(BaseStrategyConfigMap):
    # DIFFERENCE: Strategy name for perpetual version
    strategy: str = Field(default="avellaneda_perpetual_market_making")

    # DIFFERENCE: Use derivative field instead of exchange for perpetual trading
    derivative: str = Field(
        default=...,
        description="The name of the derivative connector.",
        json_schema_extra={"prompt": "Input your derivative connector", "prompt_on_new": True},
    )

    # Market field (same as spot version)
    market: str = Field(
        default=...,
        description="The trading pair.",
        json_schema_extra={"prompt": "Enter the token trading pair you would like to trade on (e.g. BTC-USDT)", "prompt_on_new": True},
    )

    # DIFFERENCE: Add leverage field for perpetual trading
    leverage: int = Field(
        default=5,
        description="The leverage to use for trading (e.g., 5 means 5x leverage).",
        ge=1,
        le=100,
        json_schema_extra={"prompt": "How much leverage do you want to use? (e.g., 5 for 5x)", "prompt_on_new": True},
    )

    # All remaining fields are IDENTICAL to spot Avellaneda strategy
    execution_timeframe_mode: Union[InfiniteModel, FromDateToDateModel, DailyBetweenTimesModel] = Field(
        default=...,
        description="The execution timeframe.",
        json_schema_extra={
            "prompt": f"Select the execution timeframe ({'/'.join(EXECUTION_TIMEFRAME_MODELS.keys())})",
            "prompt_on_new": True,
        }
    )
    order_amount: Decimal = Field(
        default=...,
        description="The strategy order amount.",
        gt=0,
        json_schema_extra={
            "prompt": lambda mi: AvellanedaPerpetualMarketMakingConfigMap.order_amount_prompt(mi),
            "prompt_on_new": True,
        }
    )
    order_optimization_enabled: bool = Field(
        default=True,
        description=(
            "Allows the bid and ask order prices to be adjusted based on"
            " the current top bid and ask prices in the market."
        ),
        json_schema_extra={"prompt": "Do you want to enable order optimization? (Yes/No)"}
    )
    risk_factor: Decimal = Field(
        default=Decimal("1"),
        description="The risk factor (γ).",
        gt=0,
        json_schema_extra={"prompt": "Enter risk factor (γ)", "prompt_on_new": True},
    )
    order_amount_shape_factor: Decimal = Field(
        default=Decimal("0"),
        description="The amount shape factor (η)",
        ge=0,
        le=1,
        json_schema_extra={"prompt": "Enter order amount shape factor (η)"},
    )
    min_spread: Decimal = Field(
        default=Decimal("0"),
        description="The minimum spread limit as percentage of the mid price.",
        ge=0,
        json_schema_extra={"prompt": "Enter minimum spread limit (as % of mid price)"},
    )
    order_refresh_time: float = Field(
        default=...,
        description="The frequency at which the orders' spreads will be re-evaluated.",
        gt=0.,
        json_schema_extra={"prompt": "How often do you want to refresh orders (in seconds)?", "prompt_on_new": True},
    )
    max_order_age: float = Field(
        default=1800.,
        description="A given order's maximum lifetime irrespective of spread.",
        gt=0.,
        json_schema_extra={"prompt": "How long do you want to cancel and replace bids and asks with the same price (in seconds)?"}
    )
    order_refresh_tolerance_pct: Decimal = Field(
        default=Decimal("0"),
        description="The range of spreads tolerated on refresh cycles. Orders over that range are cancelled and re-submitted.",
        ge=-10, le=10,
        json_schema_extra={"prompt": "Enter the percent change in price needed to refresh orders at each cycle (Enter 1 to indicate 1%)"},
    )
    filled_order_delay: float = Field(
        default=60.,
        description="The delay before placing a new order after an order fill.",
        gt=0.,
        json_schema_extra={"prompt": "How long do you want to wait before placing the next order if your order gets filled (in seconds)"},
    )
    inventory_target_base_pct: Decimal = Field(
        default=Decimal("50"),
        description="Defines the inventory target for the base asset.",
        ge=0,
        le=100,
        json_schema_extra={"prompt": "Enter the inventory target for the base asset (Enter 50 for 50%)", "prompt_on_new": True},
    )
    add_transaction_costs: bool = Field(
        default=False,
        description="If activated, transaction costs will be added to order prices.",
        json_schema_extra={"prompt": "Do you want to add transaction costs automatically to order prices? (Yes/No)"},
    )
    volatility_buffer_size: int = Field(
        default=200,
        description="The number of ticks that will be stored to calculate volatility.",
        ge=1,
        le=10_000,
        json_schema_extra={"prompt": "Enter amount of ticks that will be stored to estimate order book liquidity"},
    )
    trading_intensity_buffer_size: int = Field(
        default=200,
        description="The number of ticks that will be stored to calculate order book liquidity.",
        ge=1,
        le=10_000,
        json_schema_extra={"prompt": "Enter amount of ticks that will be stored to estimate order book liquidity"},
    )
    order_levels_mode: Union[SingleOrderLevelModel, MultiOrderLevelModel] = Field(
        default=SingleOrderLevelModel.model_construct(),
        description="Allows activating multi-order levels.",
        json_schema_extra={"prompt": f"Select the order levels mode ({'/'.join(list(ORDER_LEVEL_MODELS.keys()))})"},
    )
    order_override: Optional[Dict] = Field(
        default=None,
        description="Allows custom specification of the order levels and their spreads and amounts.",
    )
    hanging_orders_mode: Union[IgnoreHangingOrdersModel, TrackHangingOrdersModel] = Field(
        default=IgnoreHangingOrdersModel(),
        description="When tracking hanging orders, the orders on the side opposite to the filled orders remain active.",
        json_schema_extra={"prompt": f"Select the hanging orders mode ({'/'.join(list(HANGING_ORDER_MODELS.keys()))})"},
    )
    should_wait_order_cancel_confirmation: bool = Field(
        default=True,
        description="If activated, the strategy will await cancellation confirmation from the exchange before placing a new order.",
        json_schema_extra={
            "prompt": "Should the strategy wait to receive a confirmation for orders cancellation before creating a new set of orders? (Yes/No)",
        }
    )

    model_config = ConfigDict(title="avellaneda_perpetual_market_making")

    # === prompts ===

    @classmethod
    def order_amount_prompt(cls, model_instance: 'AvellanedaPerpetualMarketMakingConfigMap') -> str:
        trading_pair = model_instance.market
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        return f"What is the amount of {base_asset} per order?"

    # === specific validations ===

    # DIFFERENCE: Override derivative field validation instead of exchange
    @field_validator("derivative", mode="before")
    @classmethod
    def validate_derivative_field(cls, v: str):
        """Used for client-friendly error output."""
        ret = validate_derivative(v)
        if ret is not None:
            raise ValueError(ret)
        return v

    @field_validator("market", mode="after")
    @classmethod
    def validate_derivative_trading_pair(cls, v: str, info):
        # DIFFERENCE: Use derivative field instead of exchange for market validation
        if hasattr(info, 'data') and info.data:
            derivative = info.data.get("derivative")
            if derivative:
                ret = validate_market_trading_pair(derivative, v)
                if ret is not None:
                    raise ValueError(ret)
        return v

    @field_validator("execution_timeframe_mode", mode="before")
    @classmethod
    def validate_execution_timeframe(
        cls, v: Union[str, InfiniteModel, FromDateToDateModel, DailyBetweenTimesModel]
    ):
        if isinstance(v, (InfiniteModel, FromDateToDateModel, DailyBetweenTimesModel, Dict)):
            sub_model = v
        elif v not in EXECUTION_TIMEFRAME_MODELS:
            raise ValueError(
                f"Invalid timeframe, please choose value from {list(EXECUTION_TIMEFRAME_MODELS.keys())}"
            )
        else:
            sub_model = EXECUTION_TIMEFRAME_MODELS[v].model_construct()
        return sub_model

    @field_validator("order_refresh_tolerance_pct", mode="before")
    @classmethod
    def validate_order_refresh_tolerance_pct(cls, v: str):
        """Used for client-friendly error output."""
        ret = validate_decimal(v, min_value=Decimal("-10"), max_value=Decimal("10"), inclusive=True)
        if ret is not None:
            raise ValueError(ret)
        return v

    @field_validator("volatility_buffer_size", "trading_intensity_buffer_size", mode="before")
    @classmethod
    def validate_buffer_size(cls, v: str):
        """Used for client-friendly error output."""
        ret = validate_int(v, 1, 10_000)
        if ret is not None:
            raise ValueError(ret)
        return v

    @field_validator("order_levels_mode", mode="before")
    @classmethod
    def validate_order_levels_mode(cls, v: Union[str, SingleOrderLevelModel, MultiOrderLevelModel]):
        if isinstance(v, (SingleOrderLevelModel, MultiOrderLevelModel, Dict)):
            sub_model = v
        elif v not in ORDER_LEVEL_MODELS:
            raise ValueError(
                f"Invalid order levels mode, please choose value from {list(ORDER_LEVEL_MODELS.keys())}."
            )
        else:
            sub_model = ORDER_LEVEL_MODELS[v].model_construct()
        return sub_model

    @field_validator("hanging_orders_mode", mode="before")
    @classmethod
    def validate_hanging_orders_mode(cls, v: Union[str, IgnoreHangingOrdersModel, TrackHangingOrdersModel]):
        if isinstance(v, (TrackHangingOrdersModel, IgnoreHangingOrdersModel, Dict)):
            sub_model = v
        elif v not in HANGING_ORDER_MODELS:
            raise ValueError(
                f"Invalid hanging order mode, please choose value from {list(HANGING_ORDER_MODELS.keys())}."
            )
        else:
            sub_model = HANGING_ORDER_MODELS[v].model_construct()
        return sub_model

    # === generic validations (identical to spot version) ===

    @field_validator(
        "order_optimization_enabled",
        "add_transaction_costs",
        "should_wait_order_cancel_confirmation",
        mode="before"
    )
    @classmethod
    def validate_bool_field(cls, v: str):
        """Used for client-friendly error output."""
        if isinstance(v, str):
            ret = validate_bool(v)
            if ret is not None:
                raise ValueError(ret)
        return v

    @field_validator("risk_factor", "inventory_target_base_pct", mode="before")
    @classmethod
    def validate_decimal_field_gt_zero(cls, v: str):
        """Used for client-friendly error output."""
        ret = validate_decimal(v, 0, inclusive=False)
        if ret is not None:
            raise ValueError(ret)
        return v

    @field_validator("order_amount_shape_factor", "min_spread", mode="before")
    @classmethod
    def validate_decimal_field_gte_zero(cls, v: str):
        """Used for client-friendly error output - these can be zero."""
        ret = validate_decimal(v, 0, inclusive=True)
        if ret is not None:
            raise ValueError(ret)
        return v

    @field_validator(
        "order_refresh_time",
        "max_order_age",
        "filled_order_delay",
        mode="before"
    )
    @classmethod
    def validate_decimal_field_gte_zero(cls, v: str):
        """Used for client-friendly error output."""
        ret = validate_decimal(v, 0)
        if ret is not None:
            raise ValueError(ret)
        return v

    @field_validator("order_amount", mode="before")
    @classmethod
    def validate_order_amount(cls, v: str):
        """Used for client-friendly error output."""
        ret = validate_decimal(v, min_value=Decimal("0"), inclusive=False)
        if ret is not None:
            raise ValueError(ret)
        return v

    # DIFFERENCE: Override parent model validation to use derivative field
    @model_validator(mode="after")
    def post_validations(self):
        # Add the derivative to required_exchanges for proper initialization
        if hasattr(self, 'derivative') and self.derivative:
            required_exchanges.add(self.derivative)
        return self
