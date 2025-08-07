from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDC"  # Typical perpetual pair

# Derivative fees are typically higher than spot
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),  # 0.02% maker fee (typical for derivatives)
    taker_percent_fee_decimal=Decimal("0.0004"),  # 0.04% taker fee (typical for derivatives)
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information

    Args:
        exchange_info: The exchange information for a trading pair from Backpack's /markets endpoint

    Returns:
        True if the trading pair is enabled for derivative trading, False otherwise
    """
    # Check if market is open for trading
    order_book_state = exchange_info.get("orderBookState", "").upper()
    is_trading = order_book_state == "OPEN"

    # Check if it's a futures/perpetual market (for derivative connector)
    market_type = exchange_info.get("marketType", "").upper()
    is_derivative = market_type in ["FUTURE", "PERPETUAL"]

    # Check if required fields are present
    has_symbol = bool(exchange_info.get("symbol"))
    has_base_symbol = bool(exchange_info.get("baseSymbol"))
    has_quote_symbol = bool(exchange_info.get("quoteSymbol"))

    return is_trading and is_derivative and has_symbol and has_base_symbol and has_quote_symbol


def validate_position_mode_config(position_mode: str) -> bool:
    """
    Validate position mode configuration.

    Args:
        position_mode: Position mode string ("ONEWAY" or "HEDGE")

    Returns:
        True if valid, False otherwise
    """
    valid_modes = ["ONEWAY", "HEDGE"]
    return position_mode.upper() in valid_modes


def validate_leverage_config(leverage: int) -> bool:
    """
    Validate leverage configuration.

    Args:
        leverage: Leverage value

    Returns:
        True if valid, False otherwise
    """
    # Conservative validation - will be updated based on Backpack's actual limits
    return 1 <= leverage <= 20


def validate_derivative_trading_pair(trading_pair: str) -> bool:
    """
    Validate that a trading pair is suitable for derivative trading.

    Args:
        trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

    Returns:
        True if valid for derivatives, False otherwise
    """
    if not trading_pair or "-" not in trading_pair:
        return False

    base, quote = trading_pair.split("-", 1)

    # Basic validation - both symbols should be non-empty
    if not base or not quote:
        return False

    # For derivatives, quote is typically a stablecoin
    common_quote_assets = ["USDC", "USDT", "USD", "BUSD"]
    return quote.upper() in common_quote_assets


def get_collateral_token_from_trading_pair(trading_pair: str) -> str:
    """
    Get the collateral token for a trading pair.

    Args:
        trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

    Returns:
        Collateral token symbol
    """
    if not trading_pair or "-" not in trading_pair:
        return "USDC"  # Default collateral

    base, quote = trading_pair.split("-", 1)
    return quote.upper()


class BackpackPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "backpack_perpetual"
    backpack_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Backpack Perpetual API key (public key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    backpack_perpetual_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Backpack Perpetual secret key (private key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )

    # Derivative-specific configuration options
    backpack_perpetual_leverage: int = Field(
        default=1,
        json_schema_extra={
            "prompt": "Enter default leverage (1-20)",
            "prompt_on_new": False,
        }
    )

    backpack_perpetual_position_mode: str = Field(
        default="ONEWAY",
        json_schema_extra={
            "prompt": "Enter position mode (ONEWAY/HEDGE)",
            "prompt_on_new": False,
        }
    )

    model_config = ConfigDict(title="backpack_perpetual")


KEYS = BackpackPerpetualConfigMap.model_construct()
