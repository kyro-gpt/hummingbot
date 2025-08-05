from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "SOL-USDC"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0020"),  # 0.20% maker fee
    taker_percent_fee_decimal=Decimal("0.0025"),  # 0.25% taker fee
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information

    Args:
        exchange_info: The exchange information for a trading pair from Backpack's /markets endpoint

    Returns:
        True if the trading pair is enabled for trading, False otherwise
    """
    # Check if market is open for trading
    order_book_state = exchange_info.get("orderBookState", "").upper()
    is_trading = order_book_state == "OPEN"

    # Check if it's a spot market (for spot connector)
    market_type = exchange_info.get("marketType", "").upper()
    is_spot = market_type == "SPOT"

    # Check if required fields are present
    has_symbol = bool(exchange_info.get("symbol"))
    has_base_symbol = bool(exchange_info.get("baseSymbol"))
    has_quote_symbol = bool(exchange_info.get("quoteSymbol"))

    return is_trading and is_spot and has_symbol and has_base_symbol and has_quote_symbol


class BackpackConfigMap(BaseConnectorConfigMap):
    connector: str = "backpack"
    backpack_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Backpack API key (public key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    backpack_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Backpack secret key (private key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="backpack")


KEYS = BackpackConfigMap.model_construct()
