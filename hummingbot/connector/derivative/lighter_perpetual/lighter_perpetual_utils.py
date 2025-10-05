"""
Lighter Perpetual Utilities

This module contains utility functions and configuration for the Lighter Perpetual exchange connector.
"""
from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

# Default trading fees for Lighter (to be updated with actual values)
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),  # 0.02%
    taker_percent_fee_decimal=Decimal("0.0005"),  # 0.05%
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = False  # Lighter is a decentralized exchange

EXAMPLE_PAIR = "BTC-USDC"

BROKER_ID = "HBOT"


class LighterPerpetualConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for Lighter Perpetual connector
    """
    connector: str = "lighter_perpetual"
    
    lighter_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter private key (hex format starting with 0x)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    
    lighter_account_index: int = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter account index",
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    
    lighter_api_key_index: int = Field(
        default=1,
        json_schema_extra={
            "prompt": "Enter your Lighter API key index (default: 1)",
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )


class LighterPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for Lighter Perpetual testnet connector
    """
    connector: str = "lighter_perpetual_testnet"
    
    lighter_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter testnet private key (hex format starting with 0x)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    
    lighter_account_index: int = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter testnet account index",
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    
    lighter_api_key_index: int = Field(
        default=1,
        json_schema_extra={
            "prompt": "Enter your Lighter testnet API key index (default: 1)",
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )


KEYS = LighterPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["lighter_perpetual_testnet"]

# Default fees for different domains
# Format: [maker_fee_percent, taker_fee_percent]
OTHER_DOMAINS_DEFAULT_FEES = {
    "lighter_perpetual_testnet": [0.05, 0.1]  # 0.05% maker, 0.1% taker
}

# Example trading pairs for different domains
OTHER_DOMAINS_EXAMPLE_PAIR = {
    "lighter_perpetual_testnet": "ETH-USDC"
}

# Configuration keys for different domains
OTHER_DOMAINS_KEYS = {
    "lighter_perpetual_testnet": LighterPerpetualTestnetConfigMap.model_construct()
}

# Domain parameters for different domains
OTHER_DOMAINS_PARAMETER = {
    "lighter_perpetual_testnet": "lighter_perpetual_testnet"
}


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Validate exchange information response from Lighter API
    
    :param exchange_info: Exchange information dictionary from API
    :return: True if the exchange information is valid, False otherwise
    """
    try:
        # Check for required fields in exchange info response
        required_fields = ["markets"]  # Adjust based on actual API response structure
        
        if not isinstance(exchange_info, dict):
            return False
            
        for field in required_fields:
            if field not in exchange_info:
                return False
                
        # Validate markets data if present
        if "markets" in exchange_info:
            markets = exchange_info["markets"]
            if not isinstance(markets, list) or len(markets) == 0:
                return False
                
        return True
        
    except Exception:
        return False


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    """
    Convert exchange trading pair format to Hummingbot format
    
    :param exchange_trading_pair: Trading pair in exchange format
    :return: Trading pair in Hummingbot format
    """
    # Lighter uses format like "BTC_USDC" or similar
    # Convert to Hummingbot format "BTC-USDC"
    if "_" in exchange_trading_pair:
        return exchange_trading_pair.replace("_", "-")
    elif "/" in exchange_trading_pair:
        return exchange_trading_pair.replace("/", "-")
    else:
        # If already in correct format or unknown format, return as-is
        return exchange_trading_pair


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    """
    Convert Hummingbot trading pair format to exchange format
    
    :param hb_trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")
    :return: Trading pair in exchange format
    """
    # Convert from Hummingbot format "BTC-USDC" to exchange format
    # This will depend on how Lighter represents trading pairs
    return hb_trading_pair.replace("-", "_")


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    """
    Generate a new client order ID for Lighter
    
    :param is_buy: True for buy orders, False for sell orders
    :param trading_pair: The trading pair for the order
    :return: New client order ID string
    """
    import time
    import random
    
    side = "B" if is_buy else "S"
    timestamp = int(time.time() * 1000)  # milliseconds
    random_part = random.randint(1000, 9999)
    
    return f"{BROKER_ID}{side}{timestamp}{random_part}"


def validate_trading_pair(trading_pair: str) -> bool:
    """
    Validate if a trading pair is in the correct format
    
    :param trading_pair: Trading pair to validate
    :return: True if valid, False otherwise
    """
    if not isinstance(trading_pair, str):
        return False
        
    # Check for Hummingbot format: BASE-QUOTE
    parts = trading_pair.split("-")
    if len(parts) != 2:
        return False
        
    base, quote = parts
    if not base or not quote:
        return False
        
    # Check that both parts are valid symbols (letters/numbers only)
    if not base.replace("_", "").isalnum() or not quote.replace("_", "").isalnum():
        return False
        
    return True


def format_decimal_places(value: Decimal, precision: int) -> Decimal:
    """
    Format decimal value to specified precision
    
    :param value: Decimal value to format
    :param precision: Number of decimal places
    :return: Formatted decimal value
    """
    if precision <= 0:
        return value.quantize(Decimal("1"))
    else:
        quantizer = Decimal("0." + "0" * (precision - 1) + "1")
        return value.quantize(quantizer)


def safe_ensure_future(coro, loop=None):
    """
    Safely ensure future for coroutine
    
    :param coro: Coroutine to ensure
    :param loop: Event loop (optional)
    :return: Future object
    """
    import asyncio
    
    if loop is None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    
    return asyncio.ensure_future(coro, loop=loop)
