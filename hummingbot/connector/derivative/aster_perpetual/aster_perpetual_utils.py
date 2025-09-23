from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

# TODO: Verify Aster's actual fee structure
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0004"),
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"

BROKER_ID = "x-aster-hb"  # To be assigned by Aster team


class AsterPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "aster_perpetual"

    # Web3 Authentication Configuration - Different from traditional API keys
    aster_perpetual_user_wallet: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual main wallet address (user)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_signer_wallet: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual API wallet address (signer)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual signer wallet private key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )


KEYS = AsterPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["aster_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"aster_perpetual_testnet": "aster_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"aster_perpetual_testnet": "BTC-USDT"}
OTHER_DOMAINS_DEFAULT_FEES = {"aster_perpetual_testnet": [0.02, 0.04]}


class AsterPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "aster_perpetual_testnet"

    # Testnet Web3 Authentication
    aster_perpetual_testnet_user_wallet: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual testnet main wallet address (user)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_testnet_signer_wallet: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual testnet API wallet address (signer)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_testnet_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual testnet signer wallet private key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    model_config = ConfigDict(title="aster_perpetual")


OTHER_DOMAINS_KEYS = {"aster_perpetual_testnet": AsterPerpetualTestnetConfigMap.model_construct()}
