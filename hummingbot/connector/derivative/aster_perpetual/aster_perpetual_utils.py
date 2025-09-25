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

    # Unified Authentication Configuration (version is hardcoded internally)
    aster_perpetual_user_wallet: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual user wallet address",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual API Key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual Secret Key (HMAC secret for v1, private key for v3)",
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
        default="",
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual testnet API wallet address (signer) [auto-derived from private key]",
            "is_secure": True, "is_connect_key": False, "prompt_on_new": False}
    )
    aster_perpetual_testnet_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster Perpetual testnet signer wallet private key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    model_config = ConfigDict(title="aster_perpetual")


OTHER_DOMAINS_KEYS = {"aster_perpetual_testnet": AsterPerpetualTestnetConfigMap.model_construct()}
