import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import patch
from urllib.parse import urlencode

from aioresponses import aioresponses
from aioresponses.core import RequestCall

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative
from hummingbot.connector.perpetual_trading import PerpetualTrading
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase


class AsterPerpetualDerivativeTests(AbstractPerpetualDerivativeTests.PerpetualDerivativeTests):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # Web3 credentials for Aster (different from API key/secret)
        cls.user_wallet = "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"  # noqa: mock
        cls.signer_wallet = "0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0"  # noqa: mock
        cls.private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"  # noqa: mock
        cls.quote_asset = "USDT"
        cls.trading_pair = combine_to_hb_trading_pair(cls.base_asset, cls.quote_asset)

    @property
    def all_symbols_url(self):
        url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_URL)
        return url

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(CONSTANTS.TICKER_PRICE_CHANGE_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(CONSTANTS.PING_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_URL)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_INFO_URL)
        return url

    @property
    def funding_info_url(self):
        url = web_utils.public_rest_url(CONSTANTS.MARK_PRICE_URL)
        return url

    @property
    def funding_payment_url(self):
        url = web_utils.private_rest_url(CONSTANTS.GET_INCOME_HISTORY_URL)
        return url

    # === Mock Response Properties ===

    @property
    def all_symbols_request_mock_response(self):
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING"
                }
            ]
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "symbol": "BTCUSDT",
            "lastPrice": "50000.0"
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        return "INVALID-PAIR", {
            "symbols": [
                {
                    "symbol": "INVALID",
                    "baseAsset": "INVALID",
                    "quoteAsset": "PAIR",
                    "contractType": "PERPETUAL",
                    "status": "BREAK"  # Invalid status
                }
            ]
        }

    @property
    def network_status_request_successful_mock_response(self):
        return {}  # Ping returns empty response on success

    @property
    def trading_rules_request_mock_response(self):
        return {
            "symbols": [
                {
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "baseAsset": self.base_asset,
                    "quoteAsset": self.quote_asset,
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "filters": []
                }
            ]
        }

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "code": -1121,
            "msg": "Invalid symbol."
        }

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "orderId": "12345",
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "NEW",
            "clientOrderId": "test_order_id",
            "updateTime": 1640001112223
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "assets": [
                {
                    "asset": self.base_asset,
                    "walletBalance": "10.0",
                    "availableBalance": "8.0"
                },
                {
                    "asset": self.quote_asset,
                    "walletBalance": "50000.0",
                    "availableBalance": "45000.0"
                }
            ]
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "assets": [
                {
                    "asset": self.base_asset,
                    "walletBalance": "10.0",
                    "availableBalance": "8.0"
                }
            ]
        }

    @property
    def balance_event_websocket_update(self):
        return {
            "e": "ACCOUNT_UPDATE",
            "E": 1640001112223,
            "a": {
                "B": [
                    {
                        "a": self.base_asset,
                        "wb": "10.0",
                        "cw": "8.0"
                    }
                ]
            }
        }

    # === Expected Values Properties ===

    @property
    def expected_latest_price(self):
        return 50000.0

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0"),
            max_order_size=Decimal("0"),
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        return "Invalid symbol."

    @property
    def expected_exchange_order_id(self):
        return "12345"

    @property
    def is_cancel_request_executed_synchronously_by_server(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("50000")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(amount=Decimal("10"), token=self.quote_asset)]
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "trade_123"

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY, PositionMode.HEDGE]

    @property
    def funding_info_mock_response(self):
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "indexPrice": "50000.0",
            "markPrice": "50001.0",
            "lastFundingRate": "0.0001",
            "nextFundingTime": 1640001112000
        }

    @property
    def empty_funding_payment_mock_response(self):
        return []

    @property
    def funding_payment_mock_response(self):
        return [
            {
                "time": 1640001112000,
                "income": "-0.001",
                "asset": self.quote_asset,
                "incomeType": "FUNDING_FEE"
            }
        ]

    # === Method Implementations ===

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"

    def create_exchange_instance(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        return AsterPerpetualDerivative(
            client_config_map=client_config_map,
            aster_perpetual_user_wallet=self.user_wallet,
            aster_perpetual_signer_wallet=self.signer_wallet,
            aster_perpetual_private_key=self.private_key,
            trading_pairs=[self.trading_pair],
            domain=CONSTANTS.TESTNET_DOMAIN,
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        """Validate that Web3 auth credentials are present"""
        request_data = request_call.kwargs.get("data", {})
        if isinstance(request_data, str):
            request_data = json.loads(request_data) if request_data else {}
        
        # Check for Web3 authentication parameters
        self.assertIn("user", request_data)
        self.assertIn("signer", request_data)
        self.assertIn("nonce", request_data)
        self.assertIn("signature", request_data)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        """Validate order creation request parameters"""
        request_data = request_call.kwargs.get("data", {})
        if isinstance(request_data, str):
            request_data = json.loads(request_data) if request_data else {}

        self.assertEqual(request_data["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))
        self.assertEqual(request_data["side"], "BUY" if order.trade_type == TradeType.BUY else "SELL")
        self.assertEqual(request_data["type"], "MARKET" if order.order_type == OrderType.MARKET else "LIMIT")
        self.assertEqual(request_data["newClientOrderId"], order.client_order_id)

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        """Validate order cancellation request parameters"""
        request_params = request_call.kwargs.get("params", {})
        
        self.assertEqual(request_params["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))
        self.assertEqual(request_params["origClientOrderId"], order.client_order_id)

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        """Validate order status request parameters"""
        request_params = request_call.kwargs.get("params", {})
        
        self.assertEqual(request_params["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))
        self.assertEqual(request_params["origClientOrderId"], order.client_order_id)

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        """Validate trades request parameters"""
        request_params = request_call.kwargs.get("params", {})
        
        self.assertEqual(request_params["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))

    # === Configuration Methods ===

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        """Configure successful order cancellation response"""
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "orderId": self.expected_exchange_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "CANCELED",
            "clientOrderId": order.client_order_id
        }
        
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure erroneous order cancellation response"""
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "code": CONSTANTS.UNKNOWN_ORDER_ERROR_CODE,
            "msg": CONSTANTS.UNKNOWN_ORDER_MESSAGE
        }
        
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_completely_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure completely filled order status response"""
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "orderId": self.expected_exchange_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "FILLED",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure canceled order status response"""
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "orderId": self.expected_exchange_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "CANCELED",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_open_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure open order status response"""
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "orderId": self.expected_exchange_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "NEW",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_partially_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure partially filled order status response"""
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "orderId": self.expected_exchange_order_id,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "PARTIALLY_FILLED",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_successful_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        """Configure successful leverage setting response"""
        url = web_utils.private_rest_url(CONSTANTS.SET_LEVERAGE_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "leverage": leverage,
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        }
        
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)

    def configure_failed_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        """Configure failed leverage setting response"""
        url = web_utils.private_rest_url(CONSTANTS.SET_LEVERAGE_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        error_msg = "Invalid leverage"
        response = {
            "code": -4028,
            "msg": error_msg
        }
        
        mock_api.post(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url, error_msg

    def configure_successful_set_position_mode(
        self,
        position_mode: PositionMode,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        """Configure successful position mode setting response"""
        url = web_utils.private_rest_url(CONSTANTS.CHANGE_POSITION_MODE_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {"msg": "success"}
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)

    def configure_failed_set_position_mode(
        self,
        position_mode: PositionMode,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        """Configure failed position mode setting response"""
        url = web_utils.private_rest_url(CONSTANTS.CHANGE_POSITION_MODE_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        error_msg = "Position mode change failed"
        response = {
            "code": -4059,
            "msg": error_msg
        }
        
        mock_api.post(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url, error_msg

    # === WebSocket Event Methods ===

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1640001112223,
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": self.expected_exchange_order_id,
                "X": "NEW"
            }
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1640001112223,
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": self.expected_exchange_order_id,
                "X": "CANCELED"
            }
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1640001112223,
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": self.expected_exchange_order_id,
                "X": "FILLED"
            }
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1640001112223,
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": self.expected_exchange_order_id,
                "X": "FILLED",
                "l": str(self.expected_partial_fill_amount),
                "L": str(self.expected_partial_fill_price),
                "n": "10",  # Commission
                "N": self.quote_asset  # Commission asset
            }
        }

    def position_event_for_full_fill_websocket_update(self, order: InFlightOrder, unrealized_pnl: float):
        return {
            "e": "ACCOUNT_UPDATE",
            "E": 1640001112223,
            "a": {
                "P": [
                    {
                        "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "pa": str(self.expected_partial_fill_amount),
                        "ep": str(self.expected_partial_fill_price),
                        "up": str(unrealized_pnl),
                        "ps": "LONG" if order.trade_type == TradeType.BUY else "SHORT"
                    }
                ]
            }
        }

    def funding_info_event_for_websocket_update(self):
        return {
            "e": "markPriceUpdate",
            "E": 1640001112223,
            "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "p": "50001.0",
            "i": "50000.0",
            "r": "0.0001",
            "T": 1640001112000
        }

    # === Additional Required Methods ===

    def configure_one_successful_one_erroneous_cancel_all_response(
        self,
        successful_order: InFlightOrder,
        erroneous_order: InFlightOrder,
        mock_api: aioresponses,
    ) -> List[str]:
        """Configure mixed success/error cancel all response"""
        # This would require implementing cancel all functionality
        # For now, return empty list
        return []

    def configure_erroneous_http_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure erroneous fill trade response"""
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "code": -2013,
            "msg": "Order does not exist."
        }
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url

    def configure_http_error_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure HTTP error order status response"""
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = {
            "code": -2013,
            "msg": "Order does not exist."
        }
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url

    def configure_partial_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure partial fill trade response"""
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = [
            {
                "orderId": self.expected_exchange_order_id,
                "id": self.expected_fill_trade_id,
                "price": str(self.expected_partial_fill_price),
                "qty": str(self.expected_partial_fill_amount),
                "quoteQty": str(self.expected_partial_fill_price * self.expected_partial_fill_amount),
                "commission": "10",
                "commissionAsset": self.quote_asset,
                "time": 1640001112223,
                "positionSide": "LONG"
            }
        ]
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """Configure full fill trade response"""
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        response = [
            {
                "orderId": self.expected_exchange_order_id,
                "id": self.expected_fill_trade_id,
                "price": str(order.price),
                "qty": str(order.amount),
                "quoteQty": str(order.price * order.amount),
                "commission": "10",
                "commissionAsset": self.quote_asset,
                "time": 1640001112223,
                "positionSide": "LONG"
            }
        ]
        
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url
