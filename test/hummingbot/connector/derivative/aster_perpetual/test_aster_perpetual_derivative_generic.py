import asyncio
from decimal import Decimal
from typing import List
from unittest.mock import AsyncMock, patch
import aiohttp
import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionMode
import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS



class AsterPerpetualDerivativeTests(AbstractPerpetualDerivativeTests.PerpetualDerivativeTests):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # Web3 credentials for Aster
        cls.user_wallet = "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"  # noqa: mock
        cls.signer_wallet = "0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0"  # noqa: mock
        cls.private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"  # noqa: mock
        cls.quote_asset = "USDT"
        cls.trading_pair = combine_to_hb_trading_pair(cls.base_asset, cls.quote_asset)

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

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}{quote_token}"


    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY, PositionMode.HEDGE]

    # === ASTER-SPECIFIC URL PROPERTIES ===
    
    @property
    def all_symbols_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        return web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_URL, domain=CONSTANTS.TESTNET_DOMAIN)
    
    @property  
    def latest_prices_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        import re
        url = web_utils.public_rest_url(CONSTANTS.TICKER_PRICE_CHANGE_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        return re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
    
    @property
    def network_status_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        return web_utils.public_rest_url(CONSTANTS.PING_URL, domain=CONSTANTS.TESTNET_DOMAIN)
    
    @property
    def trading_rules_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        return web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_URL, domain=CONSTANTS.TESTNET_DOMAIN)
    
    @property
    def order_creation_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        import re
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        return re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
    
    @property
    def balance_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        return web_utils.private_rest_url(CONSTANTS.ACCOUNT_INFO_URL, domain=CONSTANTS.TESTNET_DOMAIN)
    
    @property
    def funding_info_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        import re
        url = web_utils.public_rest_url(CONSTANTS.MARK_PRICE_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        return re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
    
    @property
    def funding_payment_url(self):
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        import re
        url = web_utils.private_rest_url(CONSTANTS.GET_INCOME_HISTORY_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        return re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
    @property
    def all_symbols_request_mock_response(self):
        return {
            "symbols": [
                {
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "baseAsset": self.base_asset,
                    "quoteAsset": self.quote_asset,
                    "contractType": "PERPETUAL",
                    "status": "TRADING"
                }
            ]
        }
    
    @property
    def latest_prices_request_mock_response(self):
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "lastPrice": "50000.0"
        }
    
    @property
    def all_symbols_including_invalid_pair_mock_response(self):
        return "INVALID-PAIR", {
            "symbols": [
                {
                    "symbol": "INVALID",
                    "baseAsset": "INVALID", 
                    "quoteAsset": "PAIR",
                    "contractType": "PERPETUAL",
                    "status": "BREAK"
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
                    "walletBalance": "15.0",
                    "availableBalance": "10.0"
                },
                {
                    "asset": self.quote_asset,
                    "walletBalance": "2000.0", 
                    "availableBalance": "2000.0"
                }
            ]
        }
    
    @property
    def balance_request_mock_response_only_base(self):
        return {
            "assets": [
                {
                    "asset": self.base_asset,
                    "walletBalance": "15.0",
                    "availableBalance": "10.0"
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
                        "wb": "15.0",
                        "cw": "10.0"
                    }
                ]
            }
        }
    @property
    def expected_latest_price(self): return 50000.0
    @property
    def expected_trading_rule(self):
        from hummingbot.connector.trading_rule import TradingRule
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("1000000"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.001"),
        )
    @property
    def expected_logged_error_for_erroneous_trading_rule(self): return "Invalid symbol."
    @property
    def expected_exchange_order_id(self): return "12345"
    @property
    def is_cancel_request_executed_synchronously_by_server(self): return True
    @property
    def is_order_fill_http_update_included_in_status_update(self): return True
    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self): return False
    @property
    def expected_partial_fill_price(self): return Decimal("50000")
    @property
    def expected_partial_fill_amount(self): return Decimal("0.5")
    @property
    def expected_fill_fee(self): 
        from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(amount=Decimal("10"), token=self.quote_asset)]
        )
    @property
    def expected_fill_trade_id(self): return "trade_123"
    @property
    def funding_info_mock_response(self):
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "indexPrice": "1",
            "markPrice": "2", 
            "lastFundingRate": "3",
            "nextFundingTime": 1657099053
        }
    
    @property
    def empty_funding_payment_mock_response(self):
        return []
    
    @property
    def funding_payment_mock_response(self):
        return [
            {
                "time": 1657110053000,
                "income": "-0.001", 
                "asset": self.quote_asset,
                "incomeType": "FUNDING_FEE",
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
            }
        ]
    
    def validate_auth_credentials_present(self, request_call):
        """Validate that Web3 auth credentials are present"""
        request_data = request_call.kwargs.get("data", {})
        if isinstance(request_data, str):
            import json
            request_data = json.loads(request_data) if request_data else {}
        
        request_params = request_call.kwargs.get("params", {})
        # Check for Web3 authentication parameters in params
        if request_params:
            self.assertIn("user", request_params)
            self.assertIn("signer", request_params)
            self.assertIn("nonce", request_params)
            self.assertIn("signature", request_params)
        
    def validate_order_creation_request(self, order, request_call):
        """Validate order creation request parameters"""
        request_data = request_call.kwargs.get("data", {})
        if isinstance(request_data, str):
            import json
            request_data = json.loads(request_data) if request_data else {}

        if request_data:
            self.assertEqual(request_data["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))
            self.assertEqual(request_data["side"], "BUY" if order.trade_type.name == "BUY" else "SELL")
            self.assertEqual(request_data["newClientOrderId"], order.client_order_id)
        
    def validate_order_cancelation_request(self, order, request_call):
        """Validate order cancellation request parameters"""
        request_params = request_call.kwargs.get("params", {})
        self.assertEqual(request_params["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))
        self.assertEqual(request_params["origClientOrderId"], order.client_order_id)
        
    def validate_order_status_request(self, order, request_call):
        """Validate order status request parameters"""
        request_params = request_call.kwargs.get("params", {})
        self.assertEqual(request_params["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))
        self.assertEqual(request_params["origClientOrderId"], order.client_order_id)
        
    def validate_trades_request(self, order, request_call):
        """Validate trades request parameters"""
        request_params = request_call.kwargs.get("params", {})
        self.assertEqual(request_params["symbol"], self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset))
    def configure_all_symbols_response(self, mock_api, callback=None):
        """Configure all symbols response for test_all_trading_pairs"""
        import json
        from typing import Optional, Callable
        url = self.all_symbols_url
        response = self.all_symbols_request_mock_response
        mock_api.get(url, body=json.dumps(response), callback=callback)
        return [url]
    
    def configure_trading_rules_response(self, mock_api, callback=None):
        """Configure trading rules response"""
        import json
        url = self.trading_rules_url
        response = self.trading_rules_request_mock_response
        mock_api.get(url, body=json.dumps(response), callback=callback)
        return [url]
    
    def configure_erroneous_trading_rules_response(self, mock_api, callback=None):
        """Configure erroneous trading rules response"""
        import json
        url = self.trading_rules_url
        response = self.trading_rules_request_erroneous_mock_response
        mock_api.get(url, body=json.dumps(response), callback=callback, status=400)
        return [url]
    
    def _configure_balance_response(self, response, mock_api, callback=None):
        """Configure balance response"""
        import json, re
        url = self.balance_url
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
    
    def configure_successful_cancelation_response(self, order, mock_api, callback=None):
        """Configure successful cancellation response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {
            "orderId": "12345",
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "CANCELED",
            "clientOrderId": order.client_order_id
        }
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback)
        return url
    def configure_erroneous_cancelation_response(self, order, mock_api, callback=None):
        """Configure erroneous cancellation response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {"code": -1000, "msg": "Server error"}
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback, status=500)
        return url
    def configure_order_creation_response(self, order, mock_api, callback=None):
        """Configure order creation response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = self.order_creation_request_successful_mock_response
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url
    
    def configure_one_successful_one_erroneous_cancel_all_response(self, successful_order, erroneous_order, mock_api):
        """Configure mixed success/error cancel all response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        
        # Configure successful cancellation for first order
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        def cancel_callback(url, **kwargs):
            params = kwargs.get("params", {})
            if params.get("origClientOrderId") == successful_order.client_order_id:
                return json.dumps({
                    "orderId": "12345",
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "status": "CANCELED",
                    "clientOrderId": successful_order.client_order_id
                })
            else:
                return json.dumps({"code": -2011, "msg": "Unknown order sent."})
        
        mock_api.delete(regex_url, callback=cancel_callback)
        return [url]
    def configure_completely_filled_order_status_response(self, order, mock_api, callback=None):
        """Configure completely filled order status response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        
        # Mock order status response
        order_url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        order_regex_url = re.compile(f"^{order_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        order_response = {
            "orderId": "12345",
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "FILLED",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        mock_api.get(order_regex_url, body=json.dumps(order_response), callback=callback)
        
        # Mock trade fills response
        trades_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        trades_regex_url = re.compile(f"^{trades_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        trades_response = [
            {
                "orderId": "12345",
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
        mock_api.get(trades_regex_url, body=json.dumps(trades_response), callback=callback)
        
        return order_url
    def configure_canceled_order_status_response(self, order, mock_api, callback=None):
        """Configure canceled order status response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        
        # Mock order status response
        order_url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        order_regex_url = re.compile(f"^{order_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        order_response = {
            "orderId": "12345",
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "CANCELED",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        mock_api.get(order_regex_url, body=json.dumps(order_response), callback=callback)
        
        # Mock trade fills response (empty for canceled order)
        trades_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        trades_regex_url = re.compile(f"^{trades_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        trades_response = []
        mock_api.get(trades_regex_url, body=json.dumps(trades_response), callback=callback)
        
        return order_url
    def configure_erroneous_http_fill_trade_response(self, order, mock_api, callback=None):
        """Configure erroneous fill trade response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {"code": -2013, "msg": "Order does not exist."}
        mock_api.get(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url
    def configure_open_order_status_response(self, order, mock_api, callback=None):
        """Configure open order status response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        
        # Mock order status response
        order_url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        order_regex_url = re.compile(f"^{order_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        order_response = {
            "orderId": "12345",
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "NEW",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        mock_api.get(order_regex_url, body=json.dumps(order_response), callback=callback)
        
        # Mock trade fills response (empty for open order)
        trades_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        trades_regex_url = re.compile(f"^{trades_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        trades_response = []
        mock_api.get(trades_regex_url, body=json.dumps(trades_response), callback=callback)
        
        return order_url
    def configure_http_error_order_status_response(self, order, mock_api, callback=None):
        """Configure HTTP error order status response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {"code": -2013, "msg": "Order does not exist."}
        mock_api.get(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url
    def configure_partially_filled_order_status_response(self, order, mock_api, callback=None):
        """Configure partially filled order status response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        
        # Mock order status response
        order_url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        order_regex_url = re.compile(f"^{order_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        order_response = {
            "orderId": "12345",
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "status": "PARTIALLY_FILLED",
            "clientOrderId": order.client_order_id,
            "updateTime": 1640001112223
        }
        mock_api.get(order_regex_url, body=json.dumps(order_response), callback=callback)
        
        # Mock trade fills response
        trades_url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        trades_regex_url = re.compile(f"^{trades_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        trades_response = [
            {
                "orderId": "12345",
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
        mock_api.get(trades_regex_url, body=json.dumps(trades_response), callback=callback)
        
        return order_url
    def configure_partial_fill_trade_response(self, order, mock_api, callback=None):
        """Configure partial fill trade response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = [
            {
                "orderId": "12345",
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
        
    def configure_full_fill_trade_response(self, order, mock_api, callback=None):
        """Configure full fill trade response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNT_TRADE_LIST_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = [
            {
                "orderId": "12345",
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
    def order_event_for_new_order_websocket_update(self, order):
        return {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1640001112223,
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": "12345",
                "X": "NEW"
            }
        }
    
    def order_event_for_canceled_order_websocket_update(self, order):
        return {
            "e": "ORDER_TRADE_UPDATE", 
            "E": 1640001112223,
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": "12345",
                "X": "CANCELED"
            }
        }
    
    def order_event_for_full_fill_websocket_update(self, order):
        return {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1640001112223, 
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": "12345",
                "X": "FILLED"
            }
        }
    
    def trade_event_for_full_fill_websocket_update(self, order):
        return {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1640001112223,
            "o": {
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "c": order.client_order_id,
                "i": "12345", 
                "X": "FILLED",
                "l": "1.0",  # Last filled quantity
                "L": "50000",  # Last filled price
                "n": "10",  # Commission
                "N": self.quote_asset  # Commission asset
            }
        }
    def configure_failed_set_leverage(self, leverage, mock_api, callback=None):
        """Configure failed leverage setting response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.SET_LEVERAGE_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        error_msg = "Invalid leverage"
        response = {"code": -4028, "msg": error_msg}
        mock_api.post(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url, error_msg
        
    def configure_successful_set_leverage(self, leverage, mock_api, callback=None):
        """Configure successful leverage setting response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.SET_LEVERAGE_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {"leverage": leverage, "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
    def position_event_for_full_fill_websocket_update(self, order, unrealized_pnl):
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
                        "ps": "LONG"
                    }
                ]
            }
        }
        
    def funding_info_event_for_websocket_update(self):
        return {
            "stream": f"{self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset).lower()}@markPrice",
            "data": {
                "e": "markPriceUpdate",
                "E": 1640001112223,
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "p": "2",
                "i": "1", 
                "r": "3",
                "T": 1657099053
            }
        }
    def configure_successful_set_position_mode(self, position_mode, mock_api, callback=None):
        """Configure successful position mode setting response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.CHANGE_POSITION_MODE_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {"msg": "success"}
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        
    def configure_failed_set_position_mode(self, position_mode, mock_api, callback=None):
        """Configure failed position mode setting response"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.CHANGE_POSITION_MODE_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        error_msg = "Position mode change failed"
        response = {"code": -4059, "msg": error_msg}
        mock_api.post(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url, error_msg
    def configure_order_not_found_error_cancelation_response(self, order, mock_api, callback=None):
        """Configure order not found error for cancellation"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {"code": CONSTANTS.UNKNOWN_ORDER_ERROR_CODE, "msg": CONSTANTS.UNKNOWN_ORDER_MESSAGE}
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url
        
    def configure_order_not_found_error_order_status_response(self, order, mock_api, callback=None):
        """Configure order not found error for order status"""
        import json, re
        import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
        url = web_utils.private_rest_url(CONSTANTS.ORDER_URL, domain=CONSTANTS.TESTNET_DOMAIN)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {"code": CONSTANTS.UNKNOWN_ORDER_ERROR_CODE, "msg": CONSTANTS.UNKNOWN_ORDER_MESSAGE}
        mock_api.get(regex_url, body=json.dumps(response), callback=callback, status=400)
        return url
    def test_get_buy_and_sell_collateral_tokens(self):
        """Test collateral token methods"""
        exchange = self.create_exchange_instance()
        
        # Set up a basic trading rule so the method doesn't fail
        from hummingbot.connector.trading_rule import TradingRule
        trading_rule = TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("1000000"),
        )
        exchange._trading_rules[self.trading_pair] = trading_rule
        
        buy_collateral_token = exchange.get_buy_collateral_token(self.trading_pair)
        sell_collateral_token = exchange.get_sell_collateral_token(self.trading_pair)
        
        self.assertEqual(buy_collateral_token, self.quote_asset)
        self.assertEqual(sell_collateral_token, self.quote_asset)
