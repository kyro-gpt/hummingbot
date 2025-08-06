import asyncio
import base64
import hashlib
import json
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall

# Import cryptography for generating Ed25519 test keys
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderFilledEvent


class BackpackExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "SOL"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.MARKETS_PATH_URL)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_PATH_URL)
        url = f"{url}?symbol={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(CONSTANTS.PING_PATH_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.public_rest_url(CONSTANTS.MARKETS_PATH_URL)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.BALANCES_PATH_URL)
        return url

    @property
    def all_symbols_request_mock_response(self):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "baseSymbol": self.base_asset,
                "quoteSymbol": self.quote_asset,
                "marketType": "SPOT",
                "orderBookState": "Open",
                "filters": {
                    "price": {
                        "minPrice": "0.01",
                        "maxPrice": None,
                        "tickSize": "0.01"
                    },
                    "quantity": {
                        "minQuantity": "0.01",
                        "maxQuantity": None,
                        "stepSize": "0.01"
                    }
                }
            }
        ]

    @property
    def latest_prices_request_mock_response(self):
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "lastPrice": str(self.expected_latest_price),
            "priceChange": "1.5",
            "priceChangePercent": "0.15",
            "volume": "10000",
            "quoteVolume": "999999.50",
            "openTime": 1640995200000,
            "closeTime": 1640995260000,
            "firstId": 1,
            "lastId": 1000,
            "count": 1000
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "baseSymbol": self.base_asset,
                "quoteSymbol": self.quote_asset,
                "marketType": "SPOT",
                "orderBookState": "Open",
                "filters": {
                    "price": {
                        "minPrice": "0.01",
                        "maxPrice": None,
                        "tickSize": "0.01"
                    },
                    "quantity": {
                        "minQuantity": "0.01",
                        "maxQuantity": None,
                        "stepSize": "0.01"
                    }
                }
            },
            {
                "symbol": self.exchange_symbol_for_tokens("INVALID", "PAIR"),
                "baseSymbol": "INVALID",
                "quoteSymbol": "PAIR",
                "marketType": "SPOT",
                "orderBookState": "Closed",
                "filters": {
                    "price": {
                        "minPrice": "0.01",
                        "maxPrice": None,
                        "tickSize": "0.01"
                    },
                    "quantity": {
                        "minQuantity": "0.01",
                        "maxQuantity": None,
                        "stepSize": "0.01"
                    }
                }
            }
        ]
        return "INVALID-PAIR", response

    @property
    def network_status_request_successful_mock_response(self):
        return "pong"

    @property
    def trading_rules_request_mock_response(self):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "baseSymbol": self.base_asset,
                "quoteSymbol": self.quote_asset,
                "marketType": "SPOT",
                "orderBookState": "Open",
                "filters": {
                    "price": {
                        "minPrice": "0.01",
                        "maxPrice": None,
                        "tickSize": "0.01"
                    },
                    "quantity": {
                        "minQuantity": "0.01",
                        "maxQuantity": None,
                        "stepSize": "0.01"
                    }
                }
            }
        ]

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return [
            {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "baseSymbol": self.base_asset,
                "quoteSymbol": self.quote_asset,
                "marketType": "SPOT",
                "orderBookState": "Open"
                # Missing "filters" field to trigger error
            }
        ]

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "orderType": "Limit",
            "id": str(self.expected_exchange_order_id),
            "clientId": 12345,
            "createdAt": 1640995200000,
            "executedQuantity": "0",
            "executedQuoteQuantity": "0",
            "quantity": "100",
            "quoteQuantity": "1000000",  # 100 * 10000
            "reduceOnly": False,
            "timeInForce": "GTC",
            "selfTradePrevention": "RejectTaker",
            "side": "Bid",
            "status": "New",  # Changed from "Cancelled" to "New" for new order
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            # Optional fields for stop/take profit orders
            "stopLossTriggerPrice": None,
            "stopLossLimitPrice": None,
            "stopLossTriggerBy": None,
            "takeProfitTriggerPrice": None,
            "takeProfitLimitPrice": None,
            "takeProfitTriggerBy": None,
            "triggerBy": None,
            "triggerPrice": None,
            "triggerQuantity": None,
            "triggeredAt": None,
            "relatedOrderId": None,
            "strategyId": None
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            self.base_asset: {
                "available": "10.0",
                "locked": "5.0"
            },
            self.quote_asset: {
                "available": "2000.0",
                "locked": "0.0"
            }
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            self.base_asset: {
                "available": "10.0",
                "locked": "5.0"
            }
        }

    @property
    def balance_event_websocket_update(self):
        return {
            "stream": "account.positionUpdate",
            "data": {
                "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "balance": "15.0",
                "available": "10.0",
                "locked": "5.0",
                "timestamp": "1640995200000"
            }
        }

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        filters = self.trading_rules_request_mock_response[0]["filters"]
        # Note: maxQuantity is None in Backpack, so we use a very large default value
        max_order_size = Decimal("1000000")  # This matches what our implementation sets for None
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(filters["quantity"]["minQuantity"]),
            max_order_size=max_order_size,
            min_price_increment=Decimal(filters["price"]["tickSize"]),
            min_base_amount_increment=Decimal(filters["quantity"]["stepSize"]),
            min_notional_size=Decimal("1.0"),  # Backpack doesn't provide this, use default
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response[0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return "28"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal(10500)

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return DeductedFromReturnsTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("30"))])

    @property
    def expected_fill_trade_id(self) -> str:
        return str(30000)

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}_{quote_token}"

    def create_exchange_instance(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())

        # Generate valid Ed25519 keys for testing
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()

        # Serialize keys in the format expected by BackpackAuth
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )

        api_key = base64.b64encode(public_key_bytes).decode('utf-8')
        secret_key = base64.b64encode(private_key_bytes).decode('utf-8')

        return BackpackExchange(
            client_config_map=client_config_map,
            backpack_api_key=api_key,
            backpack_secret_key=secret_key,
            trading_pairs=[self.trading_pair],
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs["headers"]
        self.assertIn("X-API-Key", request_headers)
        self.assertIn("X-Timestamp", request_headers)
        self.assertIn("X-Signature", request_headers)
        self.assertIn("X-Window", request_headers)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        import json
        import hashlib
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["symbol"])
        self.assertEqual("Bid" if order.trade_type == TradeType.BUY else "Ask", request_data["side"])
        self.assertEqual("Limit" if order.order_type == OrderType.LIMIT else "Market", request_data["orderType"])
        # Convert to Decimal for consistent comparison (handles 100 vs 100.000000)
        from decimal import Decimal
        self.assertEqual(Decimal(str(order.amount)), Decimal(request_data["quantity"]))
        if order.order_type == OrderType.LIMIT:
            self.assertEqual(Decimal(str(order.price)), Decimal(request_data["price"]))
        # Backpack uses integer clientId generated from hash of order.client_order_id
        expected_client_id = int(hashlib.md5(order.client_order_id.encode()).hexdigest()[:8], 16) % (2**32 - 1)
        self.assertEqual(expected_client_id, request_data["clientId"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        import json
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["symbol"])
        self.assertEqual(order.exchange_order_id, request_data["orderId"])  # Backpack uses orderId, not clientId

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])
        self.assertEqual(order.exchange_order_id, request_params["orderId"])  # Backpack uses orderId, not clientId

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_params["symbol"])

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.delete(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.delete(regex_url, status=400, callback=callback)
        return url

    def configure_order_not_found_error_cancelation_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = {"error": "Order not found"}
        mock_api.delete(regex_url, status=404, body=json.dumps(response), callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self,
            successful_order: InFlightOrder,
            erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        """
        :return: a list of all configured URLs for the cancelations
        """
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_completely_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.FILLS_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_open_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        """
        :return: the URL configured
        """
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=401, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_order_not_found_error_order_status_response(
        self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> List[str]:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = {"error": "Order not found"}
        mock_api.get(regex_url, body=json.dumps(response), status=404, callback=callback)
        return [url]

    def configure_partial_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.FILLS_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_partial_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.FILLS_PATH_URL)
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_full_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "stream": "account.orderUpdate",
            "data": {
                "E": 1754462606817679,           # Event time in microseconds (real format)
                "O": "USER",                     # Origin of the update
                "S": "Bid" if order.trade_type == TradeType.BUY else "Ask",  # Side
                "T": 1754462606815843,           # Engine timestamp in microseconds
                "V": "RejectTaker",              # Self trade prevention
                "X": "New",                      # Order state (real format)
                "Z": "0",                        # Cumulative filled quantity
                "c": int(hashlib.md5(order.client_order_id.encode()).hexdigest()[:8], 16) % (2**32 - 1),  # Client order ID (32-bit integer)
                "e": "orderAccepted",            # Event type (real format)
                "f": "GTC",                      # Time in force
                "i": str(order.exchange_order_id),  # Exchange order ID (real format)
                "o": "LIMIT" if order.order_type == OrderType.LIMIT else "MARKET",  # Order type
                "p": str(order.price),           # Price
                "q": str(order.amount),          # Quantity
                "r": False,                      # Reduce only flag
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),  # Symbol (real format)
                "t": None,                       # Trade ID (null for new orders)
                "z": "0"                         # Last filled quantity
            }
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "stream": "account.orderUpdate",
            "data": {
                "E": 1754462606817679,           # Event time in microseconds (real format)
                "O": "USER",                     # Origin of the update
                "S": "Bid" if order.trade_type == TradeType.BUY else "Ask",  # Side
                "T": 1754462606815843,           # Engine timestamp in microseconds
                "V": "RejectTaker",              # Self trade prevention
                "X": "Cancelled",                # Order state (real format)
                "Z": "0",                        # Cumulative filled quantity
                "c": int(hashlib.md5(order.client_order_id.encode()).hexdigest()[:8], 16) % (2**32 - 1),  # Client order ID (32-bit integer)
                "e": "orderCancelled",           # Event type (real format)
                "f": "GTC",                      # Time in force
                "i": str(order.exchange_order_id),  # Exchange order ID (real format)
                "o": "LIMIT" if order.order_type == OrderType.LIMIT else "MARKET",  # Order type
                "p": str(order.price),           # Price
                "q": str(order.amount),          # Quantity
                "r": False,                      # Reduce only flag
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),  # Symbol (real format)
                "t": None,                       # Trade ID (null for cancelled orders)
                "z": "0"                         # Last filled quantity
            }
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "stream": "account.orderUpdate",
            "data": {
                "E": 1754462606817679,           # Event time in microseconds (real format)
                "O": "USER",                     # Origin of the update
                "S": "Bid" if order.trade_type == TradeType.BUY else "Ask",  # Side
                "T": 1754462606815843,           # Engine timestamp in microseconds
                "V": "RejectTaker",              # Self trade prevention
                "X": "Filled",                   # Order state (real format)
                "Z": str(order.amount),          # Cumulative filled quantity
                "c": int(hashlib.md5(order.client_order_id.encode()).hexdigest()[:8], 16) % (2**32 - 1),  # Client order ID (32-bit integer)
                "e": "orderFilled",              # Event type (real format)
                "f": "GTC",                      # Time in force
                "i": str(order.exchange_order_id),  # Exchange order ID (real format)
                "o": "LIMIT" if order.order_type == OrderType.LIMIT else "MARKET",  # Order type
                "p": str(order.price),           # Price
                "q": str(order.amount),          # Quantity
                "r": False,                      # Reduce only flag
                "s": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),  # Symbol (real format)
                "t": 30000,                      # Trade ID (when trade occurs)
                "z": str(order.amount),          # Last filled quantity
                "n": "30",                       # Fee amount (from real format)
                "N": "USDC"                      # Fee symbol (from real format)
            }
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return None  # Backpack includes trade info in order update

    # Helper methods for mock responses
    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "orderId": order.exchange_order_id,
            "clientOrderId": order.client_order_id,
            "status": "Cancelled",
            "timestamp": "1640995200000"
        }

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "orderType": "Limit" if order.order_type == OrderType.LIMIT else "Market",
            "id": order.exchange_order_id,
            "clientId": 12345,
            "createdAt": 1640995200000,
            "executedQuantity": str(order.amount),  # Fully filled
            "executedQuoteQuantity": str(order.amount * order.price),
            "quantity": str(order.amount),
            "quoteQuantity": str(order.amount * order.price),
            "reduceOnly": False,
            "timeInForce": "GTC",
            "selfTradePrevention": "RejectTaker",
            "side": "Bid" if order.trade_type == TradeType.BUY else "Ask",
            "status": "Filled",  # This is the key field for determining state
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            # Optional fields
            "stopLossTriggerPrice": None,
            "stopLossLimitPrice": None,
            "stopLossTriggerBy": None,
            "takeProfitTriggerPrice": None,
            "takeProfitLimitPrice": None,
            "takeProfitTriggerBy": None,
            "triggerBy": None,
            "triggerPrice": None,
            "triggerQuantity": None,
            "triggeredAt": None,
            "relatedOrderId": None,
            "strategyId": None
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "orderType": "Limit" if order.order_type == OrderType.LIMIT else "Market",
            "id": order.exchange_order_id,  # Backpack uses "id", not "orderId" in response
            "clientId": 12345,  # Backpack uses integer clientId
            "createdAt": 1640995200000,  # Backpack uses "createdAt", not "timestamp"
            "executedQuantity": str(order.amount),  # Assuming fully cancelled
            "executedQuoteQuantity": str(order.amount * order.price),
            "quantity": str(order.amount),
            "quoteQuantity": str(order.amount * order.price),
            "reduceOnly": False,
            "timeInForce": "GTC",
            "selfTradePrevention": "RejectTaker",
            "side": "Bid" if order.trade_type == TradeType.BUY else "Ask",
            "status": "Cancelled",  # This is the key field for determining state
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            # Optional fields
            "stopLossTriggerPrice": None,
            "stopLossLimitPrice": None,
            "stopLossTriggerBy": None,
            "takeProfitTriggerPrice": None,
            "takeProfitLimitPrice": None,
            "takeProfitTriggerBy": None,
            "triggerBy": None,
            "triggerPrice": None,
            "triggerQuantity": None,
            "triggeredAt": None,
            "relatedOrderId": None,
            "strategyId": None
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "orderType": "Limit" if order.order_type == OrderType.LIMIT else "Market",
            "id": order.exchange_order_id,
            "clientId": 12345,
            "createdAt": 1640995200000,
            "executedQuantity": "0",  # Not filled yet
            "executedQuoteQuantity": "0",
            "quantity": str(order.amount),
            "quoteQuantity": str(order.amount * order.price),
            "reduceOnly": False,
            "timeInForce": "GTC",
            "selfTradePrevention": "RejectTaker",
            "side": "Bid" if order.trade_type == TradeType.BUY else "Ask",
            "status": "New",  # This is the key field for determining state
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            # Optional fields
            "stopLossTriggerPrice": None,
            "stopLossLimitPrice": None,
            "stopLossTriggerBy": None,
            "takeProfitTriggerPrice": None,
            "takeProfitLimitPrice": None,
            "takeProfitTriggerBy": None,
            "triggerBy": None,
            "triggerPrice": None,
            "triggerQuantity": None,
            "triggeredAt": None,
            "relatedOrderId": None,
            "strategyId": None
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "orderType": "Limit" if order.order_type == OrderType.LIMIT else "Market",
            "id": order.exchange_order_id,
            "clientId": 12345,
            "createdAt": 1640995200000,
            "executedQuantity": str(self.expected_partial_fill_amount),  # Partially filled
            "executedQuoteQuantity": str(self.expected_partial_fill_amount * self.expected_partial_fill_price),
            "quantity": str(order.amount),
            "quoteQuantity": str(order.amount * order.price),
            "reduceOnly": False,
            "timeInForce": "GTC",
            "selfTradePrevention": "RejectTaker",
            "side": "Bid" if order.trade_type == TradeType.BUY else "Ask",
            "status": "PartiallyFilled",  # This is the key field for determining state
            "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            # Optional fields
            "stopLossTriggerPrice": None,
            "stopLossLimitPrice": None,
            "stopLossTriggerBy": None,
            "takeProfitTriggerPrice": None,
            "takeProfitLimitPrice": None,
            "takeProfitTriggerBy": None,
            "triggerBy": None,
            "triggerPrice": None,
            "triggerQuantity": None,
            "triggeredAt": None,
            "relatedOrderId": None,
            "strategyId": None
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder):
        return [
            {
                "id": self.expected_fill_trade_id,
                "symbol": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                "orderId": order.exchange_order_id,
                "price": str(self.expected_partial_fill_price),
                "quantity": str(self.expected_partial_fill_amount),
                "side": "Bid" if order.trade_type == TradeType.BUY else "Ask",
                "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "feeSymbol": self.expected_fill_fee.flat_fees[0].token,
                "timestamp": "1640995200000"
            }
        ]

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder):
        return [
            {
                "id": self.expected_fill_trade_id,
                "symbol": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                "orderId": order.exchange_order_id,
                "price": str(order.price),
                "quantity": str(order.amount),
                "side": "Bid" if order.trade_type == TradeType.BUY else "Ask",
                "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "feeSymbol": self.expected_fill_fee.flat_fees[0].token,
                "timestamp": "1640995200000"
            }
        ]

    @aioresponses()
    def test_lost_order_included_in_order_fills_update_and_not_in_order_status_update(self, mock_api):
        """Override parent test with debug prints to identify hanging point"""
        print("DEBUG: Test starting...")
        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()
        print("DEBUG: Created request event...")

        print("DEBUG: Starting order tracking...")
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]
        print(f"DEBUG: Order tracked: {order.client_order_id}")

        print("DEBUG: Processing order not found events...")
        for i in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            print(f"DEBUG: Processing not found {i+1}/{self.exchange._order_tracker._lost_order_count_limit + 1}")
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id))

        print("DEBUG: Checking order removal from in_flight_orders...")
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        print("DEBUG: Order successfully removed from tracking")

        print("DEBUG: Configuring completely filled order status response...")
        self.configure_completely_filled_order_status_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())
        print("DEBUG: Completely filled response configured")

        print(f"DEBUG: is_order_fill_http_update_included_in_status_update = {self.is_order_fill_http_update_included_in_status_update}")
        if self.is_order_fill_http_update_included_in_status_update:
            print("DEBUG: Configuring full fill trade response...")
            trade_url = self.configure_full_fill_trade_response(
                order=order,
                mock_api=mock_api,
                callback=lambda *args, **kwargs: request_sent_event.set())
            print(f"DEBUG: Trade URL configured: {trade_url}")
        else:
            print("DEBUG: Setting completely filled event manually...")
            # If the fill events will not be requested with the order status, we need to manually set the event
            # to allow the ClientOrderTracker to process the last status update
            order.completely_filled_event.set()
            request_sent_event.set()
            print("DEBUG: Events set manually")

        print("DEBUG: Calling _update_order_status() using async_run_with_timeout...")
        self.async_run_with_timeout(self.exchange._update_order_status())
        print("DEBUG: _update_order_status() completed")

        print("DEBUG: Waiting for request_sent_event using async_run_with_timeout...")
        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(request_sent_event.wait())
        print("DEBUG: request_sent_event received")

        print("DEBUG: Waiting for order to be completely filled...")
        self.async_run_with_timeout(order.wait_until_completely_filled())
        print("DEBUG: Order completely filled")

        print(f"DEBUG: order.is_done = {order.is_done}, order.is_failure = {order.is_failure}")
        self.assertTrue(order.is_done)
        self.assertTrue(order.is_failure)
        print("DEBUG: Order state assertions passed")

        if self.is_order_fill_http_update_included_in_status_update:
            print("DEBUG: Checking trade requests...")
            if trade_url:
                trades_request = self._all_executed_requests(mock_api, trade_url)[0]
                self.validate_auth_credentials_present(trades_request)
                self.validate_trades_request(
                    order=order,
                    request_call=trades_request)
                print("DEBUG: Trade request validation passed")

            fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
            print(f"DEBUG: Got fill event: {fill_event}")
            self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
            self.assertEqual(order.client_order_id, fill_event.order_id)
            self.assertEqual(order.trading_pair, fill_event.trading_pair)
            self.assertEqual(order.trade_type, fill_event.trade_type)
            self.assertEqual(order.order_type, fill_event.order_type)
            self.assertEqual(order.price, fill_event.price)
            self.assertEqual(order.amount, fill_event.amount)
            self.assertEqual(self.expected_fill_fee, fill_event.trade_fee)
            print("DEBUG: Fill event assertions passed")

        print("DEBUG: Test completed successfully!")

    @aioresponses()
    def test_lost_order_user_stream_full_fill_events_are_processed(self, mock_api):
        """Override parent test with debug prints to identify hanging point"""
        print("DEBUG: User stream test starting...")
        self.exchange._set_current_timestamp(1640780000)

        print("DEBUG: Starting order tracking...")
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]
        print(f"DEBUG: Order tracked: {order.client_order_id}")

        print("DEBUG: Processing order not found events...")
        for i in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            print(f"DEBUG: Processing not found {i+1}/{self.exchange._order_tracker._lost_order_count_limit + 1}")
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id))

        print("DEBUG: Checking order removal from in_flight_orders...")
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        print("DEBUG: Order successfully removed from tracking")

        print("DEBUG: Creating WebSocket events...")
        order_event = self.order_event_for_full_fill_websocket_update(order=order)
        trade_event = self.trade_event_for_full_fill_websocket_update(order=order)
        print(f"DEBUG: Order event: {order_event}")
        print(f"DEBUG: Trade event: {trade_event}")

        print("DEBUG: Setting up mock user stream...")
        mock_queue = AsyncMock()
        event_messages = []
        if trade_event:
            event_messages.append(trade_event)
            print("DEBUG: Added trade event to messages")
        if order_event:
            event_messages.append(order_event)
            print("DEBUG: Added order event to messages")
        event_messages.append(asyncio.CancelledError)
        print(f"DEBUG: Total messages: {len(event_messages)}")
        mock_queue.get.side_effect = event_messages
        self.exchange._user_stream_tracker._user_stream = mock_queue
        print("DEBUG: Mock user stream configured")

        print(f"DEBUG: is_order_fill_http_update_executed_during_websocket_order_event_processing = {self.is_order_fill_http_update_executed_during_websocket_order_event_processing}")
        if self.is_order_fill_http_update_executed_during_websocket_order_event_processing:
            print("DEBUG: Configuring full fill trade response...")
            self.configure_full_fill_trade_response(
                order=order,
                mock_api=mock_api)
            print("DEBUG: Trade response configured")

        print("DEBUG: Starting user stream event listener...")
        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
            print("DEBUG: User stream event listener completed normally")
        except asyncio.CancelledError:
            print("DEBUG: User stream event listener cancelled (expected)")
            pass

        print("DEBUG: Waiting for order to be completely filled...")
        self.async_run_with_timeout(order.wait_until_completely_filled())
        print("DEBUG: Order completely filled")

        self.async_run_with_timeout(asyncio.sleep(0.1))
        print("DEBUG: Sleep completed")

        print("DEBUG: Checking fill event...")
        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        print(f"DEBUG: Got fill event: {fill_event}")

        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(order.price, fill_event.price)
        self.assertEqual(order.amount, fill_event.amount)
        expected_fee = self.expected_fill_fee
        self.assertEqual(expected_fee, fill_event.trade_fee)
        print("DEBUG: Fill event assertions passed")

        print("DEBUG: Checking final order state...")
        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_failure)
        print(f"DEBUG: Final order state - is_filled: {order.is_filled}, is_failure: {order.is_failure}")

        print("DEBUG: User stream test completed successfully!")
