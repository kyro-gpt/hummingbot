import asyncio
import base64
from unittest import TestCase
from unittest.mock import MagicMock

from typing_extensions import Awaitable

from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class BackpackPerpetualAuthTests(TestCase):

    def setUp(self) -> None:
        # Generate test Ed25519 key pair
        from cryptography.hazmat.primitives.asymmetric import ed25519

        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()

        self._api_key = base64.b64encode(public_key.public_bytes_raw()).decode()
        self._secret = base64.b64encode(private_key.private_bytes_raw()).decode()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_rest_authenticate(self):
        now = 1234567890.000
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = now

        params = {
            "symbol": "BTC_USDC",  # Typical derivative pair
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "0.1",
            "price": "50000.0",
        }

        auth = BackpackPerpetualAuth(api_key=self._api_key, secret_key=self._secret, time_provider=mock_time_provider)
        # Create request with proper URL for derivative order creation
        from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
        request = RESTRequest(
            method=RESTMethod.POST,
            url=CONSTANTS.REST_URL + CONSTANTS.ORDER_PATH_URL,
            params=params,
            is_auth_required=True
        )
        configured_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        # Check that headers exist and contain required keys
        self.assertIsNotNone(configured_request.headers)
        self.assertIn("X-API-Key", configured_request.headers)
        self.assertIn("X-Timestamp", configured_request.headers)
        self.assertIn("X-Signature", configured_request.headers)
        self.assertIn("X-Window", configured_request.headers)

        # Check values
        self.assertEqual(self._api_key, configured_request.headers["X-API-Key"])
        self.assertEqual(str(int(now * 1000)), configured_request.headers["X-Timestamp"])
        self.assertEqual("5000", configured_request.headers["X-Window"])

        # Signature should be non-empty
        self.assertTrue(len(configured_request.headers["X-Signature"]) > 0)

    def test_websocket_login_parameters(self):
        now = 1234567890.000
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = now

        auth = BackpackPerpetualAuth(api_key=self._api_key, secret_key=self._secret, time_provider=mock_time_provider)
        login_params = auth.websocket_login_parameters()

        # Check structure
        self.assertEqual("SUBSCRIBE", login_params["method"])
        self.assertEqual([], login_params["params"])
        self.assertIn("signature", login_params)

        # Signature should be a list with 4 elements
        signature = login_params["signature"]
        self.assertIsInstance(signature, list)
        self.assertEqual(4, len(signature))
        self.assertEqual(self._api_key, signature[0])  # API key
        self.assertTrue(len(signature[1]) > 0)  # Signature
        self.assertEqual(str(int(now * 1000)), signature[2])  # Timestamp
        self.assertEqual("5000", signature[3])  # Window

    def test_derivative_position_request_authentication(self):
        """Test authentication for derivative-specific position endpoint"""
        now = 1234567890.000
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = now

        auth = BackpackPerpetualAuth(api_key=self._api_key, secret_key=self._secret, time_provider=mock_time_provider)

        # Test position query request
        from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
        request = RESTRequest(
            method=RESTMethod.GET,
            url=CONSTANTS.REST_URL + CONSTANTS.POSITION_PATH_URL,
            params={},
            is_auth_required=True
        )
        configured_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        # Should be authenticated (position query is a private endpoint)
        self.assertIsNotNone(configured_request.headers)
        self.assertIn("X-API-Key", configured_request.headers)
        self.assertIn("X-Signature", configured_request.headers)

    def test_funding_history_request_authentication(self):
        """Test authentication for derivative-specific funding history endpoint"""
        now = 1234567890.000
        mock_time_provider = MagicMock()
        mock_time_provider.time.return_value = now

        auth = BackpackPerpetualAuth(api_key=self._api_key, secret_key=self._secret, time_provider=mock_time_provider)

        # Test funding history request
        from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
        request = RESTRequest(
            method=RESTMethod.GET,
            url=CONSTANTS.REST_URL + CONSTANTS.WAPI_VERSION + CONSTANTS.HISTORY_FUNDING_PATH_URL,
            params={"symbol": "BTC_USDC"},
            is_auth_required=True
        )
        configured_request = self.async_run_with_timeout(auth.rest_authenticate(request))

        # Should be authenticated (funding history is a private endpoint)
        self.assertIsNotNone(configured_request.headers)
        self.assertIn("X-API-Key", configured_request.headers)
        self.assertIn("X-Signature", configured_request.headers)
