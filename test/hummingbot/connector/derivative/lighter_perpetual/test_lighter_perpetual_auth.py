import asyncio
import json
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock, patch

from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_auth import LighterPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class LighterPerpetualAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.private_key = "0x13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930"  # noqa: mock
        self.account_index = 65
        self.api_key_index = 1
        self.auth = LighterPerpetualAuth(
            private_key=self.private_key,
            account_index=self.account_index,
            api_key_index=self.api_key_index
        )

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_init_with_valid_private_key(self):
        """Test initialization with valid private key"""
        self.assertEqual(self.auth.account_index, self.account_index)
        self.assertEqual(self.auth.api_key_index, self.api_key_index)
        self.assertTrue(self.auth.private_key.startswith('0x'))
        self.assertIsNotNone(self.auth.account)
        self.assertIsNotNone(self.auth.address)

    def test_init_with_private_key_without_0x_prefix(self):
        """Test initialization with private key without 0x prefix"""
        private_key_no_prefix = "13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930"
        auth = LighterPerpetualAuth(
            private_key=private_key_no_prefix,
            account_index=self.account_index,
            api_key_index=self.api_key_index
        )
        self.assertTrue(auth.private_key.startswith('0x'))

    def test_init_with_invalid_private_key(self):
        """Test initialization with invalid private key raises error"""
        with self.assertRaises(ValueError):
            LighterPerpetualAuth(
                private_key="invalid_key",
                account_index=self.account_index,
                api_key_index=self.api_key_index
            )

    def test_rest_authenticate_non_sendtx_request(self):
        """Test REST authentication for non-sendTx requests"""
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://test.url/account",
            is_auth_required=True,
        )
        
        authenticated_request = self.async_run_with_timeout(self.auth.rest_authenticate(request))
        
        self.assertIn("Content-Type", authenticated_request.headers)
        self.assertEqual(authenticated_request.headers["Content-Type"], "application/json")
        self.assertIn("Accept", authenticated_request.headers)
        self.assertEqual(authenticated_request.headers["Accept"], "application/json")

    @patch('time.time')
    def test_create_order_tx_info(self, mock_time):
        """Test create order transaction info generation"""
        mock_time.return_value = 1678974447.926
        
        tx_info = self.auth.create_order_tx_info(
            market_index=0,
            client_order_index=123,
            base_amount=100000,
            price=405000,
            is_ask=True,
            order_type=0,
        )
        
        self.assertEqual(tx_info["market_index"], 0)
        self.assertEqual(tx_info["client_order_index"], 123)
        self.assertEqual(tx_info["base_amount"], 100000)
        self.assertEqual(tx_info["price"], 405000)
        self.assertEqual(tx_info["is_ask"], 1)
        self.assertEqual(tx_info["order_type"], 0)
        self.assertIn("nonce", tx_info)

    @patch('time.time')
    def test_create_cancel_order_tx_info(self, mock_time):
        """Test create cancel order transaction info generation"""
        mock_time.return_value = 1678974447.926
        
        tx_info = self.auth.create_cancel_order_tx_info(
            market_index=0,
            order_index=123,
        )
        
        self.assertEqual(tx_info["market_index"], 0)
        self.assertEqual(tx_info["order_index"], 123)
        self.assertIn("nonce", tx_info)

    @patch('time.time')
    def test_create_auth_token(self, mock_time):
        """Test authentication token creation"""
        mock_time.return_value = 1678974447.926
        
        auth_token, error = self.auth.create_auth_token(expiry_minutes=10)
        
        self.assertIsNone(error)
        self.assertIsNotNone(auth_token)
        
        # Parse the token to verify structure
        token_data = json.loads(auth_token)
        self.assertEqual(token_data["account_index"], self.account_index)
        self.assertEqual(token_data["api_key_index"], self.api_key_index)
        self.assertIn("deadline", token_data)
        self.assertIn("signature", token_data)

    def test_ws_authenticate_passthrough(self):
        """Test WebSocket authentication passes through unchanged"""
        from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
        
        ws_request = WSJSONRequest(
            payload={"test": "data"},
            is_auth_required=True
        )
        authenticated_request = self.async_run_with_timeout(self.auth.ws_authenticate(ws_request))
        
        self.assertEqual(ws_request, authenticated_request)
