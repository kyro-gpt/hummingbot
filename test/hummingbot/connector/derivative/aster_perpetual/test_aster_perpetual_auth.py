import asyncio
import json
import unittest
from typing import Awaitable
from unittest.mock import patch

from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_auth import AsterPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class AsterPerpetualAuthUnitTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()

        # Test credentials from Aster example
        cls.user_wallet = '0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e'  # noqa: mock
        cls.signer_wallet = '0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0'  # noqa: mock
        cls.private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"  # noqa: mock

        cls.auth = AsterPerpetualAuth(
            user_wallet=cls.user_wallet,
            signer_wallet=cls.signer_wallet,
            private_key=cls.private_key
        )

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_auth_initialization(self):
        """Test authentication object initialization"""
        self.assertEqual(self.auth._user_wallet, self.user_wallet)
        self.assertEqual(self.auth._signer_wallet, self.signer_wallet)
        self.assertEqual(self.auth._private_key, self.private_key)

    def test_private_key_with_without_prefix(self):
        """Test private key handling with and without 0x prefix"""
        # With 0x prefix
        auth_with_prefix = AsterPerpetualAuth(
            user_wallet=self.user_wallet,
            signer_wallet=self.signer_wallet,
            private_key="0x123abc"
        )
        self.assertEqual(auth_with_prefix._private_key, "0x123abc")  # noqa: mock

        # Without 0x prefix
        auth_without_prefix = AsterPerpetualAuth(
            user_wallet=self.user_wallet,
            signer_wallet=self.signer_wallet,
            private_key="123abc"
        )
        self.assertEqual(auth_without_prefix._private_key, "0x123abc")  # noqa: mock

    def test_trim_dict_simple_values(self):
        """Test parameter trimming with simple values"""
        params = {
            'symbol': 'BTCUSDT',
            'quantity': 100,
            'price': 50000.5,
            'side': 'BUY'
        }

        result = self.auth._trim_dict(params)

        expected = {
            'symbol': 'BTCUSDT',
            'quantity': '100',
            'price': '50000.5',
            'side': 'BUY'
        }

        self.assertEqual(result, expected)

    def test_trim_dict_with_nested_structures(self):
        """Test parameter trimming with nested lists and dictionaries"""
        params = {
            'simple': 'value',
            'nested_dict': {'inner_key': 'inner_value', 'number': 42},
            'nested_list': ['item1', 2, {'list_dict': 'value'}]
        }

        result = self.auth._trim_dict(params)

        # Verify all values are strings
        self.assertEqual(result['simple'], 'value')
        self.assertIsInstance(result['nested_dict'], str)
        self.assertIsInstance(result['nested_list'], str)

        # Verify nested structures are JSON strings
        nested_dict = json.loads(result['nested_dict'])
        self.assertEqual(nested_dict['inner_key'], 'inner_value')
        self.assertEqual(nested_dict['number'], '42')

    @patch('time.time')
    def test_generate_nonce_consistency(self, mock_time):
        """Test nonce generation consistency"""
        mock_time.return_value = 1648776523.123456

        nonce1 = self.auth._generate_nonce()
        nonce2 = self.auth._generate_nonce()

        # Should be microsecond precision
        expected_nonce = 1648776523123456
        self.assertEqual(nonce1, expected_nonce)
        self.assertEqual(nonce2, expected_nonce)

    def test_create_signature_payload(self):
        """Test signature payload creation"""
        params = {
            'symbol': 'SANDUSDT',
            'side': 'BUY',
            'type': 'LIMIT',
            'quantity': '30'
        }
        nonce = 1748310859508867

        payload_hash = self.auth._create_signature_payload(params, nonce)

        # Should return a hex hash string
        self.assertIsInstance(payload_hash, str)
        self.assertTrue(payload_hash.startswith('0x'))
        self.assertEqual(len(payload_hash), 66)  # 0x + 64 hex chars

    def test_generate_signature(self):
        """Test signature generation"""
        test_hash = '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'  # noqa: mock

        signature = self.auth._generate_signature(test_hash)

        # Should return a hex signature string
        self.assertIsInstance(signature, str)
        self.assertTrue(signature.startswith('0x'))
        self.assertEqual(len(signature), 132)  # 0x + 130 hex chars (65 bytes * 2)

    @patch('time.time')
    def test_add_auth_to_params(self, mock_time):
        """Test adding authentication parameters"""
        mock_time.return_value = 1648776523.123

        params = {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': '10',
            'price': '50000',
            'null_value': None  # Should be removed
        }

        with patch.object(self.auth, '_generate_nonce', return_value=1748310859508867):
            result = self.auth.add_auth_to_params(params)

        # Check that None values are removed
        self.assertNotIn('null_value', result)

        # Check that timing parameters are added
        self.assertIn('recvWindow', result)
        self.assertIn('timestamp', result)
        self.assertEqual(result['recvWindow'], 50000)
        self.assertEqual(result['timestamp'], 1648776523123)

        # Check that Web3 auth parameters are added
        self.assertIn('nonce', result)
        self.assertIn('user', result)
        self.assertIn('signer', result)
        self.assertIn('signature', result)

        self.assertEqual(result['nonce'], 1748310859508867)
        self.assertEqual(result['user'], self.user_wallet)
        self.assertEqual(result['signer'], self.signer_wallet)
        self.assertTrue(result['signature'].startswith('0x'))

    def test_rest_authenticate_get_request(self):
        """Test REST authentication for GET requests"""
        request = RESTRequest(
            method=RESTMethod.GET,
            url="/test",
            params={'symbol': 'BTCUSDT', 'limit': 100}
        )

        with patch.object(self.auth, 'add_auth_to_params') as mock_auth:
            mock_auth.return_value = {'symbol': 'BTCUSDT', 'limit': 100, 'signature': '0x123'}

            result = self.async_run_with_timeout(self.auth.rest_authenticate(request))

            self.assertEqual(result.params['signature'], '0x123')
            mock_auth.assert_called_once()

    def test_rest_authenticate_post_request_with_json_data(self):
        """Test REST authentication for POST requests with JSON data"""
        request = RESTRequest(
            method=RESTMethod.POST,
            url="/test",
            data='{"symbol": "BTCUSDT", "quantity": "10"}'
        )

        with patch.object(self.auth, 'add_auth_to_params') as mock_auth:
            mock_auth.return_value = {'symbol': 'BTCUSDT', 'quantity': '10', 'signature': '0x123'}

            result = self.async_run_with_timeout(self.auth.rest_authenticate(request))

            self.assertEqual(result.data['signature'], '0x123')
            mock_auth.assert_called_once()

    def test_rest_authenticate_post_request_with_dict_data(self):
        """Test REST authentication for POST requests with dict data"""
        request = RESTRequest(
            method=RESTMethod.POST,
            url="/test",
            data={'symbol': 'BTCUSDT', 'quantity': '10'}
        )

        with patch.object(self.auth, 'add_auth_to_params') as mock_auth:
            mock_auth.return_value = {'symbol': 'BTCUSDT', 'quantity': '10', 'signature': '0x123'}

            result = self.async_run_with_timeout(self.auth.rest_authenticate(request))

            self.assertEqual(result.data['signature'], '0x123')
            mock_auth.assert_called_once()

    def test_rest_authenticate_post_request_empty_data(self):
        """Test REST authentication for POST requests with no data"""
        request = RESTRequest(
            method=RESTMethod.POST,
            url="/test"
        )

        with patch.object(self.auth, 'add_auth_to_params') as mock_auth:
            mock_auth.return_value = {'signature': '0x123'}

            result = self.async_run_with_timeout(self.auth.rest_authenticate(request))

            self.assertEqual(result.data['signature'], '0x123')
            mock_auth.assert_called_once_with({})


if __name__ == "__main__":
    unittest.main()
