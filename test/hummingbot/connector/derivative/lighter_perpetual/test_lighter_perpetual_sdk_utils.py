import json
from unittest import TestCase
from unittest.mock import patch

from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_sdk_utils import (
    CreateOrder,
    CancelOrder,
    NonceManager,
    LighterSDKHelper,
    convert_amount_to_raw_units,
    convert_raw_units_to_amount,
    validate_private_key,
)


class LighterPerpetualSDKUtilsTests(TestCase):
    
    def test_create_order_to_json(self):
        """Test CreateOrder serialization to JSON"""
        order = CreateOrder(
            account_index=65,
            order_book_index=0,
            base_amount=100000,
            price=405000,
            is_ask=1,
            order_type=0,
            nonce=1678974447926,
        )
        
        json_str = order.to_json()
        data = json.loads(json_str)
        
        self.assertEqual(data["AccountIndex"], 65)
        self.assertEqual(data["OrderBookIndex"], 0)
        self.assertEqual(data["BaseAmount"], 100000)
        self.assertEqual(data["Price"], 405000)
        self.assertEqual(data["IsAsk"], 1)
        self.assertEqual(data["OrderType"], 0)
        self.assertEqual(data["Nonce"], 1678974447926)

    def test_create_order_from_json(self):
        """Test CreateOrder deserialization from JSON"""
        json_str = '{"AccountIndex": 65, "OrderBookIndex": 0, "BaseAmount": 100000, "Price": 405000, "IsAsk": 1, "OrderType": 0, "Nonce": 1678974447926}'
        
        order = CreateOrder.from_json(json_str)
        
        self.assertEqual(order.account_index, 65)
        self.assertEqual(order.order_book_index, 0)
        self.assertEqual(order.base_amount, 100000)
        self.assertEqual(order.price, 405000)
        self.assertEqual(order.is_ask, 1)
        self.assertEqual(order.order_type, 0)
        self.assertEqual(order.nonce, 1678974447926)

    def test_cancel_order_to_json(self):
        """Test CancelOrder serialization to JSON"""
        cancel = CancelOrder(
            account_index=65,
            order_book_index=0,
            order_index=123,
            nonce=1678974447926,
        )
        
        json_str = cancel.to_json()
        data = json.loads(json_str)
        
        self.assertEqual(data["AccountIndex"], 65)
        self.assertEqual(data["OrderBookIndex"], 0)
        self.assertEqual(data["OrderIndex"], 123)
        self.assertEqual(data["Nonce"], 1678974447926)

    def test_cancel_order_from_json(self):
        """Test CancelOrder deserialization from JSON"""
        json_str = '{"AccountIndex": 65, "OrderBookIndex": 0, "OrderIndex": 123, "Nonce": 1678974447926}'
        
        cancel = CancelOrder.from_json(json_str)
        
        self.assertEqual(cancel.account_index, 65)
        self.assertEqual(cancel.order_book_index, 0)
        self.assertEqual(cancel.order_index, 123)
        self.assertEqual(cancel.nonce, 1678974447926)

    def test_nonce_manager_initialization(self):
        """Test NonceManager initialization"""
        manager = NonceManager(account_index=65, start_api_key=1, end_api_key=3)
        
        self.assertEqual(manager.account_index, 65)
        self.assertEqual(manager.start_api_key, 1)
        self.assertEqual(manager.end_api_key, 3)
        self.assertEqual(manager.current_api_key, 3)
        
        # Should have nonces for all API keys
        for i in range(1, 4):
            self.assertIn(i, manager.nonce)

    def test_nonce_manager_invalid_range(self):
        """Test NonceManager with invalid API key range"""
        with self.assertRaises(ValueError):
            NonceManager(account_index=65, start_api_key=5, end_api_key=3)  # start > end
        
        with self.assertRaises(ValueError):
            NonceManager(account_index=65, start_api_key=255, end_api_key=255)  # >= 255

    def test_nonce_manager_next_nonce(self):
        """Test NonceManager nonce generation"""
        manager = NonceManager(account_index=65, start_api_key=1, end_api_key=2)
        
        # First call should return API key 1 (round-robin from current=2)
        api_key, nonce = manager.next_nonce()
        self.assertEqual(api_key, 1)
        self.assertIsInstance(nonce, int)
        
        # Second call should return API key 2
        api_key2, nonce2 = manager.next_nonce()
        self.assertEqual(api_key2, 2)
        # Nonce should be incremented (each API key maintains its own nonce)
        self.assertIsInstance(nonce2, int)

    def test_nonce_manager_acknowledge_failure(self):
        """Test NonceManager failure acknowledgment"""
        manager = NonceManager(account_index=65, start_api_key=1)
        
        # Get initial nonce
        api_key, nonce = manager.next_nonce()
        
        # Acknowledge failure should rollback nonce
        manager.acknowledge_failure(api_key)
        
        # Next nonce should be the same as before
        api_key2, nonce2 = manager.next_nonce()
        self.assertEqual(nonce, nonce2)

    def test_nonce_manager_hard_refresh(self):
        """Test NonceManager hard refresh"""
        manager = NonceManager(account_index=65, start_api_key=1)
        
        new_nonce = 9999999
        manager.hard_refresh_nonce(1, new_nonce)
        
        self.assertEqual(manager.nonce[1], new_nonce)

    @patch('time.time')
    def test_lighter_sdk_helper_create_order_tx_info(self, mock_time):
        """Test LighterSDKHelper order creation"""
        mock_time.return_value = 1678974447.926
        
        helper = LighterSDKHelper(account_index=65, api_key_index=1)
        
        tx_info_json = helper.create_order_tx_info(
            market_index=0,
            client_order_index=123,
            base_amount=100000,
            price=405000,
            is_ask=True,
        )
        
        tx_info = json.loads(tx_info_json)
        self.assertEqual(tx_info["AccountIndex"], 65)
        self.assertEqual(tx_info["OrderBookIndex"], 0)
        self.assertEqual(tx_info["BaseAmount"], 100000)
        self.assertEqual(tx_info["Price"], 405000)
        self.assertEqual(tx_info["IsAsk"], 1)

    @patch('time.time')
    def test_lighter_sdk_helper_create_cancel_order_tx_info(self, mock_time):
        """Test LighterSDKHelper cancel order creation"""
        mock_time.return_value = 1678974447.926
        
        helper = LighterSDKHelper(account_index=65, api_key_index=1)
        
        tx_info_json = helper.create_cancel_order_tx_info(
            market_index=0,
            order_index=123,
        )
        
        tx_info = json.loads(tx_info_json)
        self.assertEqual(tx_info["AccountIndex"], 65)
        self.assertEqual(tx_info["OrderBookIndex"], 0)
        self.assertEqual(tx_info["OrderIndex"], 123)

    def test_lighter_sdk_helper_format_send_tx_payload(self):
        """Test LighterSDKHelper sendTx payload formatting"""
        helper = LighterSDKHelper(account_index=65, api_key_index=1)
        
        payload = helper.format_send_tx_payload(
            tx_type=14,  # CREATE_ORDER
            tx_info='{"test": "data"}',
            price_protection=True,
        )
        
        self.assertEqual(payload["tx_type"], 14)
        self.assertEqual(payload["tx_info"], '{"test": "data"}')
        self.assertTrue(payload["price_protection"])

    def test_convert_amount_to_raw_units(self):
        """Test amount to raw units conversion"""
        # Test with default 6 decimals (USDC)
        amount = 100.5
        result = convert_amount_to_raw_units(amount)
        self.assertEqual(result, 100500000)  # 100.5 * 10^6
        
        # Test with custom decimals
        amount = 1.0
        result = convert_amount_to_raw_units(amount, decimals=18)
        self.assertEqual(result, 1000000000000000000)  # 1.0 * 10^18

    def test_convert_raw_units_to_amount(self):
        """Test raw units to amount conversion"""
        # Test with default 6 decimals (USDC)
        raw_units = 100500000
        result = convert_raw_units_to_amount(raw_units)
        self.assertEqual(result, 100.5)  # 100500000 / 10^6
        
        # Test with custom decimals
        raw_units = 1000000000000000000
        result = convert_raw_units_to_amount(raw_units, decimals=18)
        self.assertEqual(result, 1.0)  # 1000000000000000000 / 10^18

    def test_validate_private_key_valid(self):
        """Test private key validation with valid keys"""
        valid_keys = [
            "0x13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930",
            "13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930",
            "0x" + "a" * 64,  # All 'a' characters
            "f" * 64,  # All 'f' characters without 0x
        ]
        
        for key in valid_keys:
            self.assertTrue(validate_private_key(key))

    def test_validate_private_key_invalid(self):
        """Test private key validation with invalid keys"""
        invalid_keys = [
            "0x123",  # Too short
            "invalid_key",  # Not hex
            "0x" + "g" * 64,  # Invalid hex character
            "",  # Empty string
            None,  # None value
            "0x" + "a" * 63,  # One character short
            "0x" + "a" * 65,  # One character too long
        ]
        
        for key in invalid_keys:
            self.assertFalse(validate_private_key(key))
