from unittest import TestCase

from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import (
    LighterPerpetualConfigMap,
    convert_from_exchange_trading_pair,
    convert_to_exchange_trading_pair,
    get_new_client_order_id,
    validate_trading_pair,
    is_exchange_information_valid,
    format_decimal_places,
)
from decimal import Decimal


class LighterPerpetualUtilsTests(TestCase):
    
    def test_convert_from_exchange_trading_pair_underscore(self):
        """Test conversion from exchange format with underscore"""
        exchange_pair = "BTC_USDC"
        expected = "BTC-USDC"
        result = convert_from_exchange_trading_pair(exchange_pair)
        self.assertEqual(result, expected)

    def test_convert_from_exchange_trading_pair_slash(self):
        """Test conversion from exchange format with slash"""
        exchange_pair = "ETH/USDC"
        expected = "ETH-USDC"
        result = convert_from_exchange_trading_pair(exchange_pair)
        self.assertEqual(result, expected)

    def test_convert_from_exchange_trading_pair_already_correct(self):
        """Test conversion when already in correct format"""
        exchange_pair = "SOL-USDC"
        expected = "SOL-USDC"
        result = convert_from_exchange_trading_pair(exchange_pair)
        self.assertEqual(result, expected)

    def test_convert_to_exchange_trading_pair(self):
        """Test conversion to exchange format"""
        hb_pair = "BTC-USDC"
        expected = "BTC_USDC"
        result = convert_to_exchange_trading_pair(hb_pair)
        self.assertEqual(result, expected)

    def test_get_new_client_order_id_buy(self):
        """Test client order ID generation for buy orders"""
        order_id = get_new_client_order_id(is_buy=True, trading_pair="BTC-USDC")
        self.assertTrue(order_id.startswith("HBOTB"))
        self.assertTrue(len(order_id) > 10)

    def test_get_new_client_order_id_sell(self):
        """Test client order ID generation for sell orders"""
        order_id = get_new_client_order_id(is_buy=False, trading_pair="ETH-USDC")
        self.assertTrue(order_id.startswith("HBOTS"))
        self.assertTrue(len(order_id) > 10)

    def test_validate_trading_pair_valid(self):
        """Test trading pair validation with valid pairs"""
        valid_pairs = ["BTC-USDC", "ETH-USDC", "SOL-USDC", "AVAX-USDC"]
        for pair in valid_pairs:
            self.assertTrue(validate_trading_pair(pair))

    def test_validate_trading_pair_invalid(self):
        """Test trading pair validation with invalid pairs"""
        invalid_pairs = [
            "BTCUSDC",  # No separator
            "BTC_USDC",  # Wrong separator
            "BTC-",  # Missing quote
            "-USDC",  # Missing base
            "BTC--USDC",  # Double separator
            "",  # Empty string
            None,  # None value
            123,  # Non-string
        ]
        for pair in invalid_pairs:
            self.assertFalse(validate_trading_pair(pair))

    def test_is_exchange_information_valid_success(self):
        """Test exchange information validation with valid data"""
        valid_info = {
            "markets": [
                {"symbol": "BTC-USDC", "status": "active"},
                {"symbol": "ETH-USDC", "status": "active"},
            ]
        }
        self.assertTrue(is_exchange_information_valid(valid_info))

    def test_is_exchange_information_valid_missing_markets(self):
        """Test exchange information validation with missing markets"""
        invalid_info = {"status": "ok"}
        self.assertFalse(is_exchange_information_valid(invalid_info))

    def test_is_exchange_information_valid_empty_markets(self):
        """Test exchange information validation with empty markets"""
        invalid_info = {"markets": []}
        self.assertFalse(is_exchange_information_valid(invalid_info))

    def test_is_exchange_information_valid_not_dict(self):
        """Test exchange information validation with non-dict input"""
        invalid_info = "not a dict"
        self.assertFalse(is_exchange_information_valid(invalid_info))

    def test_format_decimal_places_zero_precision(self):
        """Test decimal formatting with zero precision"""
        value = Decimal("123.456")
        result = format_decimal_places(value, 0)
        self.assertEqual(result, Decimal("123"))

    def test_format_decimal_places_two_precision(self):
        """Test decimal formatting with two decimal places"""
        value = Decimal("123.456789")
        result = format_decimal_places(value, 2)
        self.assertEqual(result, Decimal("123.46"))

    def test_format_decimal_places_four_precision(self):
        """Test decimal formatting with four decimal places"""
        value = Decimal("123.456789")
        result = format_decimal_places(value, 4)
        self.assertEqual(result, Decimal("123.4568"))

    def test_config_map_structure(self):
        """Test that config map has required fields"""
        # Test with required fields provided
        config_data = {
            'lighter_private_key': '0x13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930',
            'lighter_account_index': 65,
            'lighter_api_key_index': 1
        }
        config = LighterPerpetualConfigMap(**config_data)
        
        # Test that required fields exist and have correct values
        self.assertEqual(config.connector, "lighter_perpetual")
        self.assertTrue(hasattr(config, 'lighter_private_key'))
        self.assertTrue(hasattr(config, 'lighter_account_index'))
        self.assertTrue(hasattr(config, 'lighter_api_key_index'))
        self.assertEqual(config.lighter_account_index, 65)
        self.assertEqual(config.lighter_api_key_index, 1)
