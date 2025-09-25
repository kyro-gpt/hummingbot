import unittest
from decimal import Decimal

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_utils as utils


class BackpackPerpetualUtilsTestCases(unittest.TestCase):

    def test_is_exchange_information_valid_spot_market(self):
        """Test that spot markets are rejected for derivative connector"""
        spot_market_info = {
            "symbol": "BTC_USDC",
            "baseSymbol": "BTC",
            "quoteSymbol": "USDC",
            "orderBookState": "OPEN",
            "marketType": "SPOT"  # Should be rejected
        }
        self.assertFalse(utils.is_exchange_information_valid(spot_market_info))

    def test_is_exchange_information_valid_future_market(self):
        """Test that future markets are accepted for derivative connector"""
        future_market_info = {
            "symbol": "BTC_USDC",
            "baseSymbol": "BTC",
            "quoteSymbol": "USDC",
            "orderBookState": "OPEN",
            "marketType": "FUTURE"  # Should be accepted
        }
        self.assertTrue(utils.is_exchange_information_valid(future_market_info))

    def test_is_exchange_information_valid_perpetual_market(self):
        """Test that perpetual markets are accepted for derivative connector"""
        perpetual_market_info = {
            "symbol": "ETH_USDT",
            "baseSymbol": "ETH",
            "quoteSymbol": "USDT",
            "orderBookState": "OPEN",
            "marketType": "PERPETUAL"  # Should be accepted
        }
        self.assertTrue(utils.is_exchange_information_valid(perpetual_market_info))

    def test_is_exchange_information_valid_closed_market(self):
        """Test that closed markets are rejected"""
        closed_market_info = {
            "symbol": "BTC_USDC",
            "baseSymbol": "BTC",
            "quoteSymbol": "USDC",
            "orderBookState": "CLOSED",  # Should be rejected
            "marketType": "FUTURE"
        }
        self.assertFalse(utils.is_exchange_information_valid(closed_market_info))

    def test_is_exchange_information_valid_missing_fields(self):
        """Test that markets with missing required fields are rejected"""
        incomplete_market_info = {
            "symbol": "BTC_USDC",
            # Missing baseSymbol, quoteSymbol
            "orderBookState": "OPEN",
            "marketType": "FUTURE"
        }
        self.assertFalse(utils.is_exchange_information_valid(incomplete_market_info))

    def test_validate_position_mode_config(self):
        """Test position mode validation"""
        # Valid modes
        self.assertTrue(utils.validate_position_mode_config("ONEWAY"))
        self.assertTrue(utils.validate_position_mode_config("HEDGE"))
        self.assertTrue(utils.validate_position_mode_config("oneway"))  # Case insensitive

        # Invalid modes
        self.assertFalse(utils.validate_position_mode_config("INVALID"))
        self.assertFalse(utils.validate_position_mode_config(""))

    def test_validate_leverage_config(self):
        """Test leverage validation"""
        # Valid leverage values
        self.assertTrue(utils.validate_leverage_config(1))
        self.assertTrue(utils.validate_leverage_config(10))
        self.assertTrue(utils.validate_leverage_config(20))

        # Invalid leverage values
        self.assertFalse(utils.validate_leverage_config(0))
        self.assertFalse(utils.validate_leverage_config(-1))
        self.assertFalse(utils.validate_leverage_config(25))  # Too high

    def test_validate_derivative_trading_pair(self):
        """Test derivative trading pair validation"""
        # Valid pairs (with stablecoin quotes)
        self.assertTrue(utils.validate_derivative_trading_pair("BTC-USDC"))
        self.assertTrue(utils.validate_derivative_trading_pair("ETH-USDT"))
        self.assertTrue(utils.validate_derivative_trading_pair("SOL-USD"))

        # Invalid pairs
        self.assertFalse(utils.validate_derivative_trading_pair("BTC-ETH"))  # Not stablecoin quote
        self.assertFalse(utils.validate_derivative_trading_pair("INVALID"))  # No separator
        self.assertFalse(utils.validate_derivative_trading_pair(""))  # Empty
        self.assertFalse(utils.validate_derivative_trading_pair("BTC-"))  # Missing quote

    def test_get_collateral_token_from_trading_pair(self):
        """Test collateral token extraction"""
        # Valid pairs
        self.assertEqual("USDC", utils.get_collateral_token_from_trading_pair("BTC-USDC"))
        self.assertEqual("USDT", utils.get_collateral_token_from_trading_pair("ETH-USDT"))
        self.assertEqual("USD", utils.get_collateral_token_from_trading_pair("SOL-USD"))

        # Invalid pairs should return default
        self.assertEqual("USDC", utils.get_collateral_token_from_trading_pair("INVALID"))
        self.assertEqual("USDC", utils.get_collateral_token_from_trading_pair(""))

    def test_default_fees_structure(self):
        """Test that derivative fees are properly structured"""
        fees = utils.DEFAULT_FEES

        # Check that fees exist and are reasonable for derivatives
        self.assertIsInstance(fees.maker_percent_fee_decimal, Decimal)
        self.assertIsInstance(fees.taker_percent_fee_decimal, Decimal)

        # Derivative fees should be non-zero and reasonable
        self.assertGreater(fees.maker_percent_fee_decimal, Decimal("0"))
        self.assertGreater(fees.taker_percent_fee_decimal, Decimal("0"))

        # Taker fees should be higher than maker fees
        self.assertGreater(fees.taker_percent_fee_decimal, fees.maker_percent_fee_decimal)

        # Fees should be reasonable (less than 1%)
        self.assertLess(fees.maker_percent_fee_decimal, Decimal("0.01"))
        self.assertLess(fees.taker_percent_fee_decimal, Decimal("0.01"))

    def test_config_map_structure(self):
        """Test that the derivative config map has required fields"""
        config = utils.BackpackPerpetualConfigMap.model_construct()

        # Check connector name
        self.assertEqual("backpack_perpetual", config.connector)

        # Check that derivative-specific fields exist
        self.assertTrue(hasattr(config, 'backpack_perpetual_leverage'))
        self.assertTrue(hasattr(config, 'backpack_perpetual_position_mode'))

        # Check default values
        self.assertEqual(1, config.backpack_perpetual_leverage)
        self.assertEqual("ONEWAY", config.backpack_perpetual_position_mode)
