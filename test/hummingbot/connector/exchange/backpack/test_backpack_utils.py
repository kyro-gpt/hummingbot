import unittest

from hummingbot.connector.exchange.backpack import backpack_utils as utils


class BackpackUtilTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "SOL"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.hb_trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_{cls.quote_asset}"

    def test_is_exchange_information_valid(self):
        # Test invalid cases
        invalid_info_1 = {
            "orderBookState": "CLOSED",
            "marketType": "SPOT",
            "symbol": "SOL_USDC",
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC"
        }
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_1))

        invalid_info_2 = {
            "orderBookState": "OPEN",
            "marketType": "FUTURES",  # Not SPOT
            "symbol": "SOL_USDC",
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC"
        }
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_2))

        invalid_info_3 = {
            "orderBookState": "OPEN",
            "marketType": "SPOT",
            # Missing symbol
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC"
        }
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_3))

        invalid_info_4 = {
            "orderBookState": "OPEN",
            "marketType": "SPOT",
            "symbol": "SOL_USDC",
            # Missing baseSymbol and quoteSymbol
        }
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_4))

        # Test valid case
        valid_info = {
            "orderBookState": "OPEN",
            "marketType": "SPOT",
            "symbol": "SOL_USDC",
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC"
        }
        self.assertTrue(utils.is_exchange_information_valid(valid_info))

        # Test case insensitive
        valid_info_lowercase = {
            "orderBookState": "open",
            "marketType": "spot",
            "symbol": "SOL_USDC",
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC"
        }
        self.assertTrue(utils.is_exchange_information_valid(valid_info_lowercase))
