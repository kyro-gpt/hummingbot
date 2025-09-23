import unittest

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_utils as aster_perpetual_utils
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_utils import AsterPerpetualConfigMap


class AsterPerpetualUtilsUnitTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    def test_default_fees(self):
        """Test that default fees are properly configured"""
        fees = aster_perpetual_utils.DEFAULT_FEES

        self.assertIsNotNone(fees.maker_percent_fee_decimal)
        self.assertIsNotNone(fees.taker_percent_fee_decimal)
        self.assertTrue(fees.buy_percent_fee_deducted_from_returns)

        # Verify fee values are reasonable (between 0 and 1%)
        self.assertGreaterEqual(fees.maker_percent_fee_decimal, 0)
        self.assertLessEqual(fees.maker_percent_fee_decimal, 0.01)
        self.assertGreaterEqual(fees.taker_percent_fee_decimal, 0)
        self.assertLessEqual(fees.taker_percent_fee_decimal, 0.01)

    def test_centralized_flag(self):
        """Test that the centralized flag is properly set"""
        self.assertTrue(aster_perpetual_utils.CENTRALIZED)

    def test_example_pair(self):
        """Test that example pair is properly configured"""
        self.assertEqual("BTC-USDT", aster_perpetual_utils.EXAMPLE_PAIR)

    def test_broker_id(self):
        """Test that broker ID is configured"""
        self.assertIsNotNone(aster_perpetual_utils.BROKER_ID)
        self.assertIsInstance(aster_perpetual_utils.BROKER_ID, str)
        self.assertTrue(len(aster_perpetual_utils.BROKER_ID) > 0)

    def test_config_map_structure(self):
        """Test that the config map has proper structure"""
        # Verify connector name from class definition
        self.assertEqual(AsterPerpetualConfigMap.__annotations__['connector'], str)

        # Verify Web3 configuration fields are defined in the class
        self.assertIn('aster_perpetual_user_wallet', AsterPerpetualConfigMap.model_fields)
        self.assertIn('aster_perpetual_signer_wallet', AsterPerpetualConfigMap.model_fields)
        self.assertIn('aster_perpetual_private_key', AsterPerpetualConfigMap.model_fields)

    def test_other_domains_configuration(self):
        """Test that testnet domain configuration is proper"""
        self.assertIn("aster_perpetual_testnet", aster_perpetual_utils.OTHER_DOMAINS)
        self.assertIn("aster_perpetual_testnet", aster_perpetual_utils.OTHER_DOMAINS_PARAMETER)
        self.assertIn("aster_perpetual_testnet", aster_perpetual_utils.OTHER_DOMAINS_EXAMPLE_PAIR)
        self.assertIn("aster_perpetual_testnet", aster_perpetual_utils.OTHER_DOMAINS_DEFAULT_FEES)
        self.assertIn("aster_perpetual_testnet", aster_perpetual_utils.OTHER_DOMAINS_KEYS)

    def test_testnet_config_map_structure(self):
        """Test that the testnet config map has proper structure"""
        from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_utils import AsterPerpetualTestnetConfigMap

        # Verify connector name from class definition
        self.assertEqual(AsterPerpetualTestnetConfigMap.__annotations__['connector'], str)

        # Verify Web3 testnet configuration fields are defined in the class
        self.assertIn('aster_perpetual_testnet_user_wallet', AsterPerpetualTestnetConfigMap.model_fields)
        self.assertIn('aster_perpetual_testnet_signer_wallet', AsterPerpetualTestnetConfigMap.model_fields)
        self.assertIn('aster_perpetual_testnet_private_key', AsterPerpetualTestnetConfigMap.model_fields)

    def test_keys_construction(self):
        """Test that KEYS object is properly constructed"""
        keys = aster_perpetual_utils.KEYS
        self.assertIsNotNone(keys)
        self.assertEqual(keys.connector, "aster_perpetual")


if __name__ == "__main__":
    unittest.main()
