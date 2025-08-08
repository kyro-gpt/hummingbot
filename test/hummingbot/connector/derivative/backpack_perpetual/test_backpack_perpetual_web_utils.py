import unittest

import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_web_utils as web_utils


class BackpackPerpetualWebUtilsTestCases(unittest.TestCase):

    def test_public_rest_url(self):
        path_url = "/TEST_PATH"
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION + path_url
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url, domain))

    def test_private_rest_url(self):
        path_url = "/TEST_PATH"
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = CONSTANTS.REST_URL + CONSTANTS.API_VERSION + path_url
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url, domain))

    def test_wss_url(self):
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = CONSTANTS.WS_URL
        self.assertEqual(expected_url, web_utils.ws_url(domain))

    def test_derivative_specific_urls(self):
        """Test derivative-specific URL building functions"""
        # Test funding info URL
        funding_url = web_utils.get_funding_info_url()
        self.assertTrue(funding_url.endswith(CONSTANTS.FUNDING_RATES_PATH_URL))

        # Test funding info URL with trading pair
        trading_pair = "BTC-USDC"
        funding_url_with_pair = web_utils.get_funding_info_url(trading_pair)
        self.assertIn("BTC_USDC", funding_url_with_pair)

        # Test position URL
        position_url = web_utils.get_position_url()
        self.assertTrue(position_url.endswith(CONSTANTS.POSITION_PATH_URL))

        # Test mark price URL
        mark_price_url = web_utils.get_mark_price_url()
        self.assertTrue(mark_price_url.endswith(CONSTANTS.MARK_PRICES_PATH_URL))

    def test_is_private_endpoint_derivative_paths(self):
        """Test that derivative-specific paths are correctly identified as private"""
        # Position endpoint should be private
        self.assertTrue(web_utils.is_private_endpoint(CONSTANTS.POSITION_PATH_URL))

        # Funding history should be private
        self.assertTrue(web_utils.is_private_endpoint(CONSTANTS.HISTORY_FUNDING_PATH_URL))

        # PnL history should be private
        self.assertTrue(web_utils.is_private_endpoint(CONSTANTS.HISTORY_PNL_PATH_URL))

    def test_is_public_endpoint_derivative_paths(self):
        """Test that derivative-specific public paths are correctly identified"""
        # Mark prices should be public
        self.assertTrue(web_utils.is_public_endpoint(CONSTANTS.MARK_PRICES_PATH_URL))

        # Funding rates should be public
        self.assertTrue(web_utils.is_public_endpoint(CONSTANTS.FUNDING_RATES_PATH_URL))

        # Open interest should be public
        self.assertTrue(web_utils.is_public_endpoint(CONSTANTS.OPEN_INTEREST_PATH_URL))

    def test_trading_pair_conversion(self):
        """Test trading pair format conversion functions for perpetual trading"""
        # Test Hummingbot to Backpack perpetual format
        hb_pair = "SOL-USDC"
        backpack_pair = web_utils.convert_to_exchange_trading_pair(hb_pair)
        self.assertEqual("SOL_USDC_PERP", backpack_pair)

        # Test Backpack perpetual to Hummingbot format
        backpack_pair = "ETH_USDT_PERP"
        hb_pair = web_utils.convert_from_exchange_trading_pair(backpack_pair)
        self.assertEqual("ETH-USDT", hb_pair)

        # Test format_trading_pair function
        formatted_pair = web_utils.format_trading_pair("SOL-USDC")
        self.assertEqual("SOL_USDC_PERP", formatted_pair)

        # Test handling of symbols without _PERP suffix (fallback)
        backpack_pair_no_perp = "BTC_USD"
        hb_pair_fallback = web_utils.convert_from_exchange_trading_pair(backpack_pair_no_perp)
        self.assertEqual("BTC-USD", hb_pair_fallback)

    def test_endpoint_mapping_derivative_specific(self):
        """Test that derivative-specific endpoints are properly mapped"""
        # Test position endpoint mapping
        position_url = web_utils.get_rest_url_for_endpoint("POSITION")
        self.assertIn(CONSTANTS.POSITION_PATH_URL, position_url)

        # Test funding rates endpoint mapping
        funding_url = web_utils.get_rest_url_for_endpoint("FUNDING_RATES")
        self.assertIn(CONSTANTS.FUNDING_RATES_PATH_URL, funding_url)

        # Test mark prices endpoint mapping
        mark_price_url = web_utils.get_rest_url_for_endpoint("MARK_PRICES")
        self.assertIn(CONSTANTS.MARK_PRICES_PATH_URL, mark_price_url)
