from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils import (
    public_rest_url,
    private_rest_url,
    wss_url,
    build_api_factory,
    create_throttler,
    format_trading_pair_to_market_id,
    format_market_id_to_trading_pair,
    convert_client_order_id_to_index,
    convert_client_order_index_to_id,
    get_rest_url_for_endpoint,
    is_exchange_information_valid,
)


class LighterPerpetualWebUtilsTests(TestCase):
    
    def test_public_rest_url_default_domain(self):
        """Test public REST URL generation with default domain"""
        path = "/orderBooks"
        expected = f"{CONSTANTS.REST_URL[CONSTANTS.DEFAULT_DOMAIN]}{CONSTANTS.API_VERSION}{path}"
        result = public_rest_url(path)
        self.assertEqual(result, expected)

    def test_public_rest_url_testnet_domain(self):
        """Test public REST URL generation with testnet domain"""
        path = "/orderBooks"
        expected = f"{CONSTANTS.REST_URL[CONSTANTS.TESTNET_DOMAIN]}{CONSTANTS.API_VERSION}{path}"
        result = public_rest_url(path, CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(result, expected)

    def test_private_rest_url_default_domain(self):
        """Test private REST URL generation with default domain"""
        path = "/account"
        expected = f"{CONSTANTS.REST_URL[CONSTANTS.DEFAULT_DOMAIN]}{CONSTANTS.API_VERSION}{path}"
        result = private_rest_url(path)
        self.assertEqual(result, expected)

    def test_private_rest_url_testnet_domain(self):
        """Test private REST URL generation with testnet domain"""
        path = "/account"
        expected = f"{CONSTANTS.REST_URL[CONSTANTS.TESTNET_DOMAIN]}{CONSTANTS.API_VERSION}{path}"
        result = private_rest_url(path, CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(result, expected)

    def test_wss_url_default_domain(self):
        """Test WebSocket URL generation with default domain"""
        expected = CONSTANTS.WS_URL[CONSTANTS.DEFAULT_DOMAIN]
        result = wss_url()
        self.assertEqual(result, expected)

    def test_wss_url_testnet_domain(self):
        """Test WebSocket URL generation with testnet domain"""
        expected = CONSTANTS.WS_URL[CONSTANTS.TESTNET_DOMAIN]
        result = wss_url(CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(result, expected)

    def test_create_throttler(self):
        """Test throttler creation"""
        throttler = create_throttler()
        self.assertIsNotNone(throttler)
        # Verify it has the expected rate limits
        self.assertEqual(len(throttler._rate_limits), len(CONSTANTS.RATE_LIMITS))

    def test_build_api_factory_without_auth(self):
        """Test API factory creation without authentication"""
        factory = build_api_factory()
        self.assertIsNotNone(factory)

    def test_build_api_factory_with_auth(self):
        """Test API factory creation with authentication"""
        mock_auth = MagicMock()
        factory = build_api_factory(auth=mock_auth)
        self.assertIsNotNone(factory)

    def test_format_trading_pair_to_market_id_known_pair(self):
        """Test trading pair to market ID conversion for known pairs"""
        # Test with BTC-USDC which should map to 0
        result = format_trading_pair_to_market_id("BTC-USDC")
        self.assertEqual(result, 0)

    def test_format_trading_pair_to_market_id_unknown_pair(self):
        """Test trading pair to market ID conversion for unknown pairs"""
        # Test with unknown pair which should default to 0
        result = format_trading_pair_to_market_id("UNKNOWN-PAIR")
        self.assertEqual(result, 0)

    def test_format_market_id_to_trading_pair_known_id(self):
        """Test market ID to trading pair conversion for known IDs"""
        # Test with ID 0 which should map to BTC-USDC
        result = format_market_id_to_trading_pair(0)
        self.assertEqual(result, "BTC-USDC")

    def test_format_market_id_to_trading_pair_unknown_id(self):
        """Test market ID to trading pair conversion for unknown IDs"""
        # Test with unknown ID which should default to UNKNOWN-USDC
        result = format_market_id_to_trading_pair(999)
        self.assertEqual(result, "UNKNOWN-USDC")

    def test_convert_client_order_id_to_index(self):
        """Test client order ID to index conversion"""
        client_order_id = "HBOT123456789"
        result = convert_client_order_id_to_index(client_order_id)
        self.assertIsInstance(result, int)
        self.assertGreater(result, 0)
        
        # Test that same ID always produces same index
        result2 = convert_client_order_id_to_index(client_order_id)
        self.assertEqual(result, result2)

    def test_convert_client_order_id_to_index_with_prefix(self):
        """Test client order ID to index conversion removes prefix"""
        client_order_id = "HBOT123456789"
        result1 = convert_client_order_id_to_index(client_order_id)
        
        # Should produce same result even with prefix already present
        client_order_id_with_prefix = f"{CONSTANTS.CLIENT_ORDER_ID_PREFIX}123456789"
        result2 = convert_client_order_id_to_index(client_order_id_with_prefix)
        
        # Results should be the same since prefix is removed
        self.assertEqual(result1, result2)

    def test_convert_client_order_index_to_id(self):
        """Test client order index to ID conversion"""
        index = 123456789
        result = convert_client_order_index_to_id(index)
        self.assertTrue(result.startswith(CONSTANTS.CLIENT_ORDER_ID_PREFIX))
        self.assertIn(str(index), result)

    def test_convert_client_order_index_to_id_custom_prefix(self):
        """Test client order index to ID conversion with custom prefix"""
        index = 123456789
        custom_prefix = "TEST"
        result = convert_client_order_index_to_id(index, custom_prefix)
        self.assertTrue(result.startswith(custom_prefix))
        self.assertIn(str(index), result)

    def test_get_rest_url_for_endpoint_private(self):
        """Test REST URL generation for private endpoints"""
        private_endpoint = CONSTANTS.ACCOUNT_PATH_URL
        result = get_rest_url_for_endpoint(private_endpoint)
        expected = private_rest_url(private_endpoint)
        self.assertEqual(result, expected)

    def test_get_rest_url_for_endpoint_public(self):
        """Test REST URL generation for public endpoints"""
        public_endpoint = CONSTANTS.ORDER_BOOKS_PATH_URL
        result = get_rest_url_for_endpoint(public_endpoint)
        expected = public_rest_url(public_endpoint)
        self.assertEqual(result, expected)

    def test_is_exchange_information_valid_success(self):
        """Test exchange information validation with valid data"""
        valid_info = {
            "markets": ["BTC-USDC", "ETH-USDC"],
            "status": "ok"
        }
        self.assertTrue(is_exchange_information_valid(valid_info))

    def test_is_exchange_information_valid_missing_fields(self):
        """Test exchange information validation with missing required fields"""
        invalid_info = {"status": "ok"}  # Missing markets
        self.assertFalse(is_exchange_information_valid(invalid_info))

    def test_is_exchange_information_valid_not_dict(self):
        """Test exchange information validation with non-dict input"""
        invalid_info = "not a dict"
        self.assertFalse(is_exchange_information_valid(invalid_info))
