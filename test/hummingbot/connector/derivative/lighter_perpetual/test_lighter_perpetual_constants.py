from unittest import TestCase

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.in_flight_order import OrderState


class LighterPerpetualConstantsTests(TestCase):
    
    def test_exchange_name(self):
        """Test exchange name constant"""
        self.assertEqual(CONSTANTS.EXCHANGE_NAME, "lighter_perpetual")

    def test_domain_constants(self):
        """Test domain constants"""
        self.assertEqual(CONSTANTS.DEFAULT_DOMAIN, "lighter_perpetual")
        self.assertEqual(CONSTANTS.TESTNET_DOMAIN, "lighter_perpetual_testnet")

    def test_rest_urls(self):
        """Test REST URL constants"""
        self.assertIn(CONSTANTS.DEFAULT_DOMAIN, CONSTANTS.REST_URL)
        self.assertIn(CONSTANTS.TESTNET_DOMAIN, CONSTANTS.REST_URL)
        
        # URLs should be valid HTTPS URLs
        for domain, url in CONSTANTS.REST_URL.items():
            self.assertTrue(url.startswith("https://"))
            self.assertIn("zklighter.elliot.ai", url)

    def test_ws_urls(self):
        """Test WebSocket URL constants"""
        self.assertIn(CONSTANTS.DEFAULT_DOMAIN, CONSTANTS.WS_URL)
        self.assertIn(CONSTANTS.TESTNET_DOMAIN, CONSTANTS.WS_URL)
        
        # URLs should be valid WSS URLs with /stream path
        for domain, url in CONSTANTS.WS_URL.items():
            self.assertTrue(url.startswith("wss://"))
            self.assertTrue(url.endswith("/stream"))
            self.assertIn("zklighter.elliot.ai", url)

    def test_api_version(self):
        """Test API version constant"""
        self.assertEqual(CONSTANTS.API_VERSION, "/api/v1")

    def test_transaction_types(self):
        """Test transaction type constants match SDK values"""
        # These are the correct values from the SDK
        self.assertEqual(CONSTANTS.TX_TYPE_CHANGE_PUB_KEY, 8)
        self.assertEqual(CONSTANTS.TX_TYPE_CREATE_SUB_ACCOUNT, 9)
        self.assertEqual(CONSTANTS.TX_TYPE_CREATE_PUBLIC_POOL, 10)
        self.assertEqual(CONSTANTS.TX_TYPE_UPDATE_PUBLIC_POOL, 11)
        self.assertEqual(CONSTANTS.TX_TYPE_TRANSFER, 12)
        self.assertEqual(CONSTANTS.TX_TYPE_WITHDRAW, 13)
        self.assertEqual(CONSTANTS.TX_TYPE_CREATE_ORDER, 14)
        self.assertEqual(CONSTANTS.TX_TYPE_CANCEL_ORDER, 15)
        self.assertEqual(CONSTANTS.TX_TYPE_CANCEL_ALL_ORDERS, 16)
        self.assertEqual(CONSTANTS.TX_TYPE_MODIFY_ORDER, 17)
        self.assertEqual(CONSTANTS.TX_TYPE_MINT_SHARES, 18)
        self.assertEqual(CONSTANTS.TX_TYPE_BURN_SHARES, 19)
        self.assertEqual(CONSTANTS.TX_TYPE_UPDATE_LEVERAGE, 20)

    def test_order_types(self):
        """Test order type constants"""
        self.assertEqual(CONSTANTS.ORDER_TYPE_LIMIT, 0)
        self.assertEqual(CONSTANTS.ORDER_TYPE_MARKET, 1)
        self.assertEqual(CONSTANTS.ORDER_TYPE_STOP_LOSS, 2)
        self.assertEqual(CONSTANTS.ORDER_TYPE_TAKE_PROFIT, 3)

    def test_time_in_force(self):
        """Test time in force constants"""
        self.assertEqual(CONSTANTS.TIME_IN_FORCE_GOOD_TILL_TIME, 0)
        self.assertEqual(CONSTANTS.TIME_IN_FORCE_IMMEDIATE_OR_CANCEL, 1)
        self.assertEqual(CONSTANTS.TIME_IN_FORCE_FILL_OR_KILL, 2)
        self.assertEqual(CONSTANTS.TIME_IN_FORCE_GOOD_TILL_CANCELLED, 3)

    def test_order_state_mapping_completeness(self):
        """Test that all order states from API are mapped"""
        expected_statuses = [
            "in-progress",
            "pending", 
            "open",
            "filled",
            "canceled",
            "canceled-post-only",
            "canceled-reduce-only", 
            "canceled-position-not-allowed",
            "canceled-margin-not-allowed",
            "canceled-too-much-slippage",
            "canceled-not-enough-liquidity",
            "canceled-self-trade",
            "canceled-expired",
            "canceled-oco",
            "canceled-child",
            "canceled-liquidation",
        ]
        
        for status in expected_statuses:
            self.assertIn(status, CONSTANTS.ORDER_STATE)

    def test_order_state_mapping_values(self):
        """Test that order states map to correct Hummingbot values"""
        # Test key mappings
        self.assertEqual(CONSTANTS.ORDER_STATE["in-progress"], OrderState.PENDING_CREATE)
        self.assertEqual(CONSTANTS.ORDER_STATE["pending"], OrderState.PENDING_CREATE)
        self.assertEqual(CONSTANTS.ORDER_STATE["open"], OrderState.OPEN)
        self.assertEqual(CONSTANTS.ORDER_STATE["filled"], OrderState.FILLED)
        self.assertEqual(CONSTANTS.ORDER_STATE["canceled"], OrderState.CANCELED)
        
        # All canceled-* statuses should map to CANCELED
        canceled_statuses = [k for k in CONSTANTS.ORDER_STATE.keys() if k.startswith("canceled")]
        for status in canceled_statuses:
            self.assertEqual(CONSTANTS.ORDER_STATE[status], OrderState.CANCELED)

    def test_websocket_channels(self):
        """Test WebSocket channel constants"""
        self.assertEqual(CONSTANTS.WS_CHANNEL_ORDER_BOOK, "orderbook")
        self.assertEqual(CONSTANTS.WS_CHANNEL_TRADES, "trades")
        self.assertEqual(CONSTANTS.WS_CHANNEL_ACCOUNT, "account")

    def test_rate_limits_structure(self):
        """Test rate limits are properly structured"""
        self.assertIsInstance(CONSTANTS.RATE_LIMITS, list)
        self.assertGreater(len(CONSTANTS.RATE_LIMITS), 0)
        
        # Each rate limit should have required fields
        for rate_limit in CONSTANTS.RATE_LIMITS:
            self.assertTrue(hasattr(rate_limit, 'limit_id'))
            self.assertTrue(hasattr(rate_limit, 'limit'))
            self.assertTrue(hasattr(rate_limit, 'time_interval'))

    def test_default_fees(self):
        """Test default fee structure"""
        self.assertIn("maker_percent_fee_decimal", CONSTANTS.DEFAULT_FEES)
        self.assertIn("taker_percent_fee_decimal", CONSTANTS.DEFAULT_FEES)
        
        # Fees should be reasonable values
        maker_fee = CONSTANTS.DEFAULT_FEES["maker_percent_fee_decimal"]
        taker_fee = CONSTANTS.DEFAULT_FEES["taker_percent_fee_decimal"]
        
        self.assertGreaterEqual(maker_fee, 0)
        self.assertLess(maker_fee, 0.01)  # Less than 1%
        self.assertGreaterEqual(taker_fee, 0)
        self.assertLess(taker_fee, 0.01)  # Less than 1%

    def test_broker_and_client_id_constants(self):
        """Test broker ID and client order ID constants"""
        self.assertEqual(CONSTANTS.BROKER_ID, "HBOT")
        self.assertEqual(CONSTANTS.CLIENT_ORDER_ID_PREFIX, "HBOT")
        self.assertIsInstance(CONSTANTS.MAX_ORDER_ID_LEN, int)
        self.assertGreater(CONSTANTS.MAX_ORDER_ID_LEN, 0)

    def test_heartbeat_interval(self):
        """Test heartbeat interval constant"""
        self.assertIsInstance(CONSTANTS.HEARTBEAT_TIME_INTERVAL, float)
        self.assertGreater(CONSTANTS.HEARTBEAT_TIME_INTERVAL, 0)

    def test_position_modes(self):
        """Test position mode constants"""
        self.assertEqual(CONSTANTS.POSITION_MODE_ONE_WAY, "OneWay")
        self.assertEqual(CONSTANTS.POSITION_MODE_HEDGE, "Hedge")
        self.assertIn(CONSTANTS.POSITION_MODE_ONE_WAY, CONSTANTS.SUPPORTED_POSITION_MODES)

    def test_minimum_order_constants(self):
        """Test minimum order size constants"""
        self.assertIsInstance(CONSTANTS.MIN_ORDER_SIZE, (int, float))
        self.assertIsInstance(CONSTANTS.MIN_NOTIONAL_SIZE, (int, float))
        self.assertIsInstance(CONSTANTS.TICK_SIZE, (int, float))
        self.assertIsInstance(CONSTANTS.STEP_SIZE, (int, float))
        
        # All should be positive
        self.assertGreater(CONSTANTS.MIN_ORDER_SIZE, 0)
        self.assertGreater(CONSTANTS.MIN_NOTIONAL_SIZE, 0)
        self.assertGreater(CONSTANTS.TICK_SIZE, 0)
        self.assertGreater(CONSTANTS.STEP_SIZE, 0)

    def test_endpoint_paths(self):
        """Test API endpoint path constants"""
        # Public endpoints
        self.assertEqual(CONSTANTS.STATUS_PATH_URL, "/")
        self.assertEqual(CONSTANTS.ORDER_BOOKS_PATH_URL, "/orderBooks")
        self.assertEqual(CONSTANTS.RECENT_TRADES_PATH_URL, "/recentTrades")
        
        # Private endpoints
        self.assertEqual(CONSTANTS.ACCOUNT_PATH_URL, "/account")
        self.assertEqual(CONSTANTS.SEND_TX_PATH_URL, "/sendTx")
        self.assertEqual(CONSTANTS.NEXT_NONCE_PATH_URL, "/nextNonce")
        
        # All paths should start with /
        endpoint_paths = [
            CONSTANTS.STATUS_PATH_URL,
            CONSTANTS.ORDER_BOOKS_PATH_URL,
            CONSTANTS.ACCOUNT_PATH_URL,
            CONSTANTS.SEND_TX_PATH_URL,
        ]
        
        for path in endpoint_paths:
            self.assertTrue(path.startswith("/"))
