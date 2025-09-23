import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_auth import AsterPerpetualAuth
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType


class AsterPerpetualDerivativeUnitTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.symbol = f"{cls.base_asset}{cls.quote_asset}"
        cls.domain = CONSTANTS.TESTNET_DOMAIN

        # Test Web3 credentials
        cls.user_wallet = '0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e'  # noqa: mock
        cls.signer_wallet = '0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0'  # noqa: mock
        cls.private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"  # noqa: mock

    def setUp(self) -> None:
        super().setUp()
        # Create fresh event loop for each test
        import asyncio
        self.ev_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.ev_loop)
        
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())
        
        self.connector = AsterPerpetualDerivative(
            client_config_map=self.client_config_map,
            aster_perpetual_user_wallet=self.user_wallet,
            aster_perpetual_signer_wallet=self.signer_wallet,
            aster_perpetual_private_key=self.private_key,
            trading_pairs=[self.trading_pair],
            trading_required=False,
            domain=self.domain,
        )

    def tearDown(self) -> None:
        self.ev_loop.close()
        super().tearDown()

    def async_run_with_timeout(self, coroutine, timeout: float = 1):
        import asyncio
        return self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))

    # === Property Tests ===

    def test_name_property(self):
        """Test exchange name property"""
        self.assertEqual(self.connector.name, CONSTANTS.EXCHANGE_NAME)

    def test_authenticator_property(self):
        """Test authenticator property returns Web3 auth"""
        authenticator = self.connector.authenticator
        self.assertIsInstance(authenticator, AsterPerpetualAuth)
        self.assertEqual(authenticator._user_wallet, self.user_wallet)
        self.assertEqual(authenticator._signer_wallet, self.signer_wallet)
        self.assertEqual(authenticator._private_key, self.private_key)

    def test_rate_limits_rules_property(self):
        """Test rate limits configuration"""
        rate_limits = self.connector.rate_limits_rules
        self.assertEqual(rate_limits, CONSTANTS.RATE_LIMITS)
        self.assertGreater(len(rate_limits), 0)

    def test_domain_property(self):
        """Test domain property"""
        self.assertEqual(self.connector.domain, self.domain)

    def test_client_order_id_properties(self):
        """Test client order ID configuration"""
        self.assertEqual(self.connector.client_order_id_max_length, CONSTANTS.MAX_ORDER_ID_LEN)
        self.assertEqual(self.connector.client_order_id_prefix, CONSTANTS.BROKER_ID)

    def test_request_path_properties(self):
        """Test API request path properties"""
        self.assertEqual(self.connector.trading_rules_request_path, CONSTANTS.EXCHANGE_INFO_URL)
        self.assertEqual(self.connector.trading_pairs_request_path, CONSTANTS.EXCHANGE_INFO_URL)
        self.assertEqual(self.connector.check_network_request_path, CONSTANTS.PING_URL)

    def test_trading_properties(self):
        """Test trading-related properties"""
        self.assertTrue(self.connector.is_cancel_request_in_exchange_synchronous)
        self.assertFalse(self.connector.is_trading_required)  # Set to False in setUp
        self.assertEqual(self.connector.trading_pairs, [self.trading_pair])

    def test_funding_fee_poll_interval(self):
        """Test funding fee polling interval"""
        self.assertEqual(self.connector.funding_fee_poll_interval, 600)

    def test_supported_order_types(self):
        """Test supported order types"""
        supported_types = self.connector.supported_order_types()
        expected_types = [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]
        self.assertEqual(supported_types, expected_types)

    def test_supported_position_modes(self):
        """Test supported position modes"""
        supported_modes = self.connector.supported_position_modes()
        expected_modes = [PositionMode.ONEWAY, PositionMode.HEDGE]
        self.assertEqual(supported_modes, expected_modes)

    # === Collateral Token Tests ===

    def test_collateral_tokens_with_mock_trading_rule(self):
        """Test collateral token methods with mock trading rule"""
        # Create mock trading rule
        mock_trading_rule = MagicMock()
        mock_trading_rule.buy_order_collateral_token = "USDT"
        mock_trading_rule.sell_order_collateral_token = "USDT"

        # Mock the trading rules dictionary
        self.connector._trading_rules = {self.trading_pair: mock_trading_rule}

        buy_token = self.connector.get_buy_collateral_token(self.trading_pair)
        sell_token = self.connector.get_sell_collateral_token(self.trading_pair)

        self.assertEqual(buy_token, "USDT")
        self.assertEqual(sell_token, "USDT")

    # === Error Detection Tests ===

    def test_time_synchronizer_error_detection(self):
        """Test time synchronizer error detection"""
        # Test with time sync error
        time_sync_error = Exception("Timestamp for this request is outside of the recvWindow. Error code: -1021")
        result = self.connector._is_request_exception_related_to_time_synchronizer(time_sync_error)
        self.assertTrue(result)

        # Test with non-time sync error
        other_error = Exception("Some other error")
        result = self.connector._is_request_exception_related_to_time_synchronizer(other_error)
        self.assertFalse(result)

    def test_order_not_found_error_detection(self):
        """Test order not found error detection"""
        # Test status update error
        status_error = Exception(f"{CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE} - {CONSTANTS.ORDER_NOT_EXIST_MESSAGE}")
        result = self.connector._is_order_not_found_during_status_update_error(status_error)
        self.assertTrue(result)

        # Test cancelation error
        cancel_error = Exception(f"{CONSTANTS.UNKNOWN_ORDER_ERROR_CODE} - {CONSTANTS.UNKNOWN_ORDER_MESSAGE}")
        result = self.connector._is_order_not_found_during_cancelation_error(cancel_error)
        self.assertTrue(result)

    # === Factory Method Tests ===

    def test_create_web_assistants_factory(self):
        """Test web assistants factory creation"""
        factory = self.connector._create_web_assistants_factory()
        self.assertIsNotNone(factory)
        self.assertIsInstance(factory.auth, AsterPerpetualAuth)

    def test_create_order_book_data_source(self):
        """Test order book data source creation"""
        data_source = self.connector._create_order_book_data_source()
        self.assertIsNotNone(data_source)
        # Type checking would be done in integration tests

    def test_create_user_stream_data_source(self):
        """Test user stream data source creation"""
        data_source = self.connector._create_user_stream_data_source()
        self.assertIsNotNone(data_source)
        # Type checking would be done in integration tests

    # === Fee Calculation Test ===

    def test_get_fee_basic_calculation(self):
        """Test basic fee calculation (without trading rules)"""
        fee = self.connector._get_fee(
            base_currency=self.base_asset,
            quote_currency=self.quote_asset,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000")
        )

        self.assertIsNotNone(fee)
        # Basic validation that fee object was created

    # === Method implementation tests ===
    
    def test_status_polling_loop_fetch_updates(self):
        """Test status polling loop calls all update methods"""
        # Mock the update methods that actually exist in our implementation
        with patch.object(self.connector, '_update_balances', new_callable=AsyncMock) as mock_balances, \
             patch.object(self.connector, '_update_positions', new_callable=AsyncMock) as mock_positions:
            
            # Mock the methods that come from base class
            self.connector._update_order_fills_from_trades = AsyncMock()
            self.connector._update_order_status = AsyncMock()
            
            # Run the status polling method
            self.async_run_with_timeout(self.connector._status_polling_loop_fetch_updates())
            
            # Verify our implemented methods were called
            mock_balances.assert_called_once()
            mock_positions.assert_called_once()

    def test_format_trading_rules_basic(self):
        """Test trading rules formatting with basic exchange info"""
        exchange_info = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING"
                }
            ]
        }
        
        # Mock trading pair conversion
        async def mock_trading_pair_conversion(symbol):
            return "BTC-USDT"
        self.connector.trading_pair_associated_to_exchange_symbol = mock_trading_pair_conversion
        
        result = self.async_run_with_timeout(self.connector._format_trading_rules(exchange_info))
        
        # Should return trading rules dict with one entry
        self.assertEqual(len(result), 1)
        self.assertIn("BTCUSDT", result)
        trading_rule = result["BTCUSDT"]
        self.assertEqual(trading_rule.trading_pair, "BTC-USDT")

    def test_update_balances_basic_structure(self):
        """Test update balances method structure"""
        # Mock the API call
        mock_account_info = {
            "assets": [
                {
                    "asset": "USDT",
                    "availableBalance": "1000.0",
                    "walletBalance": "1500.0"
                }
            ]
        }
        
        with patch.object(self.connector, '_api_get', new_callable=AsyncMock, return_value=mock_account_info):
            # This should not raise an exception
            self.async_run_with_timeout(self.connector._update_balances())

    def test_update_positions_basic_structure(self):
        """Test update positions method structure"""
        # Mock the API call
        mock_position_info = [
            {
                "symbol": "BTCUSDT",
                "positionSide": "LONG",
                "unRealizedProfit": "100.5",
                "entryPrice": "50000.0",
                "positionAmt": "0.001"
            }
        ]
        
        # Mock dependencies
        async def mock_trading_pair_conversion(symbol):
            return "BTC-USDT"
        
        self.connector.trading_pair_associated_to_exchange_symbol = mock_trading_pair_conversion
        self.connector._perpetual_trading = MagicMock()
        self.connector._perpetual_trading.get_position.return_value = None  # No existing position
        
        with patch.object(self.connector, '_api_get', new_callable=AsyncMock, return_value=mock_position_info):
            # This should not raise an exception
            self.async_run_with_timeout(self.connector._update_positions())

    def test_user_stream_event_listener_basic_structure(self):
        """Test user stream event listener method structure"""
        # Mock the iter_user_event_queue to return empty
        async def mock_iter_empty():
            return
            yield  # unreachable
        
        with patch.object(self.connector, '_iter_user_event_queue', return_value=mock_iter_empty()):
            # This should not raise an exception
            self.async_run_with_timeout(self.connector._user_stream_event_listener())

    # === API method tests with basic mocking ===
    
    def test_place_order_parameter_construction(self):
        """Test place order parameter construction"""
        from decimal import Decimal
        from hummingbot.core.data_type.common import OrderType, TradeType, PositionAction, PositionMode
        
        # Mock the exchange symbol conversion
        async def mock_symbol_conversion(trading_pair):
            return "BTCUSDT"
        self.connector.exchange_symbol_associated_to_pair = mock_symbol_conversion
        
        # Mock the API call to avoid actual network calls
        mock_order_response = {
            "orderId": "12345",
            "updateTime": 1640001112223
        }
        
        # Test basic limit order parameters
        with patch.object(self.connector, '_api_post', new_callable=AsyncMock, return_value=mock_order_response):
            result = self.async_run_with_timeout(
                self.connector._place_order(
                    order_id="test_order_123",
                    trading_pair="BTC-USDT", 
                    amount=Decimal("0.001"),
                    trade_type=TradeType.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal("50000")
                )
            )
        
        # Verify return format
        order_id, timestamp = result
        self.assertEqual(order_id, "12345")
        self.assertIsInstance(timestamp, float)

    def test_place_cancel_parameter_construction(self):
        """Test place cancel parameter construction"""
        # Create mock tracked order
        mock_order = MagicMock()
        mock_order.trading_pair = "BTC-USDT"
        
        # Mock the exchange symbol conversion
        async def mock_symbol_conversion(trading_pair):
            return "BTCUSDT"
        self.connector.exchange_symbol_associated_to_pair = mock_symbol_conversion
        
        # Mock successful cancellation response
        mock_cancel_response = {"status": "CANCELED"}
        
        with patch.object(self.connector, '_api_delete', new_callable=AsyncMock, return_value=mock_cancel_response):
            result = self.async_run_with_timeout(
                self.connector._place_cancel("test_order_123", mock_order)
            )
        
        # Should return True for successful cancellation
        self.assertTrue(result)

    def test_set_trading_pair_leverage_successful(self):
        """Test leverage setting with successful response"""
        # Mock the exchange symbol conversion
        async def mock_symbol_conversion(trading_pair):
            return "BTCUSDT"
        self.connector.exchange_symbol_associated_to_pair = mock_symbol_conversion
        
        # Mock successful response
        mock_response = {"leverage": 10}
        
        with patch.object(self.connector, '_api_post', new_callable=AsyncMock, return_value=mock_response):
            success, error_msg = self.async_run_with_timeout(
                self.connector._set_trading_pair_leverage("BTC-USDT", 10)
            )
        
        self.assertTrue(success)
        self.assertEqual(error_msg, "")

    def test_set_trading_pair_leverage_failure(self):
        """Test leverage setting with failure"""
        # Mock the exchange symbol conversion
        async def mock_symbol_conversion(trading_pair):
            return "BTCUSDT"
        self.connector.exchange_symbol_associated_to_pair = mock_symbol_conversion
        
        # Mock API failure
        with patch.object(self.connector, '_api_post', side_effect=Exception("API Error")):
            success, error_msg = self.async_run_with_timeout(
                self.connector._set_trading_pair_leverage("BTC-USDT", 10)
            )
        
        self.assertFalse(success)
        self.assertEqual(error_msg, "API Error")

    def test_fetch_last_fee_payment_successful(self):
        """Test fetching last fee payment with successful response"""
        # Mock the exchange symbol conversion
        async def mock_symbol_conversion(trading_pair):
            return "BTCUSDT"
        self.connector.exchange_symbol_associated_to_pair = mock_symbol_conversion
        
        # Mock successful response
        mock_response = [{
            "time": 1640001112000,
            "income": "-0.001",
            "asset": "USDT"
        }]
        
        with patch.object(self.connector, '_api_get', new_callable=AsyncMock, return_value=mock_response):
            timestamp, income, asset = self.async_run_with_timeout(
                self.connector._fetch_last_fee_payment("BTC-USDT")
            )
        
        self.assertEqual(timestamp, 1640001112.0)
        self.assertEqual(income, Decimal("-0.001"))
        self.assertEqual(asset, "USDT")

    def test_fetch_last_fee_payment_no_data(self):
        """Test fetching last fee payment with no data"""
        # Mock the exchange symbol conversion
        async def mock_symbol_conversion(trading_pair):
            return "BTCUSDT"
        self.connector.exchange_symbol_associated_to_pair = mock_symbol_conversion
        
        # Mock empty response
        with patch.object(self.connector, '_api_get', new_callable=AsyncMock, return_value=[]):
            timestamp, income, asset = self.async_run_with_timeout(
                self.connector._fetch_last_fee_payment("BTC-USDT")
            )
        
        # Should return default values
        self.assertEqual(timestamp, 0)
        self.assertEqual(income, Decimal("-1"))
        self.assertEqual(asset, "-1")

    def test_trading_pair_position_mode_set_oneway(self):
        """Test setting position mode to ONEWAY"""
        from hummingbot.core.data_type.common import PositionMode
        
        mock_response = {"msg": "success"}
        
        with patch.object(self.connector, '_api_post', new_callable=AsyncMock, return_value=mock_response):
            success, error_msg = self.async_run_with_timeout(
                self.connector._trading_pair_position_mode_set(PositionMode.ONEWAY, "BTC-USDT")
            )
        
        self.assertTrue(success)
        self.assertEqual(error_msg, "")

    def test_trading_pair_position_mode_set_hedge(self):
        """Test setting position mode to HEDGE"""
        from hummingbot.core.data_type.common import PositionMode
        
        mock_response = {"msg": "success"}
        
        with patch.object(self.connector, '_api_post', new_callable=AsyncMock, return_value=mock_response):
            success, error_msg = self.async_run_with_timeout(
                self.connector._trading_pair_position_mode_set(PositionMode.HEDGE, "BTC-USDT")
            )
        
        self.assertTrue(success)
        self.assertEqual(error_msg, "")


if __name__ == "__main__":
    unittest.main()
