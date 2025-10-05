import asyncio
import unittest
from decimal import Decimal
from typing import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import LighterPerpetualDerivative
import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee


class LighterPerpetualDerivativeTests(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.ev_loop = asyncio.get_event_loop()
        
        self.private_key = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        self.account_index = 1
        self.api_key_index = 0
        self.trading_pairs = ["ETH-USDC", "BTC-USDC"]
        
        # Create client config
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())
        
        # Create connector
        self.connector = LighterPerpetualDerivative(
            client_config_map=self.client_config_map,
            lighter_perpetual_private_key=self.private_key,
            lighter_perpetual_account_index=self.account_index,
            lighter_perpetual_api_key_index=self.api_key_index,
            trading_pairs=self.trading_pairs,
            trading_required=True,
            domain=CONSTANTS.DEFAULT_DOMAIN
        )

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_initialization(self):
        """Test connector initialization"""
        self.assertEqual(self.connector._private_key, self.private_key)
        self.assertEqual(self.connector._account_index, self.account_index)
        self.assertEqual(self.connector._api_key_index, self.api_key_index)
        self.assertEqual(self.connector._domain, CONSTANTS.DEFAULT_DOMAIN)
        self.assertEqual(self.connector._trading_pairs, self.trading_pairs)
        self.assertTrue(self.connector._trading_required)

    def test_name_property(self):
        """Test name property"""
        self.assertEqual(self.connector.name, CONSTANTS.DEFAULT_DOMAIN)

    def test_authenticator_property(self):
        """Test authenticator property"""
        auth = self.connector.authenticator
        self.assertEqual(auth.account_index, self.account_index)
        self.assertEqual(auth.api_key_index, self.api_key_index)

    def test_rate_limits_rules_property(self):
        """Test rate limits rules property"""
        rules = self.connector.rate_limits_rules
        self.assertEqual(rules, CONSTANTS.RATE_LIMITS)

    def test_domain_property(self):
        """Test domain property"""
        self.assertEqual(self.connector.domain, CONSTANTS.DEFAULT_DOMAIN)

    def test_client_order_id_max_length_property(self):
        """Test client order ID max length property"""
        self.assertEqual(self.connector.client_order_id_max_length, CONSTANTS.MAX_ORDER_ID_LEN)

    def test_client_order_id_prefix_property(self):
        """Test client order ID prefix property"""
        self.assertEqual(self.connector.client_order_id_prefix, CONSTANTS.BROKER_ID)

    def test_trading_rules_request_path_property(self):
        """Test trading rules request path property"""
        self.assertEqual(self.connector.trading_rules_request_path, CONSTANTS.ORDER_BOOK_DETAILS_PATH_URL)

    def test_trading_pairs_request_path_property(self):
        """Test trading pairs request path property"""
        self.assertEqual(self.connector.trading_pairs_request_path, CONSTANTS.ORDER_BOOKS_PATH_URL)

    def test_check_network_request_path_property(self):
        """Test check network request path property"""
        self.assertEqual(self.connector.check_network_request_path, CONSTANTS.INFO_PATH_URL)

    def test_trading_pairs_property(self):
        """Test trading pairs property"""
        self.assertEqual(self.connector.trading_pairs, self.trading_pairs)

    def test_is_cancel_request_in_exchange_synchronous_property(self):
        """Test is cancel request synchronous property"""
        self.assertTrue(self.connector.is_cancel_request_in_exchange_synchronous)

    def test_is_trading_required_property(self):
        """Test is trading required property"""
        self.assertTrue(self.connector.is_trading_required)

    def test_funding_fee_poll_interval_property(self):
        """Test funding fee poll interval property"""
        self.assertEqual(self.connector.funding_fee_poll_interval, 120)

    def test_supported_order_types(self):
        """Test supported order types"""
        expected_types = [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]
        self.assertEqual(self.connector.supported_order_types(), expected_types)

    def test_supported_position_modes(self):
        """Test supported position modes"""
        expected_modes = [PositionMode.ONEWAY]
        self.assertEqual(self.connector.supported_position_modes(), expected_modes)

    def test_get_buy_collateral_token(self):
        """Test get buy collateral token"""
        # Mock trading rule
        mock_rule = MagicMock()
        mock_rule.buy_order_collateral_token = "USDC"
        self.connector._trading_rules["ETH-USDC"] = mock_rule
        
        result = self.connector.get_buy_collateral_token("ETH-USDC")
        self.assertEqual(result, "USDC")

    def test_get_sell_collateral_token(self):
        """Test get sell collateral token"""
        # Mock trading rule
        mock_rule = MagicMock()
        mock_rule.sell_order_collateral_token = "USDC"
        self.connector._trading_rules["ETH-USDC"] = mock_rule
        
        result = self.connector.get_sell_collateral_token("ETH-USDC")
        self.assertEqual(result, "USDC")

    def test_is_request_exception_related_to_time_synchronizer(self):
        """Test time synchronizer exception check"""
        exception = Exception("Test exception")
        result = self.connector._is_request_exception_related_to_time_synchronizer(exception)
        self.assertFalse(result)

    def test_create_web_assistants_factory(self):
        """Test web assistants factory creation"""
        factory = self.connector._create_web_assistants_factory()
        self.assertIsNotNone(factory)

    def test_create_order_book_data_source(self):
        """Test order book data source creation"""
        data_source = self.connector._create_order_book_data_source()
        self.assertIsNotNone(data_source)
        self.assertEqual(data_source._trading_pairs, self.trading_pairs)
        self.assertEqual(data_source._connector, self.connector)

    def test_create_user_stream_data_source(self):
        """Test user stream data source creation"""
        data_source = self.connector._create_user_stream_data_source()
        self.assertIsNotNone(data_source)
        self.assertEqual(data_source._trading_pairs, self.trading_pairs)
        self.assertEqual(data_source._connector, self.connector)
        self.assertEqual(data_source._auth, self.connector._auth)

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative.LighterPerpetualDerivative._api_get")
    def test_make_network_check_request(self, mock_api_get):
        """Test network check request"""
        mock_api_get.return_value = {"status": "ok"}
        
        self.async_run_with_timeout(self.connector._make_network_check_request())
        
        mock_api_get.assert_called_once_with(path_url=CONSTANTS.INFO_PATH_URL)

    def test_place_order_not_implemented(self):
        """Test that place order raises NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.async_run_with_timeout(
                self.connector._place_order(
                    order_id="test_order",
                    trading_pair="ETH-USDC",
                    amount=Decimal("1.0"),
                    trade_type=TradeType.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal("3000.0")
                )
            )

    def test_place_cancel_not_implemented(self):
        """Test that place cancel raises NotImplementedError"""
        mock_order = MagicMock(spec=InFlightOrder)
        
        with self.assertRaises(NotImplementedError):
            self.async_run_with_timeout(
                self.connector._place_cancel("test_order", mock_order)
            )

    def test_trading_pair_position_mode_set_oneway(self):
        """Test setting ONEWAY position mode"""
        result = self.async_run_with_timeout(
            self.connector._trading_pair_position_mode_set(PositionMode.ONEWAY, "ETH-USDC")
        )
        
        success, message = result
        self.assertTrue(success)
        self.assertEqual(message, "")

    def test_trading_pair_position_mode_set_hedge(self):
        """Test setting HEDGE position mode (not supported)"""
        result = self.async_run_with_timeout(
            self.connector._trading_pair_position_mode_set(PositionMode.HEDGE, "ETH-USDC")
        )
        
        success, message = result
        self.assertFalse(success)
        self.assertIn("not supported", message)

    def test_set_trading_pair_leverage(self):
        """Test setting trading pair leverage"""
        result = self.async_run_with_timeout(
            self.connector._set_trading_pair_leverage("ETH-USDC", 10)
        )
        
        success, message = result
        self.assertTrue(success)
        self.assertEqual(message, "")

    def test_fetch_last_fee_payment(self):
        """Test fetching last fee payment"""
        result = self.async_run_with_timeout(
            self.connector._fetch_last_fee_payment("ETH-USDC")
        )
        
        timestamp, rate, amount = result
        self.assertEqual(timestamp, 0.0)
        self.assertEqual(rate, Decimal("0"))
        self.assertEqual(amount, Decimal("0"))

    def test_get_fee_maker(self):
        """Test fee calculation for maker orders"""
        fee = self.connector._get_fee(
            base_currency="ETH",
            quote_currency="USDC",
            order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY,
            position_action=PositionAction.OPEN,
            amount=Decimal("1.0"),
            price=Decimal("3000.0"),
            is_maker=True
        )
        
        self.assertIsInstance(fee, DeductedFromReturnsTradeFee)
        self.assertEqual(fee.percent, Decimal("0.0005"))  # 0.05% maker fee

    def test_get_fee_taker(self):
        """Test fee calculation for taker orders"""
        fee = self.connector._get_fee(
            base_currency="ETH",
            quote_currency="USDC",
            order_type=OrderType.MARKET,
            order_side=TradeType.BUY,
            position_action=PositionAction.OPEN,
            amount=Decimal("1.0"),
            price=Decimal("3000.0"),
            is_maker=False
        )
        
        self.assertIsInstance(fee, DeductedFromReturnsTradeFee)
        self.assertEqual(fee.percent, Decimal("0.001"))  # 0.1% taker fee

    def test_get_market_id_for_trading_pair(self):
        """Test getting market ID for trading pair"""
        # This tests the helper method that uses web_utils
        market_id = self.connector._get_market_id_for_trading_pair("ETH-USDC")
        self.assertIsInstance(market_id, int)

    def test_get_trading_pair_for_market_id(self):
        """Test getting trading pair for market ID"""
        # This tests the helper method that uses web_utils
        trading_pair = self.connector._get_trading_pair_for_market_id(0)
        self.assertIsInstance(trading_pair, str)

    def test_account_index_property(self):
        """Test account index property"""
        self.assertEqual(self.connector.account_index, self.account_index)

    def test_get_connection_health(self):
        """Test get connection health method"""
        health = self.connector.get_connection_health()
        
        # Basic keys that should always be present
        expected_basic_keys = {
            "connector_name", "account_index", "domain", "trading_required",
            "trading_pairs_count", "positions_count"
        }
        self.assertTrue(expected_basic_keys.issubset(set(health.keys())))
        
        self.assertEqual(health["connector_name"], CONSTANTS.DEFAULT_DOMAIN)
        self.assertEqual(health["account_index"], self.account_index)
        self.assertEqual(health["domain"], CONSTANTS.DEFAULT_DOMAIN)
        self.assertTrue(health["trading_required"])
        self.assertEqual(health["trading_pairs_count"], len(self.trading_pairs))
        self.assertEqual(health["positions_count"], 0)
        
        # user_stream_health is optional and depends on _user_stream_tracker state
        if "user_stream_health" in health:
            self.assertIsNotNone(health["user_stream_health"])

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative.LighterPerpetualDerivative._api_get")
    def test_initialize_trading_pair_symbol_map(self, mock_api_get):
        """Test initializing trading pair symbol map"""
        mock_api_get.return_value = {"markets": []}
        
        # Should not raise exception
        self.async_run_with_timeout(self.connector._initialize_trading_pair_symbol_map())
        
        mock_api_get.assert_called_once_with(path_url=CONSTANTS.ORDER_BOOKS_PATH_URL)

    def test_update_trading_rules(self):
        """Test updating trading rules"""
        # Should not raise exception
        self.async_run_with_timeout(self.connector._update_trading_rules())

    def test_update_order_status(self):
        """Test updating order status"""
        # Should not raise exception (method is placeholder)
        self.async_run_with_timeout(self.connector._update_order_status())

    def test_update_lost_orders_status(self):
        """Test updating lost orders status"""
        # Should not raise exception (method is placeholder)
        self.async_run_with_timeout(self.connector._update_lost_orders_status())

    def test_update_positions(self):
        """Test updating positions"""
        # Should not raise exception (method is placeholder)
        self.async_run_with_timeout(self.connector._update_positions())

    def test_constants_access(self):
        """Test that constants are accessible"""
        self.assertIsNotNone(CONSTANTS.DEFAULT_DOMAIN)
        self.assertIsNotNone(CONSTANTS.RATE_LIMITS)
        self.assertIsNotNone(CONSTANTS.MAX_ORDER_ID_LEN)
        self.assertIsNotNone(CONSTANTS.BROKER_ID)
