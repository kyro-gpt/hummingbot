"""
Unit tests for BackpackPerpetualDerivative

Tests the main derivative connector class with position management,
funding fee collection, and enhanced order management features.
"""
import asyncio
import json
import time
import unittest
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount


class BackpackPerpetualDerivativeTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.api_key = "test_api_key"
        self.secret_key = "dGVzdF9zZWNyZXRfa2V5XzEyMzQ1Njc4OTBhYmNkZWY="  # Valid base64 test secret
        self.trading_pairs = ["SOL-USDC", "ETH-USDT"]

        # Create mock client config
        self.client_config_map = MagicMock()

        # Mock the auth to avoid Ed25519 key validation issues in tests
        with patch('hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative.BackpackPerpetualAuth') as mock_auth_class, \
             patch.object(BackpackPerpetualDerivative, 'rate_limits_rules', return_value=[]):

            mock_auth = MagicMock()
            mock_auth_class.return_value = mock_auth

            # Create connector with mocked config
            self.connector = BackpackPerpetualDerivative(
                client_config_map=self.client_config_map,
                backpack_perpetual_api_key=self.api_key,
                backpack_perpetual_secret_key=self.secret_key,
                trading_pairs=self.trading_pairs,
                trading_required=True,
                domain=CONSTANTS.DEFAULT_DOMAIN,
            )

            # Mock essential data structures to avoid initialization issues
            self.connector._account_balances = {}
            self.connector._account_available_balances = {}
            self.connector._trading_fees = {}
            self.connector._trading_rules = {}
            
            # Mock trading pair symbol mapping (critical for derivative tests)
            # Base class expects {exchange_symbol: trading_pair} format
            from bidict import bidict
            self.connector._trading_pair_symbol_map = bidict({
                "SOL_USDC_PERP": "SOL-USDC",
                "BTC_USDC_PERP": "BTC-USDC", 
                "ETH_USDC_PERP": "ETH-USDC"
            })
            
            # Also mock the base class symbol map to avoid the KeyError
            self.connector._set_trading_pair_symbol_map(self.connector._trading_pair_symbol_map)

            # Mock web assistants factory
            self.mock_web_factory = MagicMock()
            self.mock_rest_assistant = AsyncMock()
            self.mock_web_factory.get_rest_assistant = AsyncMock(return_value=self.mock_rest_assistant)
            self.connector._web_assistants_factory = self.mock_web_factory

            # Store the mock auth for test access
            self.mock_auth = mock_auth

    def configure_rest_response(self, response_data):
        """Helper method to configure mock REST API responses"""
        self.mock_rest_assistant.execute_request = AsyncMock(return_value=response_data)

    def test_supported_order_types(self):
        """Test supported order types"""
        supported_types = self.connector.supported_order_types()
        expected_types = [OrderType.LIMIT, OrderType.MARKET]
        self.assertEqual(expected_types, supported_types)

    def test_supported_position_modes(self):
        """Test supported position modes"""
        supported_modes = self.connector.supported_position_modes()
        expected_modes = [PositionMode.ONEWAY]
        self.assertEqual(expected_modes, supported_modes)

    def test_get_buy_collateral_token(self):
        """Test buy collateral token identification"""
        # Test with known trading pair
        collateral = self.connector.get_buy_collateral_token("SOL-USDC")
        self.assertEqual("USDC", collateral)

        # Test with unknown trading pair (should extract from pair)
        collateral = self.connector.get_buy_collateral_token("BTC-USD")
        self.assertEqual("USD", collateral)

    def test_get_sell_collateral_token(self):
        """Test sell collateral token identification"""
        # Test with known trading pair
        collateral = self.connector.get_sell_collateral_token("SOL-USDC")
        self.assertEqual("USDC", collateral)

        # Test with unknown trading pair (should extract from pair)
        collateral = self.connector.get_sell_collateral_token("ETH-USDT")
        self.assertEqual("USDT", collateral)

    def test_funding_fee_poll_interval(self):
        """Test funding fee poll interval"""
        interval = self.connector.funding_fee_poll_interval
        self.assertEqual(CONSTANTS.FUNDING_FEE_POLL_INTERVAL, interval)
        self.assertEqual(300, interval)  # 5 minutes

    def test_connector_properties(self):
        """Test basic connector properties"""
        self.assertEqual(CONSTANTS.EXCHANGE_NAME, self.connector.name)
        self.assertEqual(CONSTANTS.DEFAULT_DOMAIN, self.connector.domain)
        self.assertEqual(CONSTANTS.MAX_ORDER_ID_LEN, self.connector.client_order_id_max_length)
        self.assertEqual(CONSTANTS.HBOT_ORDER_ID_PREFIX, self.connector.client_order_id_prefix)
        self.assertTrue(self.connector.is_cancel_request_in_exchange_synchronous)
        self.assertTrue(self.connector.is_trading_required)

    def test_client_order_id_generation(self):
        """Test client order ID generation"""
        # Test that the same order ID generates the same client ID
        order_id = "test_order_123"
        client_id_1 = self.connector._generate_client_order_id(order_id)
        client_id_2 = self.connector._generate_client_order_id(order_id)

        self.assertEqual(client_id_1, client_id_2)
        self.assertIsInstance(client_id_1, int)

        # Test that different order IDs generate different client IDs
        different_order_id = "test_order_456"
        different_client_id = self.connector._generate_client_order_id(different_order_id)
        self.assertNotEqual(client_id_1, different_client_id)

    @aioresponses()
    async def test_update_positions_successful(self, mock_api):
        """Test successful position update from REST API"""
        # Mock position API response
        position_data = [
            {
                "symbol": "SOL_USDC_PERP",
                "netQuantity": "0.5",
                "entryPrice": "150.00",
                "breakEvenPrice": "150.50",
                "pnlUnrealized": "2.50",
                "pnlRealized": "0.00",
                "markPrice": "155.00",
                "estLiquidationPrice": "100.00",
                "imf": "0.02",
                "mmf": "0.0125"
            },
            {
                "symbol": "ETH_USDT_PERP",
                "netQuantity": "-0.1",
                "entryPrice": "3000.00",
                "breakEvenPrice": "2999.00",
                "pnlUnrealized": "-5.00",
                "pnlRealized": "10.00",
                "markPrice": "2950.00",
                "estLiquidationPrice": "3500.00",
                "imf": "0.03",
                "mmf": "0.015"
            }
        ]

        # Mock the API response
        mock_api.get(
            f"{CONSTANTS.REST_URL}{CONSTANTS.POSITION_PATH_URL}",
            payload=position_data
        )

        # Configure the mock rest assistant
        self.configure_rest_response(position_data)

        # Test position update
        await self.connector._update_positions()

        # Verify positions were set
        sol_position = self.connector.get_position("SOL-USDC")
        self.assertIsNotNone(sol_position)
        self.assertEqual(PositionSide.LONG, sol_position.position_side)
        self.assertEqual(Decimal("0.5"), sol_position.amount)
        self.assertEqual(Decimal("150.00"), sol_position.entry_price)
        self.assertEqual(Decimal("2.50"), sol_position.unrealized_pnl)

        eth_position = self.connector.get_position("ETH-USDT")
        self.assertIsNotNone(eth_position)
        self.assertEqual(PositionSide.SHORT, eth_position.position_side)
        self.assertEqual(Decimal("0.1"), eth_position.amount)
        self.assertEqual(Decimal("3000.00"), eth_position.entry_price)
        self.assertEqual(Decimal("-5.00"), eth_position.unrealized_pnl)

    @aioresponses()
    async def test_update_positions_empty_response(self, mock_api):
        """Test position update with empty response"""
        # Mock empty API response
        mock_api.get(
            f"{CONSTANTS.REST_URL}{CONSTANTS.POSITION_PATH_URL}",
            payload=[]
        )

        # Configure the mock rest assistant
        self.configure_rest_response([])

        # Test position update
        await self.connector._update_positions()

        # Verify no positions were set
        sol_position = self.connector.get_position("SOL-USDC")
        self.assertIsNone(sol_position)

    @aioresponses()
    async def test_fetch_last_fee_payment_successful(self, mock_api):
        """Test successful funding fee payment retrieval"""
        # Mock funding payment API response
        funding_data = [
            {
                "intervalEndTimestamp": "1640995200000",
                "fundingRate": "0.0001",
                "quantity": "0.5"
            }
        ]

        # Mock the API response
        mock_api.get(
            f"{CONSTANTS.REST_URL}{CONSTANTS.HISTORY_FUNDING_PATH_URL}",
            payload=funding_data
        )

        # Configure the mock rest assistant
        self.configure_rest_response(funding_data)

        # Test funding payment retrieval
        timestamp, funding_rate, payment_amount = await self.connector._fetch_last_fee_payment("SOL-USDC")

        # Verify results
        self.assertEqual(1640995200000, timestamp)
        self.assertEqual(Decimal("0.0001"), funding_rate)
        self.assertEqual(Decimal("0.5"), payment_amount)

    @aioresponses()
    async def test_fetch_last_fee_payment_no_payments(self, mock_api):
        """Test funding fee payment retrieval with no payments"""
        # Mock empty API response
        mock_api.get(
            f"{CONSTANTS.REST_URL}{CONSTANTS.HISTORY_FUNDING_PATH_URL}",
            payload=[]
        )

        # Configure the mock rest assistant
        self.configure_rest_response([])

        # Test funding payment retrieval
        timestamp, funding_rate, payment_amount = await self.connector._fetch_last_fee_payment("SOL-USDC")

        # Verify results
        self.assertEqual(0, timestamp)
        self.assertEqual(Decimal("0"), funding_rate)
        self.assertEqual(Decimal("0"), payment_amount)

    @aioresponses()
    async def test_place_order_successful(self, mock_api):
        """Test successful order placement"""
        # Mock order placement API response
        order_response = {
            "id": "5012875717",
            "clientId": 2115950059,
            "symbol": "SOL_USDC_PERP",
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "0.5",
            "price": "150.00",
            "status": "New"
        }

        # Mock the API response
        mock_api.post(
            f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_PATH_URL}",
            payload=order_response
        )

        # Configure the mock rest assistant
        self.configure_rest_response(order_response)

        # Test order placement
        exchange_order_id, timestamp = await self.connector._place_order(
            order_id="test_order_123",
            trading_pair="SOL-USDC",
            amount=Decimal("0.5"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("150.00"),
            position_action=PositionAction.OPEN
        )

        # Verify results
        self.assertEqual("5012875717", exchange_order_id)
        self.assertIsInstance(timestamp, float)

    @aioresponses()
    async def test_place_cancel_successful(self, mock_api):
        """Test successful order cancellation"""
        # Create tracked order
        tracked_order = InFlightOrder(
            client_order_id="test_order_123",
            exchange_order_id="5012875717",
            trading_pair="SOL-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("0.5"),
            price=Decimal("150.00"),
            creation_timestamp=time.time()
        )

        # Mock cancellation API response
        mock_api.delete(
            f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_PATH_URL}",
            payload={"success": True}
        )

        # Configure the mock rest assistant
        self.configure_rest_response({"success": True})

        # Test order cancellation
        success = await self.connector._place_cancel("test_order_123", tracked_order)

        # Verify success
        self.assertTrue(success)

    @aioresponses()
    async def test_request_order_status_successful(self, mock_api):
        """Test successful order status request"""
        # Create tracked order
        tracked_order = InFlightOrder(
            client_order_id="test_order_123",
            exchange_order_id="5012875717",
            trading_pair="SOL-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("0.5"),
            price=Decimal("150.00"),
            creation_timestamp=time.time()
        )

        # Mock order status API response
        order_status_response = {
            "id": "5012875717",
            "clientId": 2115950059,
            "symbol": "SOL_USDC_PERP",
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "0.5",
            "price": "150.00",
            "status": "Filled"
        }

        # Mock the API response
        mock_api.get(
            f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_PATH_URL}",
            payload=order_status_response
        )

        # Configure the mock rest assistant
        self.configure_rest_response(order_status_response)

        # Test order status request
        order_update = await self.connector._request_order_status(tracked_order)

        # Verify results
        self.assertEqual("test_order_123", order_update.client_order_id)
        self.assertEqual("5012875717", order_update.exchange_order_id)
        self.assertEqual(OrderState.FILLED, order_update.new_state)
        self.assertEqual("SOL-USDC", order_update.trading_pair)

    @aioresponses()
    async def test_request_order_status_not_found(self, mock_api):
        """Test order status request with 404 (order not found)"""
        # Create tracked order
        tracked_order = InFlightOrder(
            client_order_id="test_order_123",
            exchange_order_id="5012875717",
            trading_pair="SOL-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("0.5"),
            price=Decimal("150.00"),
            creation_timestamp=time.time()
        )

        # Mock 404 response
        mock_api.get(
            f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_PATH_URL}",
            status=404
        )

        # Configure the mock rest assistant to raise 404 exception
        from aiohttp import ClientResponseError
        from aiohttp.client_reqrep import RequestInfo
        request_info = MagicMock(spec=RequestInfo)
        request_info.url = f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_PATH_URL}"
        exception = ClientResponseError(request_info=request_info, history=(), status=404, message="Not found")
        self.mock_rest_assistant.execute_request = AsyncMock(side_effect=exception)

        # Test order status request
        order_update = await self.connector._request_order_status(tracked_order)

        # Verify results (should assume cancelled for 404)
        self.assertEqual("test_order_123", order_update.client_order_id)
        self.assertEqual("5012875717", order_update.exchange_order_id)
        self.assertEqual(OrderState.CANCELED, order_update.new_state)

    async def test_process_position_event_long_position(self):
        """Test processing of long position event"""
        # Create position event data
        position_event = {
            "B": "150.00",           # Entry price
            "E": 1754462601563677,   # Event time
            "M": "155.00",           # Mark price
            "P": "2.50",             # PnL unrealized
            "Q": "0.5",              # Net exposure quantity
            "T": 1754462601563678,   # Engine timestamp
            "b": "150.50",           # Break even price
            "f": "0.02",             # Initial margin fraction
            "i": 4681711685,         # Position ID
            "l": "100.00",           # Liquidation price
            "m": "0.0125",           # Maintenance margin fraction
            "n": "77.50",            # Net exposure notional
            "p": "0",                # PnL realized
            "q": "0.5",              # Net quantity (positive = long)
            "s": "SOL_USDC_PERP"     # Symbol
        }

        # Process position event
        await self.connector._process_position_event(position_event)

        # Verify position was set
        position = self.connector.get_position("SOL-USDC")
        self.assertIsNotNone(position)
        self.assertEqual(PositionSide.LONG, position.position_side)
        self.assertEqual(Decimal("0.5"), position.amount)
        self.assertEqual(Decimal("150.00"), position.entry_price)
        self.assertEqual(Decimal("2.50"), position.unrealized_pnl)

    async def test_process_position_event_short_position(self):
        """Test processing of short position event"""
        # Create position event data
        position_event = {
            "B": "3000.00",          # Entry price
            "E": 1754462601563677,   # Event time
            "M": "2950.00",          # Mark price
            "P": "-5.00",            # PnL unrealized
            "Q": "-0.1",             # Net exposure quantity
            "T": 1754462601563678,   # Engine timestamp
            "b": "2999.00",          # Break even price
            "f": "0.03",             # Initial margin fraction
            "i": 4681711686,         # Position ID
            "l": "3500.00",          # Liquidation price
            "m": "0.015",            # Maintenance margin fraction
            "n": "-295.00",          # Net exposure notional
            "p": "10.00",            # PnL realized
            "q": "-0.1",             # Net quantity (negative = short)
            "s": "ETH_USDT_PERP"     # Symbol
        }

        # Process position event
        await self.connector._process_position_event(position_event)

        # Verify position was set
        position = self.connector.get_position("ETH-USDT")
        self.assertIsNotNone(position)
        self.assertEqual(PositionSide.SHORT, position.position_side)
        self.assertEqual(Decimal("0.1"), position.amount)  # Absolute value
        self.assertEqual(Decimal("3000.00"), position.entry_price)
        self.assertEqual(Decimal("-5.00"), position.unrealized_pnl)

    async def test_process_position_event_closed_position(self):
        """Test processing of closed position event"""
        # First set a position
        await self.test_process_position_event_long_position()

        # Verify position exists
        position = self.connector.get_position("SOL-USDC")
        self.assertIsNotNone(position)

        # Create position close event (quantity = 0)
        position_close_event = {
            "B": "0",                # Entry price
            "E": 1754462601563677,   # Event time
            "M": "155.00",           # Mark price
            "P": "0",                # PnL unrealized
            "Q": "0",                # Net exposure quantity
            "T": 1754462601563678,   # Engine timestamp
            "b": "0",                # Break even price
            "f": "0",                # Initial margin fraction
            "i": 4681711685,         # Position ID
            "l": "0",                # Liquidation price
            "m": "0",                # Maintenance margin fraction
            "n": "0",                # Net exposure notional
            "p": "2.50",             # PnL realized
            "q": "0",                # Net quantity (0 = closed)
            "s": "SOL_USDC_PERP"     # Symbol
        }

        # Process position close event
        await self.connector._process_position_event(position_close_event)

        # Verify position was removed
        position = self.connector.get_position("SOL-USDC")
        self.assertIsNone(position)

    async def test_set_trading_pair_leverage_placeholder(self):
        """Test leverage setting (placeholder implementation)"""
        # Test leverage setting - should return (True, message) (not implemented but no error)
        success, message = await self.connector._set_trading_pair_leverage("SOL-USDC", 10)
        self.assertTrue(success)
        self.assertIn("not yet implemented", message)
        self.assertIn("10", message)  # Should include requested leverage

    async def test_trading_pair_position_mode_set_placeholder(self):
        """Test position mode setting (placeholder implementation)"""
        # Test position mode setting - should return (True, message) (not implemented but no error)
        success, message = await self.connector._trading_pair_position_mode_set(PositionMode.HEDGE, "SOL-USDC")
        self.assertTrue(success)
        self.assertIn("not yet implemented", message)
        self.assertIn("HEDGE", message)  # Should include requested mode

    @aioresponses()
    async def test_update_balances_successful(self, mock_api):
        """Test successful balance update"""
        # Mock balance API response
        balance_data = {
            "balances": [
                {
                    "asset": "USDC",
                    "total": "1000.50",
                    "available": "900.25"
                },
                {
                    "asset": "SOL",
                    "total": "10.75",
                    "available": "8.50"
                }
            ]
        }

        # Mock the API response
        mock_api.get(
            f"{CONSTANTS.REST_URL}{CONSTANTS.CAPITAL_PATH_URL}",
            payload=balance_data
        )

        # Configure the mock rest assistant
        self.configure_rest_response(balance_data)

        # Test balance update
        await self.connector._update_balances()

        # Verify balances were set
        self.assertEqual(Decimal("1000.50"), self.connector.get_balance("USDC"))
        self.assertEqual(Decimal("900.25"), self.connector.get_available_balance("USDC"))
        self.assertEqual(Decimal("10.75"), self.connector.get_balance("SOL"))
        self.assertEqual(Decimal("8.50"), self.connector.get_available_balance("SOL"))

    def test_fee_calculation(self):
        """Test fee calculation for derivative trading"""
        # Test maker fee
        maker_fee = self.connector._get_fee(
            base_currency="SOL",
            quote_currency="USDC",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("150.0"),
            is_maker=True
        )

        # Verify maker fee
        self.assertIsInstance(maker_fee, AddedToCostTradeFee)

        # Test taker fee
        taker_fee = self.connector._get_fee(
            base_currency="SOL",
            quote_currency="USDC",
            order_type=OrderType.MARKET,
            order_side=TradeType.SELL,
            amount=Decimal("1.0"),
            price=Decimal("150.0"),
            is_maker=False
        )

        # Verify taker fee
        self.assertIsInstance(taker_fee, AddedToCostTradeFee)

    def test_error_detection_methods(self):
        """Test error detection helper methods"""
        # Test time synchronizer error detection
        time_error = Exception("timestamp is invalid")
        self.assertTrue(self.connector._is_request_exception_related_to_time_synchronizer(time_error))

        non_time_error = Exception("invalid API key")
        self.assertFalse(self.connector._is_request_exception_related_to_time_synchronizer(non_time_error))

        # Test order not found error detection
        not_found_error = Exception("404 not found")
        self.assertTrue(self.connector._is_order_not_found_during_status_update_error(not_found_error))
        self.assertTrue(self.connector._is_order_not_found_during_cancelation_error(not_found_error))

        other_error = Exception("500 internal server error")
        self.assertFalse(self.connector._is_order_not_found_during_status_update_error(other_error))
        self.assertFalse(self.connector._is_order_not_found_during_cancelation_error(other_error))


if __name__ == "__main__":
    unittest.main()
