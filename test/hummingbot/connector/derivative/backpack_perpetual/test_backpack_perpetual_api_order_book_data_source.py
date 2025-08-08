import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_order_book_data_source import (
    BackpackPerpetualAPIOrderBookDataSource,
)
from hummingbot.core.data_type.funding_info import FundingInfo


class BackpackPerpetualAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.trading_pairs = ["SOL-USDC", "ETH-USDT"]  # Will map to SOL_USDC_PERP, ETH_USDT_PERP
        self.domain = CONSTANTS.DEFAULT_DOMAIN

        # Create mock connector
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock()
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock()
        self.connector.get_last_traded_prices = AsyncMock()

        # Set up symbol mapping
        self.connector.exchange_symbol_associated_to_pair.side_effect = self._get_exchange_symbol
        self.connector.trading_pair_associated_to_exchange_symbol.side_effect = self._get_trading_pair

        # Create throttler and API factory
        self.throttler = web_utils.create_throttler()
        self.api_factory = web_utils.build_api_factory(throttler=self.throttler)

        # Create data source
        self.data_source = BackpackPerpetualAPIOrderBookDataSource(
            trading_pairs=self.trading_pairs,
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain
        )

    async def _get_exchange_symbol(self, trading_pair: str) -> str:
        """Convert trading pair to exchange symbol format with _PERP suffix"""
        return trading_pair.replace("-", "_") + "_PERP"

    async def _get_trading_pair(self, symbol: str) -> str:
        """Convert exchange symbol to trading pair format, removing _PERP suffix"""
        if symbol.endswith("_PERP"):
            base_pair = symbol[:-5]  # Remove "_PERP"
            return base_pair.replace("_", "-")
        else:
            return symbol.replace("_", "-")

    async def test_get_last_traded_prices_delegates_to_connector(self):
        """Test that get_last_traded_prices properly delegates to the connector"""
        expected_prices = {"SOL-USDC": 150.0, "ETH-USDT": 3000.0}  # Updated to match new trading pairs
        self.connector.get_last_traded_prices.return_value = expected_prices

        result = await self.data_source.get_last_traded_prices(self.trading_pairs)

        self.assertEqual(expected_prices, result)
        self.connector.get_last_traded_prices.assert_called_once_with(trading_pairs=self.trading_pairs)

    async def test_get_funding_info_successful(self):
        """Test successful funding info retrieval from mark prices endpoint"""
        trading_pair = "SOL-USDC"

        # Mock the mark prices API response
        mock_response_data = [{
            "symbol": "SOL_USDC_PERP",  # Updated to include _PERP suffix
            "markPrice": "150.00",
            "indexPrice": "149.95",
            "fundingRate": "0.0001",
            "nextFundingTimestamp": "1640995200000"
        }]

        # Mock REST assistant
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value=mock_response_data)

        with patch.object(self.data_source._api_factory, 'get_rest_assistant', return_value=mock_rest_assistant):
            funding_info = await self.data_source.get_funding_info(trading_pair)

        # Verify the results
        self.assertIsInstance(funding_info, FundingInfo)
        self.assertEqual(trading_pair, funding_info.trading_pair)
        self.assertEqual(Decimal("150.00"), funding_info.mark_price)
        self.assertEqual(Decimal("149.95"), funding_info.index_price)
        self.assertEqual(Decimal("0.0001"), funding_info.rate)
        self.assertEqual(1640995200000, funding_info.next_funding_utc_timestamp)

    async def test_get_funding_info_handles_api_error(self):
        """Test that get_funding_info handles API errors gracefully"""
        trading_pair = "SOL-USDC"

        # Mock REST assistant with error
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(side_effect=Exception("API Error"))

        with patch.object(self.data_source._api_factory, 'get_rest_assistant', return_value=mock_rest_assistant):
            funding_info = await self.data_source.get_funding_info(trading_pair)

        # Should return default funding info on error
        self.assertIsInstance(funding_info, FundingInfo)
        self.assertEqual(trading_pair, funding_info.trading_pair)
        self.assertEqual(Decimal("0"), funding_info.mark_price)
        self.assertEqual(Decimal("0"), funding_info.index_price)
        self.assertEqual(Decimal("0"), funding_info.rate)

    async def test_get_funding_info_with_empty_response(self):
        """Test get_funding_info handles empty response from API"""
        trading_pair = "SOL-USDC"

        # Mock REST assistant with empty response
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value=[])  # Empty array

        with patch.object(self.data_source._api_factory, 'get_rest_assistant', return_value=mock_rest_assistant):
            funding_info = await self.data_source.get_funding_info(trading_pair)

        # Should return default funding info for empty response
        self.assertIsInstance(funding_info, FundingInfo)
        self.assertEqual(trading_pair, funding_info.trading_pair)
        self.assertEqual(Decimal("0"), funding_info.mark_price)

    def test_parse_funding_info_message_not_implemented(self):
        """Test that _parse_funding_info_message is properly implemented (no-op for Backpack)"""
        # This should not raise an exception since it's implemented as a no-op
        mock_queue = AsyncMock()
        raw_message = {"test": "message"}

        try:
            asyncio.get_event_loop().run_until_complete(
                self.data_source._parse_funding_info_message(raw_message, mock_queue)
            )
        except NotImplementedError:
            self.fail("_parse_funding_info_message should not raise NotImplementedError")

    def test_channel_originating_message_trade(self):
        """Test message routing for trade messages"""
        trade_message = {
            "stream": "trade.SOL_USDC_PERP",  # Updated to include _PERP suffix
            "data": {"e": "trade", "s": "SOL_USDC_PERP"}
        }

        channel = self.data_source._channel_originating_message(trade_message)
        self.assertEqual(CONSTANTS.WS_TRADES_CHANNEL, channel)

    def test_channel_originating_message_depth(self):
        """Test message routing for depth messages"""
        depth_message = {
            "stream": "depth.SOL_USDC_PERP",  # Updated to include _PERP suffix
            "data": {"e": "depth", "s": "SOL_USDC_PERP"}
        }

        channel = self.data_source._channel_originating_message(depth_message)
        self.assertEqual(CONSTANTS.WS_DEPTH_CHANNEL, channel)

    def test_channel_originating_message_unknown(self):
        """Test message routing for unknown message types"""
        unknown_message = {
            "stream": "unknown.SOL_USDC_PERP",  # Updated to include _PERP suffix
            "data": {"e": "unknown"}
        }

        channel = self.data_source._channel_originating_message(unknown_message)
        self.assertEqual("", channel)

    async def test_request_order_book_snapshot_successful(self):
        """Test successful order book snapshot retrieval"""
        trading_pair = "SOL-USDC"

        # Mock the depth API response
        mock_response_data = {
            "bids": [["50000.00", "1.5"], ["49999.00", "2.0"]],
            "asks": [["50001.00", "1.2"], ["50002.00", "1.8"]]
        }

        # Mock REST assistant
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value=mock_response_data)

        with patch.object(self.data_source._api_factory, 'get_rest_assistant', return_value=mock_rest_assistant):
            snapshot = await self.data_source._request_order_book_snapshot(trading_pair)

        self.assertEqual(mock_response_data, snapshot)

    async def test_subscribe_channels_format(self):
        """Test that WebSocket subscription messages are formatted correctly"""
        mock_ws = AsyncMock()

        await self.data_source._subscribe_channels(mock_ws)

        # Verify that send was called with the correct subscription messages
        self.assertGreater(mock_ws.send.call_count, 0)

        # Check that we attempted to subscribe to trade and depth for each trading pair
        expected_calls = len(self.trading_pairs) * 2  # trade + depth for each pair
        self.assertEqual(expected_calls, mock_ws.send.call_count)
