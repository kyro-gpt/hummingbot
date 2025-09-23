import asyncio
import unittest
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.aster_perpetual import aster_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_api_order_book_data_source import (
    AsterPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_auth import AsterPerpetualAuth
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class AsterPerpetualAPIOrderBookDataSourceUnitTests(unittest.TestCase):
    # Basic test level for logs
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = CONSTANTS.TESTNET_DOMAIN

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.ev_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.ev_loop)

        # Create mock connector (minimal for testing)
        self.connector = MagicMock()
        self.connector.domain = self.domain

        # Mock exchange symbol mapping methods
        async def mock_exchange_symbol_associated_to_pair(trading_pair):
            return self.ex_trading_pair

        async def mock_trading_pair_associated_to_exchange_symbol(symbol):
            return self.trading_pair

        self.connector.exchange_symbol_associated_to_pair = mock_exchange_symbol_associated_to_pair
        self.connector.trading_pair_associated_to_exchange_symbol = mock_trading_pair_associated_to_exchange_symbol

        # Create mock API factory
        self.api_factory = MagicMock()

        self.data_source = AsterPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self) -> None:
        self.ev_loop.close()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def async_run_with_timeout(self, coroutine, timeout: float = 1):
        return self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))

    def test_data_source_initialization(self):
        """Test basic data source initialization"""
        self.assertEqual(self.data_source._domain, self.domain)
        self.assertEqual(self.data_source._connector, self.connector)
        self.assertEqual(self.data_source._trading_pairs, [self.trading_pair])
        self.assertIsNotNone(self.data_source._message_queue)

    def test_exchange_name_property(self):
        """Test exchange name property"""
        self.assertEqual(self.data_source.exchange_name, CONSTANTS.EXCHANGE_NAME)

    def test_message_queue_keys(self):
        """Test message queue key configuration"""
        self.assertEqual(self.data_source._trade_messages_queue_key, CONSTANTS.TRADE_STREAM_ID)
        self.assertEqual(self.data_source._diff_messages_queue_key, CONSTANTS.DIFF_STREAM_ID)
        self.assertEqual(self.data_source._funding_info_messages_queue_key, CONSTANTS.FUNDING_INFO_STREAM_ID)
        self.assertEqual(self.data_source._snapshot_messages_queue_key, "order_book_snapshot")

    def test_channel_originating_message_depth(self):
        """Test channel routing for depth messages"""
        # Test depth@100ms channel (our optimized channel)
        event_message = {
            "stream": f"{self.ex_trading_pair.lower()}@depth@100ms",
            "data": {"e": "depthUpdate"}
        }

        channel = self.data_source._channel_originating_message(event_message)
        self.assertEqual(channel, self.data_source._diff_messages_queue_key)

    def test_channel_originating_message_trades(self):
        """Test channel routing for trade messages"""
        event_message = {
            "stream": f"{self.ex_trading_pair.lower()}@aggTrade",
            "data": {"e": "aggTrade"}
        }

        channel = self.data_source._channel_originating_message(event_message)
        self.assertEqual(channel, self.data_source._trade_messages_queue_key)

    def test_channel_originating_message_funding(self):
        """Test channel routing for funding messages"""
        event_message = {
            "stream": f"{self.ex_trading_pair.lower()}@markPrice",
            "data": {"e": "markPriceUpdate"}
        }

        channel = self.data_source._channel_originating_message(event_message)
        self.assertEqual(channel, self.data_source._funding_info_messages_queue_key)

    def test_channel_originating_message_unknown(self):
        """Test channel routing for unknown messages"""
        event_message = {
            "stream": f"{self.ex_trading_pair.lower()}@unknown",
            "data": {"e": "unknownEvent"}
        }

        channel = self.data_source._channel_originating_message(event_message)
        self.assertEqual(channel, "")

    def test_channel_originating_message_result(self):
        """Test channel routing for result messages (subscription confirmations)"""
        event_message = {
            "result": None,
            "id": 1
        }

        channel = self.data_source._channel_originating_message(event_message)
        self.assertEqual(channel, "")

    def test_websocket_channel_optimization(self):
        """Test that we're using optimized WebSocket channels"""
        # Verify our optimized channel constants
        self.assertEqual(CONSTANTS.WS_DEPTH_CHANNEL, "@depth@100ms")
        self.assertEqual(CONSTANTS.WS_TRADE_CHANNEL, "@aggTrade")
        self.assertEqual(CONSTANTS.WS_FUNDING_CHANNEL, "@markPrice")

        # This confirms we're using 2.5x faster depth updates than Binance default

    def test_get_last_traded_prices_delegation(self):
        """Test that last traded prices delegates to connector"""
        # Mock the connector method
        mock_result = {"BTC-USDT": 50000.0}
        self.connector.get_last_traded_prices = AsyncMock(return_value=mock_result)

        result = self.async_run_with_timeout(
            self.data_source.get_last_traded_prices([self.trading_pair])
        )

        self.assertEqual(result, mock_result)
        self.connector.get_last_traded_prices.assert_called_once_with(trading_pairs=[self.trading_pair])

    # === TODO: Complex integration tests to be implemented later ===

    def test_connected_websocket_assistant_TODO(self):
        """TODO: Test WebSocket connection - requires complex mocking"""
        self.skipTest("TODO: Implement WebSocket connection testing")

    def test_subscribe_channels_TODO(self):
        """TODO: Test channel subscription with optimized @depth@100ms - requires WebSocket mocking"""
        self.skipTest("TODO: Implement channel subscription testing")

    def test_parse_order_book_diff_message_TODO(self):
        """TODO: Test order book diff message parsing - requires message queue mocking"""
        self.skipTest("TODO: Implement order book diff parsing testing")

    def test_parse_trade_message_TODO(self):
        """TODO: Test trade message parsing - requires message queue mocking"""
        self.skipTest("TODO: Implement trade message parsing testing")

    def test_parse_funding_info_message_TODO(self):
        """TODO: Test funding info message parsing - requires message queue mocking"""
        self.skipTest("TODO: Implement funding info parsing testing")

    def test_order_book_snapshot_TODO(self):
        """TODO: Test order book snapshot retrieval - requires REST API mocking"""
        self.skipTest("TODO: Implement order book snapshot testing")

    def test_request_order_book_snapshot_TODO(self):
        """TODO: Test order book snapshot request - requires REST API mocking"""
        self.skipTest("TODO: Implement order book snapshot request testing")

    def test_get_funding_info_TODO(self):
        """TODO: Test funding info retrieval - requires REST API mocking"""
        self.skipTest("TODO: Implement funding info testing")

    def test_listen_for_subscriptions_TODO(self):
        """TODO: Test subscription listening - requires complex async WebSocket mocking"""
        self.skipTest("TODO: Implement subscription listening testing")

    def test_listen_for_order_book_diffs_TODO(self):
        """TODO: Test order book diff listening - requires complex async mocking"""
        self.skipTest("TODO: Implement order book diff listening testing")

    def test_listen_for_trades_TODO(self):
        """TODO: Test trade listening - requires complex async mocking"""
        self.skipTest("TODO: Implement trade listening testing")

    def test_listen_for_funding_info_TODO(self):
        """TODO: Test funding info listening - requires complex async mocking"""
        self.skipTest("TODO: Implement funding info listening testing")


if __name__ == "__main__":
    unittest.main()
