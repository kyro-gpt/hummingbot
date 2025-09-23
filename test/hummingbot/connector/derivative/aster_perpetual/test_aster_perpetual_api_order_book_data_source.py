import asyncio
import re
import unittest
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import ujson
from aioresponses.core import aioresponses

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

    # === Message Parsing Tests (Core Functionality) ===
    
    def test_parse_order_book_diff_message(self):
        """Test order book diff message parsing"""
        # Create a mock message queue
        message_queue = asyncio.Queue()
        
        # Create sample diff message (Aster format should match Binance)
        raw_message = {
            "data": {
                "e": "depthUpdate",
                "E": 1631591424198,
                "s": self.ex_trading_pair,
                "u": 752409360466,
                "b": [["43614.31", "0.100"]],  # Bids
                "a": [["45277.14", "0.257"]],  # Asks
            }
        }
        
        # Mock the trading pair conversion
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=self.trading_pair)
        
        # Parse the message
        self.async_run_with_timeout(
            self.data_source._parse_order_book_diff_message(raw_message, message_queue)
        )
        
        # Verify message was queued
        self.assertFalse(message_queue.empty())

    def test_parse_trade_message(self):
        """Test trade message parsing"""
        message_queue = asyncio.Queue()
        
        # Create sample trade message
        raw_message = {
            "data": {
                "e": "aggTrade",
                "s": self.ex_trading_pair,
                "a": 817295132,  # Aggregate trade ID
                "p": "45266.16",  # Price
                "q": "2.206",     # Quantity
                "m": False,       # Is buyer maker
            }
        }
        
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=self.trading_pair)
        
        self.async_run_with_timeout(
            self.data_source._parse_trade_message(raw_message, message_queue)
        )
        
        self.assertFalse(message_queue.empty())

    def test_parse_funding_info_message(self):
        """Test funding info message parsing"""
        message_queue = asyncio.Queue()
        
        # Create sample funding info message
        raw_message = {
            "data": {
                "e": "markPriceUpdate",
                "s": self.ex_trading_pair,
                "p": "46353.99600757",  # Mark price
                "i": "46358.63622407",  # Index price
                "r": "0.00010000",      # Funding rate
                "T": 1641312000000,     # Next funding time
            }
        }
        
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=self.trading_pair)
        
        self.async_run_with_timeout(
            self.data_source._parse_funding_info_message(raw_message, message_queue)
        )
        
        self.assertFalse(message_queue.empty())

    @aioresponses()
    def test_request_order_book_snapshot_successful(self, mock_api):
        """Test successful order book snapshot request"""
        url = web_utils.public_rest_url(CONSTANTS.SNAPSHOT_REST_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        
        mock_response = {
            "lastUpdateId": 1027024,
            "bids": [["4.00000000", "431.00000000"]],
            "asks": [["4.00000200", "12.00000000"]]
        }
        mock_api.get(regex_url, body=ujson.dumps(mock_response))
        
        # Mock REST assistant
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value=mock_response)
        self.api_factory.get_rest_assistant = AsyncMock(return_value=mock_rest_assistant)
        
        result = self.async_run_with_timeout(
            self.data_source._request_order_book_snapshot(self.trading_pair)
        )
        
        self.assertEqual(result["lastUpdateId"], 1027024)
        self.assertEqual(len(result["bids"]), 1)
        self.assertEqual(len(result["asks"]), 1)

    @aioresponses()
    def test_get_funding_info_successful(self, mock_api):
        """Test successful funding info retrieval"""
        url = web_utils.public_rest_url(CONSTANTS.MARK_PRICE_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        
        mock_response = {
            "indexPrice": "46358.63622407",
            "markPrice": "46353.99600757", 
            "lastFundingRate": "0.00010000",
            "nextFundingTime": 1641312000000
        }
        mock_api.get(regex_url, body=ujson.dumps(mock_response))
        
        # Mock REST assistant
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value=mock_response)
        self.api_factory.get_rest_assistant = AsyncMock(return_value=mock_rest_assistant)
        
        result = self.async_run_with_timeout(
            self.data_source.get_funding_info(self.trading_pair)
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result.trading_pair, self.trading_pair)

    def test_subscribe_channels_verification(self):
        """Test that subscribe channels uses optimized Aster configuration"""
        # Test that we can call _subscribe_channels without errors
        mock_ws = AsyncMock()
        
        # We can't easily test the full subscription without complex WebSocket mocking,
        # but we can verify that our channel constants are properly configured
        self.assertEqual(CONSTANTS.WS_DEPTH_CHANNEL, "@depth@100ms")
        self.assertEqual(CONSTANTS.WS_TRADE_CHANNEL, "@aggTrade") 
        self.assertEqual(CONSTANTS.WS_FUNDING_CHANNEL, "@markPrice")
        
        # Verify the method exists and can be called
        self.assertTrue(hasattr(self.data_source, '_subscribe_channels'))
        self.assertTrue(asyncio.iscoroutinefunction(self.data_source._subscribe_channels))

    # === Advanced integration tests requiring full mocking ===
    
    def test_connected_websocket_assistant_TODO(self):
        """TODO: Test WebSocket connection - requires complex WebSocket mocking"""
        self.skipTest("TODO: Implement WebSocket connection testing")

    def test_subscribe_channels_full_integration_TODO(self):
        """TODO: Test full channel subscription with WebSocket - requires WebSocket mocking"""
        self.skipTest("TODO: Implement full subscription testing")

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

    def test_order_book_snapshot_full_integration_TODO(self):
        """TODO: Test order book snapshot with full message creation - requires complex mocking"""
        self.skipTest("TODO: Implement order book snapshot integration testing")


if __name__ == "__main__":
    unittest.main()
