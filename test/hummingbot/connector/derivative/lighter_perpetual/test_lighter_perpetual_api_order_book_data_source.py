import asyncio
import json
import time
import unittest
from decimal import Decimal
from typing import Awaitable, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_order_book_data_source import (
    LighterPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


class LighterPerpetualAPIOrderBookDataSourceTests(unittest.TestCase):
    # Logging Level = Disabled
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "ETH"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value=self.ex_trading_pair)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=self.trading_pair)
        self.connector._api_get = AsyncMock()
        self.connector.get_last_traded_prices = AsyncMock(return_value={self.trading_pair: 100.0})

        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self.api_factory = WebAssistantsFactory(throttler=self.throttler)
        self.ob_data_source = LighterPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain,
        )

        self.ob_data_source.logger().setLevel(1)
        self.ob_data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and message in record.getMessage() for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def test_get_last_traded_prices(self):
        """Test getting last traded prices"""
        result = self.async_run_with_timeout(
            self.ob_data_source.get_last_traded_prices([self.trading_pair])
        )

        expected_result = {self.trading_pair: 100.0}
        self.assertEqual(expected_result, result)
        self.connector.get_last_traded_prices.assert_called_once_with(trading_pairs=[self.trading_pair])

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_trading_pair_to_market_id")
    def test_get_funding_info(self, mock_format_pair):
        """Test getting funding information"""
        mock_format_pair.return_value = 1

        # Mock funding response
        funding_response = {
            "code": 200,
            "fundings": [
                {
                    "funding_rate": "0.0001",
                    "timestamp": 1640995200
                }
            ]
        }

        # Mock order book details response
        orderbook_response = {
            "code": 200,
            "order_book_details": [
                {
                    "mark_price": "3000.50",
                    "index_price": "3000.00"
                }
            ]
        }

        self.connector._api_get.side_effect = [funding_response, orderbook_response]

        result = self.async_run_with_timeout(self.ob_data_source.get_funding_info(self.trading_pair))

        self.assertIsInstance(result, FundingInfo)
        self.assertEqual(result.trading_pair, self.trading_pair)
        self.assertEqual(result.rate, Decimal("0.0001"))
        self.assertEqual(result.mark_price, Decimal("3000.50"))
        self.assertEqual(result.index_price, Decimal("3000.00"))

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_trading_pair_to_market_id")
    def test_request_order_book_snapshot(self, mock_format_pair):
        """Test requesting order book snapshot"""
        mock_format_pair.return_value = 1

        # Updated to match orderBookOrders endpoint format
        expected_response = {
            "code": 200,
            "total_bids": 2,
            "total_asks": 2,
            "bids": [
                {"price": "2999.50", "remaining_base_amount": "0.1", "order_id": "123"},
                {"price": "2999.00", "remaining_base_amount": "0.2", "order_id": "124"}
            ],
            "asks": [
                {"price": "3000.50", "remaining_base_amount": "0.1", "order_id": "125"},
                {"price": "3001.00", "remaining_base_amount": "0.2", "order_id": "126"}
            ]
        }

        self.connector._api_get.return_value = expected_response

        result = self.async_run_with_timeout(
            self.ob_data_source._request_order_book_snapshot(self.trading_pair)
        )

        self.assertEqual(result, expected_response)
        self.connector._api_get.assert_called_once_with(
            path_url=CONSTANTS.ORDER_BOOK_ORDERS_PATH_URL,
            params={"market_id": 1, "limit": 100}
        )

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_trading_pair_to_market_id")
    def test_order_book_snapshot(self, mock_format_pair):
        """Test converting order book snapshot to OrderBookMessage"""
        mock_format_pair.return_value = 1

        # Updated to match orderBookOrders endpoint format
        snapshot_response = {
            "code": 200,
            "total_bids": 2,
            "total_asks": 2,
            "bids": [
                {"price": "2999.50", "remaining_base_amount": "0.1", "order_id": "123"},
                {"price": "2999.00", "remaining_base_amount": "0.2", "order_id": "124"}
            ],
            "asks": [
                {"price": "3000.50", "remaining_base_amount": "0.1", "order_id": "125"},
                {"price": "3001.00", "remaining_base_amount": "0.2", "order_id": "126"}
            ]
        }

        self.connector._api_get.return_value = snapshot_response

        result = self.async_run_with_timeout(
            self.ob_data_source._order_book_snapshot(self.trading_pair)
        )

        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(result.type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(result.content["trading_pair"], self.trading_pair)

        # Check bids and asks
        expected_bids = [[2999.50, 0.1], [2999.00, 0.2]]
        expected_asks = [[3000.50, 0.1], [3001.00, 0.2]]

        self.assertEqual(result.content["bids"], expected_bids)
        self.assertEqual(result.content["asks"], expected_asks)

    def test_order_book_snapshot_no_data(self):
        """Test order book snapshot with no data"""
        snapshot_response = {
            "code": 200,
            "order_book_details": []
        }

        self.connector._api_get.return_value = snapshot_response

        with self.assertRaises(ValueError) as context:
            self.async_run_with_timeout(
                self.ob_data_source._order_book_snapshot(self.trading_pair)
            )

        self.assertIn("No order book data found", str(context.exception))

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.wss_url")
    def test_connected_websocket_assistant(self, mock_wss_url):
        """Test WebSocket connection"""
        mock_wss_url.return_value = "wss://testnet.zklighter.elliot.ai/stream"

        with patch.object(self.api_factory, 'get_ws_assistant', new_callable=AsyncMock) as mock_get_ws:
            mock_ws = AsyncMock()
            mock_get_ws.return_value = mock_ws

            result = self.async_run_with_timeout(
                self.ob_data_source._connected_websocket_assistant()
            )

            self.assertEqual(result, mock_ws)
            mock_ws.connect.assert_called_once_with(
                ws_url="wss://testnet.zklighter.elliot.ai/stream",
                ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL
            )

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_trading_pair_to_market_id")
    def test_subscribe_channels(self, mock_format_pair):
        """Test WebSocket channel subscription"""
        mock_format_pair.return_value = 1

        mock_ws = AsyncMock()

        self.async_run_with_timeout(
            self.ob_data_source._subscribe_channels(mock_ws)
        )

        # Verify subscription message was sent (should be called once per trading pair)
        mock_ws.send.assert_called_once()
        call_args = mock_ws.send.call_args[0][0]

        expected_payload = {
            "type": "subscribe",
            "channel": "order_book/1"
        }

        self.assertEqual(call_args.payload, expected_payload)

    def test_channel_originating_message(self):
        """Test message channel identification"""
        # Test order book update message
        order_book_message = {"type": "update/order_book", "data": {}}
        channel = self.ob_data_source._channel_originating_message(order_book_message)
        self.assertEqual(channel, self.ob_data_source._snapshot_messages_queue_key)

        # Test trade message
        trade_message = {"type": "update/trades", "data": {}}
        channel = self.ob_data_source._channel_originating_message(trade_message)
        self.assertEqual(channel, self.ob_data_source._trade_messages_queue_key)

        # Test unknown message
        unknown_message = {"type": "unknown", "data": {}}
        channel = self.ob_data_source._channel_originating_message(unknown_message)
        self.assertEqual(channel, "")

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_market_id_to_trading_pair")
    def test_parse_order_book_diff_message(self, mock_format_market):
        """Test parsing order book diff message"""
        mock_format_market.return_value = self.trading_pair

        raw_message = {
            "type": "update/order_book",
            "channel": "order_book:1",
            "order_book": {
                "bids": [
                    {"price": "2999.50", "size": "0.1"}
                ],
                "asks": [
                    {"price": "3000.50", "size": "0.1"}
                ]
            }
        }

        message_queue = asyncio.Queue()

        self.async_run_with_timeout(
            self.ob_data_source._parse_order_book_diff_message(raw_message, message_queue)
        )

        self.assertEqual(message_queue.qsize(), 1)

        result = self.async_run_with_timeout(message_queue.get())
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(result.type, OrderBookMessageType.DIFF)
        self.assertEqual(result.content["trading_pair"], self.trading_pair)

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_market_id_to_trading_pair")
    def test_parse_order_book_snapshot_message(self, mock_format_market):
        """Test parsing order book snapshot message"""
        mock_format_market.return_value = self.trading_pair

        raw_message = {
            "type": "subscribed/order_book",
            "channel": "order_book:1",
            "order_book": {
                "bids": [
                    {"price": "2999.50", "size": "0.1"}
                ],
                "asks": [
                    {"price": "3000.50", "size": "0.1"}
                ]
            }
        }

        message_queue = asyncio.Queue()

        self.async_run_with_timeout(
            self.ob_data_source._parse_order_book_snapshot_message(raw_message, message_queue)
        )

        self.assertEqual(message_queue.qsize(), 1)

        result = self.async_run_with_timeout(message_queue.get())
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(result.type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(result.content["trading_pair"], self.trading_pair)

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_market_id_to_trading_pair")
    def test_parse_trade_message(self, mock_format_market):
        """Test parsing trade message"""
        mock_format_market.return_value = self.trading_pair

        raw_message = {
            "type": "update/trades",
            "data": [
                {
                    "market_id": 1,
                    "trade_id": 12345,
                    "price": "3000.00",
                    "size": "0.1",
                    "timestamp": 1640995200
                }
            ]
        }

        message_queue = asyncio.Queue()

        self.async_run_with_timeout(
            self.ob_data_source._parse_trade_message(raw_message, message_queue)
        )

        self.assertEqual(message_queue.qsize(), 1)

        result = self.async_run_with_timeout(message_queue.get())
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(result.type, OrderBookMessageType.TRADE)
        self.assertEqual(result.content["trading_pair"], self.trading_pair)
        self.assertEqual(result.content["trade_id"], "12345")
        self.assertEqual(result.content["price"], 3000.00)
        self.assertEqual(result.content["amount"], 0.1)

    def test_parse_trade_message_single_trade(self):
        """Test parsing single trade message"""
        with patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_market_id_to_trading_pair") as mock_format_market:
            mock_format_market.return_value = self.trading_pair

            raw_message = {
                "type": "update/trades",
                "data": {
                    "market_id": 1,
                    "trade_id": 12345,
                    "price": "3000.00",
                    "size": "0.1",
                    "timestamp": 1640995200
                }
            }

            message_queue = asyncio.Queue()

            self.async_run_with_timeout(
                self.ob_data_source._parse_trade_message(raw_message, message_queue)
            )

            self.assertEqual(message_queue.qsize(), 1)

    def test_next_funding_time(self):
        """Test next funding time calculation"""
        # Mock current time to a known value
        with patch('time.time', return_value=1640995200):  # 2022-01-01 00:00:00 UTC
            next_funding = self.ob_data_source._next_funding_time()

            # Should be 8 hours later (8 * 3600 = 28800 seconds)
            expected_time = 1640995200 + 28800  # Next 8-hour interval
            self.assertEqual(next_funding, expected_time)

    def test_listen_for_funding_info(self):
        """Test funding info listener"""
        funding_info = FundingInfo(
            trading_pair=self.trading_pair,
            index_price=Decimal("3000.00"),
            mark_price=Decimal("3000.50"),
            next_funding_utc_timestamp=1640995200,
            rate=Decimal("0.0001")
        )

        with patch.object(self.ob_data_source, 'get_funding_info', new_callable=AsyncMock) as mock_get_funding:
            mock_get_funding.return_value = funding_info

            output_queue = asyncio.Queue()

            # Create a task that will run for a short time
            async def run_listener():
                try:
                    await asyncio.wait_for(
                        self.ob_data_source.listen_for_funding_info(output_queue),
                        timeout=0.1
                    )
                except asyncio.TimeoutError:
                    pass  # Expected timeout

            self.async_run_with_timeout(run_listener())

            # Should have at least one funding info update
            self.assertGreater(output_queue.qsize(), 0)

    def test_parse_funding_info_message(self):
        """Test parsing funding info message (placeholder)"""
        raw_message = {"type": "funding_info", "data": {}}
        message_queue = asyncio.Queue()

        # Should not raise any exception
        self.async_run_with_timeout(
            self.ob_data_source._parse_funding_info_message(raw_message, message_queue)
        )

        # Queue should remain empty as this is a placeholder
        self.assertEqual(message_queue.qsize(), 0)

    def test_error_handling_in_parse_methods(self):
        """Test error handling in parse methods"""
        message_queue = asyncio.Queue()

        # Test with malformed message
        malformed_message = {"invalid": "data"}

        # Should not raise exceptions, but log errors
        self.async_run_with_timeout(
            self.ob_data_source._parse_order_book_diff_message(malformed_message, message_queue)
        )

        self.async_run_with_timeout(
            self.ob_data_source._parse_trade_message(malformed_message, message_queue)
        )

        # Queue should remain empty
        self.assertEqual(message_queue.qsize(), 0)

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_trading_pair_to_market_id")
    def test_get_new_order_book_successful(self, mock_format_pair):
        """Test getting new OrderBook instance"""
        mock_format_pair.return_value = 1
        
        # Updated to match orderBookOrders endpoint format
        snapshot_response = {
            "code": 200,
            "total_bids": 2,
            "total_asks": 2,
            "bids": [
                {"price": "2999.50", "remaining_base_amount": "0.1", "order_id": "123"},
                {"price": "2999.00", "remaining_base_amount": "0.2", "order_id": "124"}
            ],
            "asks": [
                {"price": "3000.50", "remaining_base_amount": "0.1", "order_id": "125"},
                {"price": "3001.00", "remaining_base_amount": "0.2", "order_id": "126"}
            ]
        }
        
        self.connector._api_get.return_value = snapshot_response
        
        result = self.async_run_with_timeout(
            self.ob_data_source.get_new_order_book(self.trading_pair)
        )
        
        self.assertIsInstance(result, OrderBook)
        # Verify order book has data
        self.assertGreater(len(list(result.bid_entries())), 0)
        self.assertGreater(len(list(result.ask_entries())), 0)

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils.format_trading_pair_to_market_id")
    def test_request_complete_funding_info(self, mock_format_pair):
        """Test requesting complete funding info"""
        mock_format_pair.return_value = 1
        
        funding_response = {
            "code": 200,
            "fundings": [{"funding_rate": "0.0001", "timestamp": 1640995200}]
        }
        
        orderbook_response = {
            "code": 200,
            "order_book_details": [{"mark_price": "3000.50", "index_price": "3000.00"}]
        }
        
        self.connector._api_get.side_effect = [funding_response, orderbook_response]
        
        result = self.async_run_with_timeout(
            self.ob_data_source._request_complete_funding_info(self.trading_pair)
        )
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], funding_response)
        self.assertEqual(result[1], orderbook_response)


if __name__ == "__main__":
    unittest.main()
