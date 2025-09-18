"""
Unit tests for BackpackPerpetualAPIUserStreamDataSource

Tests the derivative-specific user stream data source with position updates,
order events, and WebSocket authentication for perpetual trading.
"""
import asyncio
import json
import unittest
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_user_stream_data_source import (
    BackpackPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BackpackPerpetualAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.trading_pairs = ["SOL-USDC", "ETH-USDT"]
        self.domain = CONSTANTS.DEFAULT_DOMAIN

        # Create mock connector
        self.connector = MagicMock()
        self.connector.web_utils = MagicMock()
        self.connector.web_utils.convert_to_exchange_trading_pair = MagicMock(
            side_effect=lambda pair: pair.replace("-", "_") + "_PERP"
        )

        # Create mock auth
        self.auth = MagicMock(spec=BackpackPerpetualAuth)
        self.auth.websocket_login_parameters = MagicMock(return_value={
            "signature": [1, 2, 3, 4],  # Ed25519 signature array
            "timestamp": 1640995200000,
            "window": 5000
        })

        # Create throttler and factory
        from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)

        self.api_factory = WebAssistantsFactory(throttler=self.throttler)

        # Create data source
        self.data_source = BackpackPerpetualAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=self.trading_pairs,
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain
        )

    async def test_listen_for_user_stream_successful_with_order_update(self):
        """Test successful user stream listening with order update events"""
        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock()
        mock_ws.send = AsyncMock()

        # Create order update message
        order_update_message = {
            "stream": "account.orderUpdate",
            "data": {
                "E": 1754462606817679,
                "O": "USER",
                "S": "Bid",
                "T": 1754462606815843,
                "V": "RejectTaker",
                "X": "New",
                "Z": "0",
                "c": 2115950059,
                "e": "orderAccepted",
                "f": "GTC",
                "i": "5012875717",
                "o": "LIMIT",
                "p": "140.00",
                "q": "0.01",
                "r": False,
                "s": "SOL_USDC_PERP",
                "t": None,
                "z": "0",
                "n": "30",
                "N": "USDC"
            }
        }

        # Create mock WebSocket response
        mock_response = MagicMock()
        mock_response.data = order_update_message

        # Mock async iterator for WebSocket messages
        async def mock_iter_messages():
            yield mock_response

        mock_ws.iter_messages = mock_iter_messages

        with patch.object(self.data_source, '_get_ws_assistant', return_value=mock_ws):
            output_queue = asyncio.Queue()

            # Process the single message
            async for ws_response in mock_ws.iter_messages():
                await self.data_source._process_event_message(ws_response.data, output_queue)
                break  # Process only one message for testing

            # Verify message was queued
            self.assertFalse(output_queue.empty())
            queued_message = await output_queue.get()
            self.assertEqual(order_update_message, queued_message)

    async def test_listen_for_user_stream_successful_with_position_update(self):
        """Test successful user stream listening with position update events"""
        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock()
        mock_ws.send = AsyncMock()

        # Create position update message
        position_update_message = {
            "stream": "account.positionUpdate",
            "data": {
                "B": "164.59",
                "E": 1754462601563677,
                "M": "163.81034805",
                "P": "-0.007796",
                "Q": "0.01",
                "T": 1754462601563678,
                "b": "164.6398",
                "f": "0.02",
                "i": 4681711685,
                "l": "0",
                "m": "0.0125",
                "n": "1.6381034805",
                "p": "0",
                "q": "0.01",
                "s": "SOL_USDC_PERP"
            }
        }

        # Create mock WebSocket response
        mock_response = MagicMock()
        mock_response.data = position_update_message

        # Mock async iterator for WebSocket messages
        async def mock_iter_messages():
            yield mock_response

        mock_ws.iter_messages = mock_iter_messages

        with patch.object(self.data_source, '_get_ws_assistant', return_value=mock_ws):
            output_queue = asyncio.Queue()

            # Process the single message
            async for ws_response in mock_ws.iter_messages():
                await self.data_source._process_event_message(ws_response.data, output_queue)
                break  # Process only one message for testing

            # Verify message was queued
            self.assertFalse(output_queue.empty())
            queued_message = await output_queue.get()
            self.assertEqual(position_update_message, queued_message)

    async def test_listen_for_user_stream_does_not_queue_empty_payload(self):
        """Test that empty payloads are not queued"""
        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock()
        mock_ws.send = AsyncMock()

        # Create empty/invalid message data
        empty_data_list = [
            {},
            {"stream": ""},
            {"data": {}},
            {"stream": "unknown"},
            None,
            "ping",
        ]

        output_queue = asyncio.Queue()

        # Process each empty message directly
        for empty_data in empty_data_list:
            await self.data_source._process_event_message(empty_data, output_queue)

        # Verify no messages were queued
        self.assertTrue(output_queue.empty())

    async def test_listen_for_user_stream_connection_failed(self):
        """Test handling of WebSocket connection failures"""
        # Mock WebSocket assistant that fails to connect
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock(side_effect=Exception("Connection failed"))

        with patch.object(self.data_source, '_get_ws_assistant', return_value=mock_ws):
            with self.assertRaises(Exception) as context:
                await self.data_source._connected_websocket_assistant()

            self.assertIn("Connection failed", str(context.exception))

    async def test_listen_for_user_stream_iter_message_throws_exception(self):
        """Test handling of exceptions during message iteration"""
        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock()
        mock_ws.send = AsyncMock()

        # Mock iter_messages to raise exception when called
        class MockIterWithException:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise Exception("Message iteration failed")

        mock_ws.iter_messages = MockIterWithException

        with patch.object(self.data_source, '_get_ws_assistant', return_value=mock_ws):
            output_queue = asyncio.Queue()

            # This should not raise an exception, but should handle it gracefully
            with self.assertLogs("hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_api_user_stream_data_source", level="ERROR") as log_context:
                await self.data_source._process_websocket_messages(mock_ws, output_queue)

            # Verify error was logged
            self.assertTrue(any("Error processing WebSocket message" in log.message for log in log_context.records))

    async def test_authentication_and_subscription(self):
        """Test WebSocket authentication and stream subscription"""
        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock()
        mock_ws.send = AsyncMock()

        # Test authentication and subscription
        await self.data_source._authenticate_and_subscribe(mock_ws)

        # Verify auth parameters were requested
        self.auth.websocket_login_parameters.assert_called_once()

        # Verify subscription message was sent
        mock_ws.send.assert_called_once()

        # Get the sent message
        sent_request = mock_ws.send.call_args[0][0]
        sent_payload = sent_request.payload

        # Verify subscription details
        self.assertEqual("SUBSCRIBE", sent_payload["method"])
        self.assertIn("account.orderUpdate", sent_payload["params"])
        self.assertIn("account.positionUpdate", sent_payload["params"])
        self.assertIn("account.orderUpdate.SOL_USDC_PERP", sent_payload["params"])
        self.assertIn("account.positionUpdate.SOL_USDC_PERP", sent_payload["params"])
        self.assertIn("account.orderUpdate.ETH_USDT_PERP", sent_payload["params"])
        self.assertIn("account.positionUpdate.ETH_USDT_PERP", sent_payload["params"])

        # Verify authentication parameters
        self.assertEqual([1, 2, 3, 4], sent_payload["signature"])
        self.assertEqual(1640995200000, sent_payload["timestamp"])
        self.assertEqual(5000, sent_payload["window"])

    async def test_subscription_confirmation_handling(self):
        """Test handling of subscription confirmation messages"""
        # Create subscription confirmation message
        confirmation_message = {
            "id": 1,
            "result": True
        }

        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        output_queue = asyncio.Queue()

        # Process confirmation message
        await self.data_source._process_event_message(confirmation_message, output_queue)

        # Verify confirmation was handled and not queued
        self.assertTrue(output_queue.empty())

    async def test_subscription_failure_handling(self):
        """Test handling of subscription failure messages"""
        # Create subscription failure message
        failure_message = {
            "id": 1,
            "result": False,
            "error": "Subscription failed"
        }

        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        output_queue = asyncio.Queue()

        # Process failure message - should log warning but not queue
        with self.assertLogs(self.data_source.logger().name, level="WARNING") as log_context:
            await self.data_source._process_event_message(failure_message, output_queue)

        # Verify warning was logged
        self.assertTrue(any("Subscription failed" in log.message for log in log_context.records))

        # Verify failure was not queued
        self.assertTrue(output_queue.empty())

    async def test_rfq_update_handling(self):
        """Test handling of RFQ update messages"""
        # Create RFQ update message
        rfq_message = {
            "stream": "account.rfqUpdate",
            "data": {
                "rfqId": "12345",
                "status": "filled"
            }
        }

        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        output_queue = asyncio.Queue()

        # Process RFQ message
        await self.data_source._process_event_message(rfq_message, output_queue)

        # Verify RFQ message was queued (for future use)
        self.assertFalse(output_queue.empty())
        queued_message = await output_queue.get()
        self.assertEqual(rfq_message, queued_message)

    async def test_unknown_stream_handling(self):
        """Test handling of unknown stream messages"""
        # Create unknown stream message
        unknown_message = {
            "stream": "unknown.stream",
            "data": {
                "some": "data"
            }
        }

        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        output_queue = asyncio.Queue()

        # Process unknown message - should log debug but not queue
        await self.data_source._process_event_message(unknown_message, output_queue)

        # Verify message was not queued
        self.assertTrue(output_queue.empty())

    async def test_websocket_url_formation(self):
        """Test WebSocket URL formation"""
        # Mock WebSocket assistant
        mock_ws = AsyncMock()
        mock_ws.connect = AsyncMock()

        with patch.object(self.data_source, '_get_ws_assistant', return_value=mock_ws), \
             patch.object(self.data_source, '_authenticate_and_subscribe', return_value=None):

            await self.data_source._connected_websocket_assistant()

            # Verify connection was attempted with correct URL
            expected_url = f"{CONSTANTS.WS_URL}/"
            mock_ws.connect.assert_called_once()
            call_kwargs = mock_ws.connect.call_args[1]
            self.assertEqual(expected_url, call_kwargs["ws_url"])
            self.assertEqual(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL, call_kwargs["ping_timeout"])

    def test_order_book_class_property(self):
        """Test that order_book_class returns the correct class"""
        from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_order_book import (
            BackpackPerpetualOrderBook,
        )

        self.assertEqual(BackpackPerpetualOrderBook, self.data_source.order_book_class)

    def test_last_recv_time_property(self):
        """Test last_recv_time property"""
        # The property should exist and return a float
        self.assertIsInstance(self.data_source.last_recv_time, float)


if __name__ == "__main__":
    unittest.main()
