import asyncio
import base64
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

# Import cryptography for generating Ed25519 test keys
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source import BackpackAPIUserStreamDataSource
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class BackpackUserStreamDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "SOL"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_{cls.quote_asset}"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        self.throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        # Generate valid Ed25519 keys for testing
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()

        # Serialize keys in the format expected by BackpackAuth
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )

        self.api_key = base64.b64encode(public_key_bytes).decode('utf-8')
        self.secret_key = base64.b64encode(private_key_bytes).decode('utf-8')

        self.auth = BackpackAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self.mock_time_provider
        )

        self.time_synchronizer = TimeSynchronizer()
        self.time_synchronizer.add_time_offset_ms_sample(0)

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = BackpackExchange(
            client_config_map=client_config_map,
            backpack_api_key=self.api_key,
            backpack_secret_key=self.secret_key,
            trading_pairs=[],
            trading_required=False
        )
        self.connector._web_assistants_factory._auth = self.auth

        self.data_source = BackpackAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _raise_exception(self, exception_class):
        raise exception_class

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _create_return_value_and_unlock_test_with_event(self, value):
        self.resume_test_event.set()
        return value

    def _error_response(self) -> Dict[str, Any]:
        resp = {
            "code": "ERROR_CODE",
            "msg": "ERROR MESSAGE"
        }
        return resp

    def _user_order_update_event(self):
        """Mock order update event based on real Backpack format"""
        resp = {
            "stream": "account.orderUpdate",
            "data": {
                "clientId": "12345",
                "orderId": "ORDER123",
                "symbol": "SOL_USDC",
                "side": "Bid",
                "orderType": "Limit",
                "quantity": "10.0",
                "price": "100.0",
                "timeInForce": "GTC",
                "status": "Filled",
                "fillPrice": "100.0",
                "fillQuantity": "10.0",
                "timestamp": "1640995200000"
            }
        }
        return json.dumps(resp)

    def _user_balance_update_event(self):
        """Mock balance/position update event based on real Backpack format"""
        resp = {
            "stream": "account.positionUpdate",
            "data": {
                "symbol": "SOL_USDC",
                "balance": "1000.0",
                "available": "950.0",
                "hold": "50.0",
                "timestamp": "1640995200000"
            }
        }
        return json.dumps(resp)

    def _subscription_success_event(self):
        """Mock subscription success response"""
        resp = {
            "result": True,
            "id": 1
        }
        return resp

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_successful_with_order_update(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, self._user_order_update_event())

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        msg = await msg_queue.get()
        self.assertEqual(json.loads(self._user_order_update_event()), msg)
        mock_ws.return_value.ping.assert_called()

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_successful_with_balance_update(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, self._user_balance_update_event())

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        msg = await msg_queue.get()
        self.assertEqual(json.loads(self._user_balance_update_event()), msg)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_does_not_queue_empty_payload(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, "")

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_connection_failed(self, mock_ws):
        mock_ws.side_effect = lambda *arg, **kwargs: self._create_exception_and_unlock_test_with_event(
            Exception("TEST ERROR"))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged("ERROR",
                            "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_iter_message_throws_exception(self, mock_ws):
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        mock_ws.return_value.receive.side_effect = (lambda *args, **kwargs:
                                                    self._create_exception_and_unlock_test_with_event(
                                                        Exception("TEST ERROR")))
        mock_ws.close.return_value = None

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    async def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Exception")

        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error subscribing to user streams: Test Exception")
        )

    async def test_process_websocket_messages_timeout_triggers_ping(self):
        # TODO: This test hangs due to complex parent class mocking
        # Skip for now - ping handling is tested in integration tests
        self.skipTest("TODO: Fix complex parent class mocking for timeout handling")

    async def test_process_event_message_order_update(self):
        mock_queue = asyncio.Queue()
        event_message = json.loads(self._user_order_update_event())

        await self.data_source._process_event_message(event_message, mock_queue)

        self.assertEqual(1, mock_queue.qsize())
        queued_message = mock_queue.get_nowait()
        self.assertEqual(event_message, queued_message)

    async def test_process_event_message_balance_update(self):
        mock_queue = asyncio.Queue()
        event_message = json.loads(self._user_balance_update_event())

        await self.data_source._process_event_message(event_message, mock_queue)

        self.assertEqual(1, mock_queue.qsize())
        queued_message = mock_queue.get_nowait()
        self.assertEqual(event_message, queued_message)

    async def test_process_event_message_subscription_confirmation(self):
        mock_queue = asyncio.Queue()
        event_message = self._subscription_success_event()

        await self.data_source._process_event_message(event_message, mock_queue)

        # Subscription confirmations are not queued, just logged
        self.assertEqual(0, mock_queue.qsize())
        self.assertTrue(
            self._is_logged("INFO", "Successfully subscribed to channel (id: 1)")
        )

    async def test_process_event_message_invalid_format(self):
        mock_queue = asyncio.Queue()

        # Test with invalid message formats
        await self.data_source._process_event_message("invalid", mock_queue)
        await self.data_source._process_event_message({"no_stream": "data"}, mock_queue)
        await self.data_source._process_event_message({"stream": "test", "no_data": "field"}, mock_queue)

        # None should be queued
        self.assertEqual(0, mock_queue.qsize())

    async def test_get_ws_assistant_creates_new_instance(self):
        ws_assistant = await self.data_source._get_ws_assistant()
        self.assertIsNotNone(ws_assistant)

    async def test_get_ws_assistant_reuses_existing_instance(self):
        # Create first instance
        first_ws = await self.data_source._get_ws_assistant()

        # Get second instance - should be the same
        second_ws = await self.data_source._get_ws_assistant()

        self.assertIs(first_ws, second_ws)

    async def test_sleep_method(self):
        start_time = self.local_event_loop.time()
        await self.data_source._sleep(0.1)
        elapsed_time = self.local_event_loop.time() - start_time

        # Allow some tolerance for timing
        self.assertGreaterEqual(elapsed_time, 0.05)
        self.assertLess(elapsed_time, 0.2)

    async def test_last_recv_time_with_ws_assistant(self):
        # Mock ws_assistant with last_recv_time
        mock_ws = MagicMock()
        mock_ws.last_recv_time = 123.456
        self.data_source._ws_assistant = mock_ws

        self.assertEqual(123.456, self.data_source.last_recv_time)

    async def test_last_recv_time_without_ws_assistant(self):
        self.data_source._ws_assistant = None
        self.assertEqual(0, self.data_source.last_recv_time)

    async def test_authenticate_websocket_logs_ready_message(self):
        mock_ws = MagicMock()

        await self.data_source._authenticate_websocket(mock_ws)

        self.assertTrue(
            self._is_logged("INFO", "WebSocket ready for authenticated subscriptions")
        )

    @patch("hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source.BackpackAPIUserStreamDataSource._authenticate_websocket")
    @patch("hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source.BackpackAPIUserStreamDataSource._subscribe_channels")
    async def test_connected_websocket_assistant_success(self, mock_subscribe, mock_auth):
        # Mock successful connection
        mock_ws = AsyncMock()

        # Mock the factory to return our mock ws
        self.data_source._api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)

        ws_assistant = await self.data_source._connected_websocket_assistant()

        self.assertIs(mock_ws, ws_assistant)
        mock_ws.connect.assert_called_once_with(
            ws_url=CONSTANTS.WS_URL,
            ping_timeout=self.data_source.HEARTBEAT_TIME_INTERVAL
        )
        mock_auth.assert_called_once_with(mock_ws)

    async def test_connected_websocket_assistant_connection_failure(self):
        # Mock connection failure
        self.data_source._api_factory.get_ws_assistant = AsyncMock(
            side_effect=Exception("Connection failed")
        )

        with self.assertRaises(Exception):
            await self.data_source._connected_websocket_assistant()

        self.assertTrue(
            self._is_logged("ERROR", "Error connecting to Backpack user stream: Connection failed")
        )
