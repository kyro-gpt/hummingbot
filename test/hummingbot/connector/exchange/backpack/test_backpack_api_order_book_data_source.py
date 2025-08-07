import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils
from hummingbot.connector.exchange.backpack.backpack_api_order_book_data_source import BackpackAPIOrderBookDataSource
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BackpackAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
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
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        client_config_map = ClientConfigAdapter(ClientConfigMap())

        # Generate test credentials for the exchange
        import base64

        from cryptography.hazmat.primitives.asymmetric import ed25519

        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        test_api_key = base64.b64encode(public_key.public_bytes_raw()).decode()
        test_secret = base64.b64encode(private_key.private_bytes_raw()).decode()

        self.connector = BackpackExchange(
            client_config_map=client_config_map,
            backpack_api_key=test_api_key,
            backpack_secret_key=test_secret,
            trading_pairs=[],
            trading_required=False)

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

        self.data_source = BackpackAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory)

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self._original_full_order_book_reset_time = self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        self.resume_test_event = asyncio.Event()

        # Set trading pair symbol map
        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _snapshot_response(self):
        """Mock response for order book snapshot"""
        return {
            "bids": [
                ["100.5", "10.0"],
                ["100.0", "5.0"]
            ],
            "asks": [
                ["101.0", "15.0"],
                ["101.5", "20.0"]
            ],
            "lastUpdateId": 12345,
            "timestamp": 1684026955123
        }

    def _trade_update_event(self):
        """Mock WebSocket trade update event - Real Backpack format"""
        return {
            "stream": f"trade.{self.ex_trading_pair}",
            "data": {
                "E": 1754456573572989,       # Event time in microseconds
                "T": 1754456573571000,       # Trade time in microseconds
                "a": "5006295321",           # Aggressor order ID
                "b": "5006296871",           # Other order ID
                "e": "trade",                # Event type
                "m": False,                  # Is the buyer the maker?
                "p": "100.25",               # Price
                "q": "5.5",                  # Quantity
                "s": self.ex_trading_pair,   # Symbol
                "t": 362888899               # Trade ID
            }
        }

    def _order_diff_event(self):
        """Mock WebSocket order book diff event - Real Backpack format"""
        return {
            "stream": f"depth.{self.ex_trading_pair}",
            "data": {
                "E": 1754456529627970,       # Event time in microseconds
                "T": 1754456529626233,       # Engine timestamp in microseconds
                "U": 2545341877,             # First update ID in event
                "a": [                       # Asks
                    ["102.0", "12.0"]
                ],
                "b": [                       # Bids
                    ["99.5", "8.0"]
                ],
                "e": "depth",                # Event type
                "s": self.ex_trading_pair,   # Symbol
                "u": 2545341877              # Last update ID in event
            }
        }

    @aioresponses()
    async def test_get_new_order_book_successful(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = self._snapshot_response()
        mock_api.get(regex_url, body=json.dumps(resp))

        order_book: OrderBook = await self.data_source.get_new_order_book(self.trading_pair)

        expected_update_id = resp["lastUpdateId"]

        self.assertEqual(expected_update_id, order_book.snapshot_uid)
        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())

        # Check bids
        self.assertEqual(2, len(bids))
        self.assertEqual(100.5, bids[0].price)
        self.assertEqual(10.0, bids[0].amount)
        self.assertEqual(expected_update_id, bids[0].update_id)

        # Check asks
        self.assertEqual(2, len(asks))
        self.assertEqual(101.0, asks[0].price)
        self.assertEqual(15.0, asks[0].amount)
        self.assertEqual(expected_update_id, asks[0].update_id)

    @aioresponses()
    async def test_get_new_order_book_raises_exception(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, status=400, body=json.dumps({"error": "Bad request"}))

        with self.assertRaises(IOError):
            await self.data_source.get_new_order_book(self.trading_pair)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        result_subscribe_trades = {"result": None, "id": 1}
        result_subscribe_diffs = {"result": None, "id": 2}

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_trades))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_diffs))

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(2, len(sent_subscription_messages))
        expected_trade_subscription = {
            "method": "SUBSCRIBE",
            "params": [f"trade.{self.ex_trading_pair}"],  # Correct format: trade.SYMBOL
            "id": 1
        }
        expected_diff_subscription = {
            "method": "SUBSCRIBE",
            "params": [f"depth.{self.ex_trading_pair}"],  # Correct format: depth.SYMBOL
            "id": 2
        }

        self.assertEqual(expected_trade_subscription, sent_subscription_messages[0])
        self.assertEqual(expected_diff_subscription, sent_subscription_messages[1])

    async def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()
        event_messages = [self._trade_update_event(), asyncio.CancelledError()]
        mock_queue.get.side_effect = event_messages

        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(362888899, msg.trade_id)
        self.assertEqual(1754456573.571, msg.timestamp)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        content = msg.content
        self.assertEqual(1.0, content["trade_type"])  # Buy trade
        self.assertEqual(5.5, content["amount"])
        self.assertEqual(100.25, content["price"])

    async def test_listen_for_order_book_diffs_successful(self):
        mock_queue = AsyncMock()
        event_messages = [self._order_diff_event(), asyncio.CancelledError()]
        mock_queue.get.side_effect = event_messages

        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(2545341877, msg.update_id)
        self.assertEqual(2545341877, msg.first_update_id)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        # Check bids and asks
        self.assertEqual(1, len(msg.bids))
        self.assertEqual(99.5, msg.bids[0].price)
        self.assertEqual(8.0, msg.bids[0].amount)

        self.assertEqual(1, len(msg.asks))
        self.assertEqual(102.0, msg.asks[0].price)
        self.assertEqual(12.0, msg.asks[0].amount)
