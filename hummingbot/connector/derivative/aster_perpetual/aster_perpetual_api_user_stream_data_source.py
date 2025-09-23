import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_auth import AsterPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative


class AsterPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # Recommended to Ping/Update listen key to keep connection alive
    HEARTBEAT_TIME_INTERVAL = 30.0
    LISTEN_KEY_RETRY_INTERVAL = 5.0
    MAX_RETRIES = 3
    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: AsterPerpetualAuth,
            connector: 'AsterPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._connector = connector
        self._current_listen_key = None
        self._last_listen_key_ping_ts = None
        self._manage_listen_key_task = None
        self._listen_key_initialized_event = asyncio.Event()

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Creates a new WSAssistant instance.
        """
        # Always create a new assistant to avoid connection issues
        return await self._api_factory.get_ws_assistant()

    async def _get_listen_key(self, max_retries: int = MAX_RETRIES) -> str:
        """
        Retrieves a listen key from Aster for private stream authentication

        NOTE: Based on Binance pattern - Aster should support similar listen key mechanism
        """
        for retry_count in range(max_retries):
            try:
                rest_assistant = await self._api_factory.get_rest_assistant()
                response = await rest_assistant.execute_request(
                    url=web_utils.private_rest_url(CONSTANTS.ASTER_PERPETUAL_USER_STREAM_ENDPOINT, domain=self._domain),
                    method=RESTMethod.POST,
                    throttler_limit_id=CONSTANTS.ASTER_PERPETUAL_USER_STREAM_ENDPOINT,
                    is_auth_required=True,
                )

                return response["listenKey"]

            except Exception as e:
                self.logger().warning(f"Failed to get listen key (attempt {retry_count + 1}/{max_retries}): {e}")
                if retry_count == max_retries - 1:
                    raise
                await asyncio.sleep(self.LISTEN_KEY_RETRY_INTERVAL)

    async def _ping_listen_key(self, listen_key: str) -> bool:
        """
        Ping the listen key to keep it alive

        NOTE: Based on Binance pattern - Aster should support similar keep-alive mechanism
        """
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            await rest_assistant.execute_request(
                url=web_utils.private_rest_url(CONSTANTS.ASTER_PERPETUAL_USER_STREAM_ENDPOINT, domain=self._domain),
                method=RESTMethod.PUT,
                throttler_limit_id=CONSTANTS.ASTER_PERPETUAL_USER_STREAM_ENDPOINT,
                is_auth_required=True,
            )

            return True

        except Exception as e:
            self.logger().warning(f"Failed to ping listen key: {e}")
            return False

    async def _manage_listen_key_task_loop(self):
        """
        Manages the listen key lifecycle (creation and keep-alive)
        """
        try:
            while True:
                now = time.time()

                if self._current_listen_key is None:
                    self._current_listen_key = await self._get_listen_key()
                    self._last_listen_key_ping_ts = now
                    self._listen_key_initialized_event.set()
                    self.logger().info(f"Successfully created listen key: {self._current_listen_key}")

                elif now - self._last_listen_key_ping_ts >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    success = await self._ping_listen_key(self._current_listen_key)
                    if success:
                        self._last_listen_key_ping_ts = now
                        self.logger().info("Successfully pinged listen key")
                    else:
                        self.logger().warning("Failed to ping listen key, will recreate")
                        self._current_listen_key = None
                        self._listen_key_initialized_event.clear()
                        continue

                await asyncio.sleep(self.LISTEN_KEY_KEEP_ALIVE_INTERVAL)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().exception(f"Unexpected error in listen key management: {e}")
            raise

    async def _ensure_listen_key_task_running(self):
        """
        Ensures the listen key management task is running
        """
        if self._manage_listen_key_task is None or self._manage_listen_key_task.done():
            self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange.

        This method ensures the listen key is ready before connecting.
        """
        # Make sure the listen key management task is running
        await self._ensure_listen_key_task_running()

        # Wait for the listen key to be initialized
        await self._listen_key_initialized_event.wait()

        # Get a websocket assistant and connect it
        ws = await self._get_ws_assistant()
        url = f"{web_utils.wss_url(CONSTANTS.PRIVATE_WS_ENDPOINT, self._domain)}/{self._current_listen_key}"

        self.logger().info(f"Connecting to user stream with listen key {self._current_listen_key}")
        await ws.connect(ws_url=url, ping_timeout=self.HEARTBEAT_TIME_INTERVAL)
        self.logger().info("Successfully connected to user stream")

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        Aster follows Binance pattern - no explicit channel subscription needed for user streams.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        pass

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        """
        Handle user stream interruption by resetting listen key state
        """
        self._current_listen_key = None
        self._listen_key_initialized_event.clear()

        if self._manage_listen_key_task and not self._manage_listen_key_task.done():
            self._manage_listen_key_task.cancel()
            self._manage_listen_key_task = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger
