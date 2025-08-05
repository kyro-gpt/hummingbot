import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange


class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Data source for Backpack private user stream via WebSocket.

    Handles real-time updates for:
    - Order status changes (including fills) via account.orderUpdate
    - Position updates (including balance info) via account.positionUpdate
    - RFQ updates via account.rfqUpdate (can be added later if needed)
    """

    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: BackpackAuth,
                 trading_pairs: List[str],
                 connector: 'BackpackExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: BackpackAuth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def order_book_class(self):
        """Returns the order book class for this exchange (not used in user stream)."""
        return None

    @property
    def last_recv_time(self) -> float:
        """Return the last time a message was received."""
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates and returns a connected WebSocket assistant for the private user stream.

        Returns:
            WSAssistant: Connected WebSocket assistant
        """
        try:
            ws: WSAssistant = await self._api_factory.get_ws_assistant()
            await ws.connect(
                ws_url=CONSTANTS.WS_URL,
                ping_timeout=self.HEARTBEAT_TIME_INTERVAL
            )

            # Authenticate the WebSocket connection
            await self._authenticate_websocket(ws)

            return ws
        except Exception as e:
            self.logger().error(f"Error connecting to Backpack user stream: {e}")
            raise

    async def _authenticate_websocket(self, ws: WSAssistant):
        """
        Authenticate the WebSocket connection with Backpack.

        Note: For Backpack, authentication happens during subscription, not separately.
        This method is kept for compatibility but doesn't send a separate auth message.

        Args:
            ws: WebSocket assistant to authenticate
        """
        try:
            # Backpack doesn't use separate authentication - it's done during SUBSCRIBE
            # Just log that we're ready for subscription
            self.logger().info("WebSocket ready for authenticated subscriptions")

        except Exception as e:
            self.logger().error(f"Error preparing WebSocket for auth: {e}")
            raise

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribe to private user data channels using Backpack's authentication format.

        Backpack requires authentication on each subscription with signature array format:
        [verifying_key, signature, timestamp, window]

        Args:
            websocket_assistant: Connected WebSocket assistant
        """
        try:
            # Get authenticated subscription parameters
            auth_params = self._auth.websocket_login_parameters()

            # Use the exact same streams that worked in discovery test
            streams = [
                "account.orderUpdate",
                "account.positionUpdate",
                "account.rfqUpdate",
                "account.orderUpdate.SOL_USDC",
                "account.positionUpdate.SOL_USDC",
                "account.orderUpdate.SOL_USDC_SPOT",
            ]

            # Create authenticated subscription payload following Backpack's format
            subscription_payload = {
                "method": "SUBSCRIBE",
                "params": streams,
                "signature": auth_params["signature"]
            }

            # Send subscription request (authentication already included in payload)
            subscribe_request = WSJSONRequest(payload=subscription_payload, is_auth_required=False)
            await websocket_assistant.send(subscribe_request)

            self.logger().info(f"Subscribed to Backpack private channels: {streams}")
            self.logger().info(f"Subscription payload: {subscription_payload}")

            # Wait for subscription confirmation and log any immediate responses
            print("   🔍 Waiting for subscription responses...")
            try:
                for i in range(3):  # Check for 3 seconds
                    message = await asyncio.wait_for(websocket_assistant.receive(), timeout=1.0)
                    if message:
                        print(f"      📩 Subscription response {i+1}: {message}")
                    await asyncio.sleep(1)
            except asyncio.TimeoutError:
                print("      ⏱️ No immediate subscription responses")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Unexpected error subscribing to user streams: {e}")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Process incoming WebSocket messages and add them to the queue.

        Args:
            websocket_assistant: Connected WebSocket assistant
            queue: Queue to add processed messages to
        """
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant,
                    queue=queue
                )
            except asyncio.TimeoutError:
                # Send ping to keep connection alive (similar to OKX pattern)
                ping_request = WSPlainTextRequest(payload="ping")
                await websocket_assistant.send(request=ping_request)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error processing WebSocket messages: {e}")
                await self._sleep(5.0)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """
        Process individual event messages and route them to the appropriate queue.

        Expected message formats (based on Backpack WebSocket API):
        - Order updates: {"stream": "account.orderUpdate", "data": {...}}
        - Fill updates: {"stream": "account.fills", "data": {...}}
        - Balance updates: {"stream": "account.balances", "data": {...}}

        Args:
            event_message: Raw WebSocket event message
            queue: Queue to add processed events to
        """
        try:
            # Check if this is a valid event message
            if not isinstance(event_message, dict):
                return

            # Handle subscription confirmation messages
            if "id" in event_message and "result" in event_message:
                if event_message.get("result") is True:
                    self.logger().info(f"Successfully subscribed to channel (id: {event_message['id']})")
                else:
                    self.logger().warning(f"Subscription failed for id {event_message['id']}: {event_message}")
                return

            # Handle stream data messages
            stream = event_message.get("stream", "")
            data = event_message.get("data", {})

            if not stream or not data:
                return

            # Route messages based on stream type
            if "account.orderUpdate" in stream:
                self.logger().debug(f"Received order update: {data}")
                queue.put_nowait(event_message)
            elif "account.fills" in stream:
                self.logger().debug(f"Received fill update: {data}")
                queue.put_nowait(event_message)
            elif "account.balances" in stream:
                self.logger().debug(f"Received balance update: {data}")
                queue.put_nowait(event_message)
            else:
                self.logger().debug(f"Received unknown stream message: {stream}")

        except Exception as e:
            self.logger().error(f"Error processing event message: {e}")

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Creates a new WebSocket assistant instance.

        Returns:
            WSAssistant: New WebSocket assistant
        """
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _sleep(self, delay: float):
        """
        Sleep for the specified delay.

        Args:
            delay: Sleep duration in seconds
        """
        await asyncio.sleep(delay)
