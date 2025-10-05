"""
Lighter Perpetual User Stream Data Source

This module handles the WebSocket user stream for Lighter perpetual/derivative trading.
It processes real-time updates for account data, orders, positions, and balances.

Based on Lighter's WebSocket API:
- Account updates via account_all/{account_index}
- Real-time order status changes
- Position updates and PnL changes
- Balance and collateral updates
"""

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_auth import LighterPerpetualAuth
import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils as web_utils
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import (
        LighterPerpetualDerivative,
    )


class LighterPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Data source for Lighter Perpetual private user stream via WebSocket.

    Handles real-time updates for:
    - Account balance and collateral changes
    - Order status changes (including fills and cancellations)
    - Position updates (including PnL changes)
    - Account status and metadata updates
    """

    HEARTBEAT_TIME_INTERVAL = 30.0
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: LighterPerpetualAuth,
        trading_pairs: List[str],
        connector: "LighterPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth: LighterPerpetualAuth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory
        self._last_recv_time = 0.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message
        """
        if hasattr(self, '_ws_assistant') and self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return self._last_recv_time

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        url = web_utils.wss_url(self._domain)
        await ws.connect(ws_url=url, ping_timeout=self.PING_TIMEOUT)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to account events for the user's account.

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            # Get account index from auth
            account_index = self._auth.account_index
            
            # Subscribe to account updates
            # Channel format: account_all/{account_index}
            account_payload = {
                "type": "subscribe",
                "channel": f"account_all/{account_index}"
            }
            
            subscribe_request: WSJSONRequest = WSJSONRequest(payload=account_payload)
            await ws.send(subscribe_request)
            
            self.logger().info(f"Subscribed to account updates for account index: {account_index}")
            
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to user streams...")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """
        Process incoming WebSocket messages and forward relevant ones to the queue.
        
        :param event_message: the event message received from the WebSocket
        :param queue: the queue to put the processed message
        """
        message_type = event_message.get("type")
        
        if message_type == "connected":
            # Connection confirmation - no action needed
            self.logger().debug("WebSocket connected successfully")
            
        elif message_type == "subscribed/account_all":
            # Account subscription confirmation with initial state
            self.logger().info("Successfully subscribed to account updates")
            # Forward the initial account state
            queue.put_nowait(event_message)
            
        elif message_type == "update/account_all":
            # Real-time account updates
            self.logger().debug("Received account update")
            queue.put_nowait(event_message)
            
        else:
            # Log unhandled message types for debugging
            self.logger().debug(f"Unhandled message type: {message_type}")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Process WebSocket messages with proper error handling and reconnection logic.
        """
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant,
                    queue=queue
                )
            except asyncio.TimeoutError:
                # Send ping on timeout to keep connection alive
                ping_request = WSJSONRequest(payload={"type": "ping"})
                await websocket_assistant.send(ping_request)
                self.logger().debug("Sent ping to keep WebSocket connection alive")
            except Exception as e:
                self.logger().error(f"Error processing WebSocket messages: {e}")
                # Let the parent class handle reconnection
                raise

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Get or create a WebSocket assistant instance.
        """
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Determine which channel an event message originated from.
        
        :param event_message: the event message
        :return: the channel name
        """
        message_type = event_message.get("type", "")
        channel = event_message.get("channel", "")
        
        if message_type in ["subscribed/account_all", "update/account_all"]:
            return "account_updates"
        elif channel.startswith("account_all:"):
            return "account_updates"
        else:
            return "unknown"
