"""
Backpack Perpetual User Stream Data Source

This module handles the WebSocket user stream for Backpack perpetual/derivative trading.
It processes real-time updates for orders, positions, and funding payments.

Based on the spot implementation with derivative-specific enhancements:
- Order updates via account.orderUpdate
- Position updates via account.positionUpdate
- Enhanced message processing for derivative events
"""

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import (
        BackpackPerpetualDerivative,
    )


class BackpackPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Data source for Backpack Perpetual private user stream via WebSocket.

    Handles real-time updates for:
    - Order status changes (including fills) via account.orderUpdate
    - Position updates (including PnL changes) via account.positionUpdate
    - Funding payment notifications (if available)
    - RFQ updates via account.rfqUpdate (can be added later if needed)
    """

    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: BackpackPerpetualAuth,
        trading_pairs: List[str],
        connector: "BackpackPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth: BackpackPerpetualAuth = auth
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
    def order_book_class(self):
        """
        Returns the order book class associated with this data source
        """
        from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_order_book import (
            BackpackPerpetualOrderBook,
        )

        return BackpackPerpetualOrderBook

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
        Creates a WebSocket connection to Backpack with authentication.

        WebSocket URL: wss://ws.backpack.exchange/
        Authentication: Ed25519 signature-based

        Returns:
            WSAssistant: Authenticated WebSocket connection
        """
        try:
            ws: WSAssistant = await self._get_ws_assistant()
            url = f"{CONSTANTS.WS_URL}/"
            await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)

            # Authenticate and subscribe to streams
            await self._authenticate_and_subscribe(ws)

            return ws
        except Exception as e:
            self.logger().error(f"Error connecting to WebSocket: {e}", exc_info=True)
            raise

    async def _authenticate_and_subscribe(self, ws: WSAssistant):
        """
        Authenticate WebSocket connection and subscribe to user streams.

        Backpack WebSocket authentication format:
        {
            "method": "SUBSCRIBE",
            "params": ["account.orderUpdate", "account.positionUpdate"],
            "signature": [...],  # Ed25519 signature array
            "timestamp": 1234567890,
            "window": 5000
        }
        """
        try:
            # Generate authentication parameters
            auth_params = self._auth.websocket_login_parameters()

            # Subscribe to account streams using the SAME format as spot
            # Derivatives use the same WebSocket stream names as spot trading
            streams = [
                "account.orderUpdate",     # Order status changes and fills
                "account.positionUpdate",  # Position changes and PnL updates
                # "account.rfqUpdate",     # RFQ updates (if needed later)
            ]

            # Add symbol-specific streams for derivatives
            for trading_pair in self._trading_pairs:
                symbol = self._connector.web_utils.convert_to_exchange_trading_pair(trading_pair)
                streams.extend([
                    f"account.orderUpdate.{symbol}",     # Symbol-specific order updates
                    f"account.positionUpdate.{symbol}",  # Symbol-specific position updates
                ])

            # Create subscription message with authentication parameters
            subscription_message = {
                "method": "SUBSCRIBE",
                "params": streams,
                "signature": auth_params["signature"],
                "timestamp": auth_params["timestamp"],
                "window": auth_params["window"]
            }

            subscribe_request: WSJSONRequest = WSJSONRequest(payload=subscription_message)

            await ws.send(subscribe_request)
            self.logger().info(f"Subscribed to Backpack user streams: {streams}")

        except Exception as e:
            self.logger().error(f"Error authenticating and subscribing: {e}", exc_info=True)
            raise

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribe to user stream channels.

        This method is called by the parent class after connection is established.
        The actual subscription is handled in _authenticate_and_subscribe.
        """
        try:
            self.logger().info("WebSocket user stream subscription completed")
        except Exception as e:
            self.logger().error(f"Error subscribing to channels: {e}", exc_info=True)
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Process incoming WebSocket messages and route them to the appropriate queue.

        Args:
            websocket_assistant: WebSocket assistant instance
            queue: Queue to put processed messages
        """
        try:
            async for ws_response in websocket_assistant.iter_messages():
                try:
                    data = ws_response.data

                    # Handle different message types
                    if isinstance(data, dict):
                        await self._process_event_message(data, queue)
                    elif isinstance(data, str):
                        # Handle ping/pong or other string messages
                        if data.lower() in ["ping", "pong"]:
                            self.logger().debug(f"Received WebSocket {data}")
                        else:
                            self.logger().debug(f"Received string message: {data}")
                    else:
                        self.logger().debug(f"Received unknown message type: {type(data)}")

                except Exception as e:
                    self.logger().error(f"Error processing WebSocket message: {e}", exc_info=True)
        except Exception as e:
            self.logger().error(f"Error processing WebSocket message: {e}", exc_info=True)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """
        Process individual event messages and route them to the appropriate queue.

        Expected message formats (based on real Backpack WebSocket API):

        - Order updates:
        {
            "stream": "account.orderUpdate",
            "data": {
                "E": 1754462606817679,     # Event time in microseconds
                "O": "USER",              # Origin of the update
                "S": "Bid",               # Side (Ask = Sell, Bid = Buy)
                "T": 1754462606815843,    # Engine timestamp in microseconds
                "V": "RejectTaker",       # Self trade prevention
                "X": "New",               # Order state (New, Filled, Cancelled, etc.)
                "Z": "0",                 # Cumulative filled quantity
                "c": 2115950059,          # Client order ID (32-bit integer)
                "e": "orderAccepted",     # Event type (orderAccepted, orderFilled, etc.)
                "f": "GTC",               # Time in force
                "i": "5012875717",        # Exchange order ID
                "o": "LIMIT",             # Order type (LIMIT, MARKET)
                "p": "140.00",            # Price
                "q": "0.01",              # Quantity
                "r": False,               # Reduce only flag
                "s": "SOL_USDC_PERP",     # Symbol (includes _PERP suffix)
                "t": None,                # Trade ID (when trade occurs)
                "z": "0",                 # Last filled quantity
                "n": "30",                # Fee amount (when filled)
                "N": "USDC"               # Fee symbol (when filled)
            }
        }

        - Position updates:
        {
            "stream": "account.positionUpdate",
            "data": {
                "B": "164.59",            # Entry price
                "E": 1754462601563677,    # Event time in microseconds
                "M": "163.81034805",      # Mark price
                "P": "-0.007796",         # PnL unrealized
                "Q": "0.01",              # Net exposure quantity
                "T": 1754462601563678,    # Engine timestamp in microseconds
                "b": "164.6398",          # Break even price
                "f": "0.02",              # Initial margin fraction
                "i": 4681711685,          # Position ID
                "l": "0",                 # Estimated liquidation price
                "m": "0.0125",            # Maintenance margin fraction
                "n": "1.6381034805",      # Net exposure notional
                "p": "0",                 # PnL realized
                "q": "0.01",              # Net quantity
                "s": "SOL_USDC_PERP"      # Symbol (includes _PERP suffix)
            }
        }

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
            if "orderUpdate" in stream:
                self.logger().debug(f"Received order update: {data}")
                queue.put_nowait(event_message)
            elif "positionUpdate" in stream:
                self.logger().debug(f"Received position update: {data}")
                queue.put_nowait(event_message)
            elif "rfqUpdate" in stream:
                self.logger().debug(f"Received RFQ update: {data}")
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
