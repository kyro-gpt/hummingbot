from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BackpackOrderBook(OrderBook):
    """
    Order book implementation for Backpack Exchange.

    Handles conversion of Backpack's order book data formats to Hummingbot's
    internal OrderBookMessage format.
    """

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message.

        Args:
            msg: The response from the exchange when requesting the order book snapshot
            timestamp: The snapshot timestamp
            metadata: A dictionary with extra information to add to the snapshot data

        Returns:
            A snapshot message with the snapshot information received from the exchange

        Backpack WebSocket depth snapshot format:
        {
        "asks": [
        [
        "21.9",
        "500.123"
        ],
        [
        "22.1",
        "2321.11"
        ]
        ],
        "bids": [
        [
        "20.12",
        "255.123"
        ],
        [
        "20.5",
        "499.555"
        ]
        ],
        "lastUpdateId": "1684026955123",
        "timestamp": 1684026955123
        }
        """
        if metadata:
            msg.update(metadata)

        # Extract data based on Backpack's API response format
        # Backpack returns: {"bids": [[price, size], ...], "asks": [[price, size], ...], "lastUpdateId": id}
        content = {
            "trading_pair": msg["trading_pair"],
            "update_id": int(msg.get("lastUpdateId", 0)),  # Convert to integer
            "bids": msg.get("bids", []),
            "asks": msg.get("asks", [])
        }

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a diff message with the changes in the order book received from the exchange.

        Args:
            msg: The changes in the order book
            timestamp: The timestamp of the difference
            metadata: A dictionary with extra information to add to the difference data

        Returns:
            A diff message with the changes in the order book notified by the exchange

        Backpack WebSocket depth updates format:

        {
        "e": "depth",           // Event type
        "E": 1694687965941000,  // Event time in microseconds
        "s": "SOL_USDC",        // Symbol
        "a": [                  // Asks
            [
            "18.70",
            "0.000"
            ]
        ],
        "b": [                  // Bids
            [
            "18.67",
            "0.832"
            ],
            [
            "18.68",
            "0.000"
            ]
        ],
        "U": 94978271,          // First update ID in event
        "u": 94978271,          // Last update ID in event
        "T": 1694687965940999   // Engine timestamp in microseconds
        }
        """
        if metadata:
            msg.update(metadata)

        # Extract data from Backpack WebSocket depth update format
        content = {
            "trading_pair": msg["trading_pair"],
            "update_id": int(msg.get("u", 0)),           # Last update ID in event - convert to integer
            "first_update_id": int(msg.get("U", 0)),    # First update ID in event - convert to integer
            "bids": msg.get("b", []),               # Bids array
            "asks": msg.get("a", [])                # Asks array
        }

        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, any],
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a trade message from exchange trade data.

        Args:
            msg: The trade data from the exchange
            metadata: A dictionary with extra information to add to the trade data

        Returns:
            A trade message with the trade information received from the exchange

        {
        "e": "trade",                   // Event type
        "E": 1694688638091000,          // Event time in microseconds
        "s": "SOL_USDC",                // Symbol
        "p": "18.68",                   // Price
        "q": "0.122",                   // Quantity
        "b": "111063114377265150",      // Buyer order ID
        "a": "111063114585735170",      // Seller order ID
        "t": 12345,                     // Trade ID
        "T": 1694688638089000,          // Engine timestamp in microseconds
        "m": true                       // Is the buyer the maker?
        }
        """
        if metadata:
            msg.update(metadata)

        # Extract trade information from Backpack format
        trade_id = msg.get("t", 0)
        price = float(msg.get("p", 0))
        quantity = float(msg.get("q", 0))

        # Determine trade type from the maker flag
        # If buyer is maker (m=true), then this trade is a sell (someone sold to the maker)
        # If buyer is not maker (m=false), then this trade is a buy (someone bought from the maker)
        is_buyer_maker = msg.get("m", False)
        trade_type = float(TradeType.SELL.value) if is_buyer_maker else float(TradeType.BUY.value)

        content = {
            "trading_pair": msg["trading_pair"],
            "trade_type": trade_type,
            "amount": quantity,
            "price": price,
            "trade_id": trade_id,
            "update_id": trade_id  # Use trade_id as update_id
        }

        # Use the engine timestamp (T) if available, otherwise event time (E)
        timestamp = msg.get("T", msg.get("E", 0))
        if timestamp:
            # Convert from microseconds to seconds
            if timestamp > 1e12:  # If timestamp is in microseconds
                timestamp = timestamp / 1e6
            elif timestamp > 1e10:  # If timestamp is in milliseconds
                timestamp = timestamp / 1000.0
        else:
            import time
            timestamp = time.time()

        return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=timestamp)
