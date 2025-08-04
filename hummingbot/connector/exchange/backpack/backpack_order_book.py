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
        """
        if metadata:
            msg.update(metadata)

        # Extract data based on Backpack's API response format
        # Backpack returns: {"bids": [[price, size], ...], "asks": [[price, size], ...], "lastUpdateId": id}
        content = {
            "trading_pair": msg["trading_pair"],
            "update_id": msg.get("lastUpdateId", 0),
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
        """
        if metadata:
            msg.update(metadata)

        # Backpack WebSocket depth updates format:
        # {"bids": [[price, size], ...], "asks": [[price, size], ...], "u": update_id, "U": first_update_id}
        content = {
            "trading_pair": msg["trading_pair"],
            "update_id": msg.get("u", 0),
            "first_update_id": msg.get("U", 0),
            "bids": msg.get("bids", []),
            "asks": msg.get("asks", [])
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
        """
        if metadata:
            msg.update(metadata)

        # Backpack trade message format:
        # {"symbol": "SOL_USDC", "price": "100.5", "quantity": "1.5", "side": "Bid", "timestamp": 1234567890}
        content = {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.BUY.value) if msg.get("side") == "Bid" else float(TradeType.SELL.value),
            "amount": float(msg.get("quantity", 0)),
            "price": float(msg.get("price", 0)),
            "update_id": msg.get("timestamp", 0)
        }

        # Use the trade timestamp if available, otherwise use current time
        timestamp = msg.get("timestamp", 0)
        if timestamp:
            # Convert from milliseconds to seconds if needed
            if timestamp > 1e10:  # If timestamp is in milliseconds
                timestamp = timestamp / 1000.0
        else:
            import time
            timestamp = time.time()

        return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=timestamp)
