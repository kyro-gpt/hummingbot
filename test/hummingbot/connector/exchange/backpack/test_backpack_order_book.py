from unittest import TestCase

from hummingbot.connector.exchange.backpack.backpack_order_book import BackpackOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class BackpackOrderBookTests(TestCase):

    def test_snapshot_message_from_exchange(self):
        # Real Backpack WebSocket depth snapshot format
        snapshot_message = BackpackOrderBook.snapshot_message_from_exchange(
            msg={
                "asks": [
                    ["101.0", "15.0"],
                    ["101.5", "20.0"]
                ],
                "bids": [
                    ["100.5", "10.0"],
                    ["100.0", "5.0"]
                ],
                "lastUpdateId": "12345",
                "timestamp": 1684026955123
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "SOL-USDC"}
        )

        self.assertEqual("SOL-USDC", snapshot_message.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_message.type)
        self.assertEqual(1640000000.0, snapshot_message.timestamp)
        self.assertEqual("12345", snapshot_message.update_id)  # String in real format
        self.assertEqual(-1, snapshot_message.trade_id)

        # Check bids
        self.assertEqual(2, len(snapshot_message.bids))
        self.assertEqual(100.5, snapshot_message.bids[0].price)
        self.assertEqual(10.0, snapshot_message.bids[0].amount)
        self.assertEqual("12345", snapshot_message.bids[0].update_id)

        # Check asks
        self.assertEqual(2, len(snapshot_message.asks))
        self.assertEqual(101.0, snapshot_message.asks[0].price)
        self.assertEqual(15.0, snapshot_message.asks[0].amount)
        self.assertEqual("12345", snapshot_message.asks[0].update_id)

    def test_diff_message_from_exchange(self):
        # Real Backpack WebSocket depth update format
        diff_msg = BackpackOrderBook.diff_message_from_exchange(
            msg={
                "e": "depth",                   # Event type
                "E": 1694687965941000,          # Event time in microseconds
                "s": "SOL_USDC",                # Symbol
                "a": [                          # Asks
                    ["102.0", "12.0"]
                ],
                "b": [                          # Bids
                    ["99.5", "8.0"]
                ],
                "U": 100,                       # First update ID in event
                "u": 102,                       # Last update ID in event
                "T": 1694687965940999           # Engine timestamp in microseconds
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "SOL-USDC"}
        )

        self.assertEqual("SOL-USDC", diff_msg.trading_pair)
        self.assertEqual(OrderBookMessageType.DIFF, diff_msg.type)
        self.assertEqual(1640000000.0, diff_msg.timestamp)
        self.assertEqual(102, diff_msg.update_id)
        self.assertEqual(100, diff_msg.first_update_id)
        self.assertEqual(-1, diff_msg.trade_id)

        # Check bids
        self.assertEqual(1, len(diff_msg.bids))
        self.assertEqual(99.5, diff_msg.bids[0].price)
        self.assertEqual(8.0, diff_msg.bids[0].amount)
        self.assertEqual(102, diff_msg.bids[0].update_id)

        # Check asks
        self.assertEqual(1, len(diff_msg.asks))
        self.assertEqual(102.0, diff_msg.asks[0].price)
        self.assertEqual(12.0, diff_msg.asks[0].amount)
        self.assertEqual(102, diff_msg.asks[0].update_id)

    def test_trade_message_from_exchange(self):
        # Real Backpack trade message format
        trade_update = {
            "e": "trade",                   # Event type
            "E": 1694688638091000,          # Event time in microseconds
            "s": "SOL_USDC",                # Symbol
            "p": "100.25",                  # Price
            "q": "5.5",                     # Quantity
            "b": "111063114377265150",      # Buyer order ID
            "a": "111063114585735170",      # Seller order ID
            "t": 12345,                     # Trade ID
            "T": 1694688638089000,          # Engine timestamp in microseconds
            "m": False                      # Is the buyer the maker? (False = buy trade)
        }

        trade_message = BackpackOrderBook.trade_message_from_exchange(
            msg=trade_update,
            metadata={"trading_pair": "SOL-USDC"}
        )

        self.assertEqual("SOL-USDC", trade_message.trading_pair)
        self.assertEqual(OrderBookMessageType.TRADE, trade_message.type)
        self.assertEqual(1694688638.089, trade_message.timestamp)  # converted from microseconds
        self.assertEqual(-1, trade_message.update_id)
        self.assertEqual(-1, trade_message.first_update_id)
        self.assertEqual(12345, trade_message.trade_id)

        # Check trade content
        content = trade_message.content
        self.assertEqual(1.0, content["trade_type"])  # Buy trade (buyer not maker)
        self.assertEqual(5.5, content["amount"])
        self.assertEqual(100.25, content["price"])

    def test_trade_message_sell_side(self):
        # Real Backpack trade message format - sell trade
        trade_update = {
            "e": "trade",                   # Event type
            "E": 1694688638092000,          # Event time in microseconds
            "s": "SOL_USDC",                # Symbol
            "p": "99.75",                   # Price
            "q": "3.0",                     # Quantity
            "b": "111063114377265151",      # Buyer order ID
            "a": "111063114585735171",      # Seller order ID
            "t": 12346,                     # Trade ID
            "T": 1694688638090000,          # Engine timestamp in microseconds
            "m": True                       # Is the buyer the maker? (True = sell trade)
        }

        trade_message = BackpackOrderBook.trade_message_from_exchange(
            msg=trade_update,
            metadata={"trading_pair": "SOL-USDC"}
        )

        # Check trade content for sell side
        content = trade_message.content
        self.assertEqual(2.0, content["trade_type"])  # Sell trade (buyer is maker)
        self.assertEqual(3.0, content["amount"])
        self.assertEqual(99.75, content["price"])
        self.assertEqual(1694688638.090, trade_message.timestamp)  # converted from microseconds
