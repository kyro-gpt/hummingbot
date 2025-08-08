"""
Backpack Perpetual Order Book

This module provides order book functionality for Backpack derivative trading.
Since Backpack uses the same order book format for both spot and derivative trading,
we can reuse the existing BackpackOrderBook implementation.
"""

# For now, we'll import and re-export the spot BackpackOrderBook since the format is identical
# If needed in the future, we can extend it with derivative-specific functionality
from hummingbot.connector.exchange.backpack.backpack_order_book import BackpackOrderBook

# Alias for clarity and future extensibility
BackpackPerpetualOrderBook = BackpackOrderBook

# If we need derivative-specific order book functionality in the future, we can extend here:
# class BackpackPerpetualOrderBook(BackpackOrderBook):
#     """
#     Backpack Perpetual Order Book with derivative-specific enhancements.
#     """
#
#     def __init__(self):
#         super().__init__()
#         # Add derivative-specific initialization if needed
#
#     # Add derivative-specific methods here if needed
