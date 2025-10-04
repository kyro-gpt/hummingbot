"""
Lighter Perpetual Exchange Constants

This module contains all constants used by the Lighter Perpetual exchange connector including
API URLs, rate limits, order types, and other configuration values.
"""
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# Exchange Information
EXCHANGE_NAME = "lighter_perpetual"
DEFAULT_DOMAIN = "lighter_perpetual"
TESTNET_DOMAIN = "lighter_perpetual_testnet"

# Base URLs
REST_URL = {
    DEFAULT_DOMAIN: "https://mainnet.zklighter.elliot.ai",
    TESTNET_DOMAIN: "https://testnet.zklighter.elliot.ai"
}

WS_URL = {
    DEFAULT_DOMAIN: "wss://mainnet.zklighter.elliot.ai/stream",
    TESTNET_DOMAIN: "wss://testnet.zklighter.elliot.ai/stream"
}

# API Version Prefixes
API_VERSION = "/api/v1"

# Public API Endpoints
STATUS_PATH_URL = "/"
INFO_PATH_URL = "/info"
EXCHANGE_STATS_PATH_URL = "/exchangeStats"
ORDER_BOOKS_PATH_URL = "/orderBooks"
ORDER_BOOK_DETAILS_PATH_URL = "/orderBookDetails"
RECENT_TRADES_PATH_URL = "/recentTrades"
TRADES_PATH_URL = "/trades"
CANDLESTICKS_PATH_URL = "/candlesticks"
FUNDINGS_PATH_URL = "/fundings"

# Private API Endpoints
ACCOUNT_PATH_URL = "/account"
ACCOUNT_ACTIVE_ORDERS_PATH_URL = "/accountActiveOrders"
ACCOUNT_INACTIVE_ORDERS_PATH_URL = "/accountInactiveOrders"
SEND_TX_PATH_URL = "/sendTx"
SEND_TX_BATCH_PATH_URL = "/sendTxBatch"
NEXT_NONCE_PATH_URL = "/nextNonce"
ACCOUNT_TXS_PATH_URL = "/accountTxs"

# Transaction Types (from lighter SDK - CORRECT VALUES)
TX_TYPE_CHANGE_PUB_KEY = 8
TX_TYPE_CREATE_SUB_ACCOUNT = 9
TX_TYPE_CREATE_PUBLIC_POOL = 10
TX_TYPE_UPDATE_PUBLIC_POOL = 11
TX_TYPE_TRANSFER = 12
TX_TYPE_WITHDRAW = 13
TX_TYPE_CREATE_ORDER = 14
TX_TYPE_CANCEL_ORDER = 15
TX_TYPE_CANCEL_ALL_ORDERS = 16
TX_TYPE_MODIFY_ORDER = 17
TX_TYPE_MINT_SHARES = 18
TX_TYPE_BURN_SHARES = 19
TX_TYPE_UPDATE_LEVERAGE = 20

# Order Types (from lighter SDK)
ORDER_TYPE_LIMIT = 0
ORDER_TYPE_MARKET = 1
ORDER_TYPE_STOP_LOSS = 2
ORDER_TYPE_TAKE_PROFIT = 3

# Time in Force
TIME_IN_FORCE_GOOD_TILL_TIME = 0
TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 1
TIME_IN_FORCE_FILL_OR_KILL = 2
TIME_IN_FORCE_GOOD_TILL_CANCELLED = 3

# Order States Mapping (Lighter -> Hummingbot) - COMPLETE FROM OPENAPI.JSON
ORDER_STATE = {
    "in-progress": OrderState.PENDING_CREATE,
    "pending": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "canceled": OrderState.CANCELED,
    "canceled-post-only": OrderState.CANCELED,
    "canceled-reduce-only": OrderState.CANCELED,
    "canceled-position-not-allowed": OrderState.CANCELED,
    "canceled-margin-not-allowed": OrderState.CANCELED,
    "canceled-too-much-slippage": OrderState.CANCELED,
    "canceled-not-enough-liquidity": OrderState.CANCELED,
    "canceled-self-trade": OrderState.CANCELED,
    "canceled-expired": OrderState.CANCELED,
    "canceled-oco": OrderState.CANCELED,
    "canceled-child": OrderState.CANCELED,
    "canceled-liquidation": OrderState.CANCELED,
}

# WebSocket Channels
WS_CHANNEL_ORDER_BOOK = "orderbook"
WS_CHANNEL_TRADES = "trades"
WS_CHANNEL_ACCOUNT = "account"

# Rate Limits (Conservative estimates)
RATE_LIMITS = [
    # General rate limit
    RateLimit(limit_id="general", limit=100, time_interval=60),
    
    # Public endpoints
    RateLimit(limit_id=STATUS_PATH_URL, limit=100, time_interval=60),
    RateLimit(limit_id=ORDER_BOOKS_PATH_URL, limit=100, time_interval=60),
    RateLimit(limit_id=ORDER_BOOK_DETAILS_PATH_URL, limit=100, time_interval=60),
    RateLimit(limit_id=RECENT_TRADES_PATH_URL, limit=100, time_interval=60),
    RateLimit(limit_id=TRADES_PATH_URL, limit=100, time_interval=60),
    RateLimit(limit_id=CANDLESTICKS_PATH_URL, limit=100, time_interval=60),
    RateLimit(limit_id=FUNDINGS_PATH_URL, limit=100, time_interval=60),
    
    # Private endpoints (more conservative)
    RateLimit(limit_id=ACCOUNT_PATH_URL, limit=50, time_interval=60),
    RateLimit(limit_id=ACCOUNT_ACTIVE_ORDERS_PATH_URL, limit=50, time_interval=60),
    RateLimit(limit_id=SEND_TX_PATH_URL, limit=20, time_interval=60),  # Most restrictive for order operations
    RateLimit(limit_id=SEND_TX_BATCH_PATH_URL, limit=10, time_interval=60),
    RateLimit(limit_id=NEXT_NONCE_PATH_URL, limit=50, time_interval=60),
]

# Default fees (to be updated with actual values)
DEFAULT_FEES = {
    "maker_percent_fee_decimal": 0.0002,  # 0.02%
    "taker_percent_fee_decimal": 0.0005,  # 0.05%
}

# Heartbeat interval for WebSocket
HEARTBEAT_TIME_INTERVAL = 30.0

# Maximum order ID length
MAX_ORDER_ID_LEN = 32

# Broker ID for Hummingbot orders
BROKER_ID = "HBOT"

# Client order ID prefix
CLIENT_ORDER_ID_PREFIX = "HBOT"

# Funding rate update interval
FUNDING_RATE_UPDATE_INTERVAL = 60  # seconds

# Position modes
POSITION_MODE_ONE_WAY = "OneWay"
POSITION_MODE_HEDGE = "Hedge"

# Supported position modes
SUPPORTED_POSITION_MODES = [POSITION_MODE_ONE_WAY]

# Default leverage
DEFAULT_LEVERAGE = 1

# Minimum order amounts (to be updated with actual values from API)
MIN_ORDER_SIZE = 0.001
MIN_NOTIONAL_SIZE = 1.0
TICK_SIZE = 0.01
STEP_SIZE = 0.001

# Error messages
ORDER_NOT_EXIST_MESSAGE = "order not found"
UNKNOWN_ORDER_MESSAGE = "unknown order"
INSUFFICIENT_BALANCE_MESSAGE = "insufficient balance"

# Authentication constants
DEFAULT_10_MIN_AUTH_EXPIRY = 600  # 10 minutes in seconds
MINUTE = 60  # seconds
