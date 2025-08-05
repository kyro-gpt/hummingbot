"""
Backpack Exchange Constants

This module contains all constants used by the Backpack exchange connector including
API URLs, rate limits, order types, and other configuration values.
"""
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType

# Order State Mapping for Hummingbot (Backpack status -> OrderState)
from hummingbot.core.data_type.in_flight_order import OrderState

# Exchange Information
EXCHANGE_NAME = "backpack"
DEFAULT_DOMAIN = "backpack"

# Base URLs
REST_URL = "https://api.backpack.exchange"
WS_URL = "wss://ws.backpack.exchange"

# API Version Prefixes
API_VERSION = "/api/v1"
WAPI_VERSION = "/wapi/v1"

# Public API Endpoints
PING_PATH_URL = "/ping"
STATUS_PATH_URL = "/status"
TIME_PATH_URL = "/time"
MARKETS_PATH_URL = "/markets"
MARKET_PATH_URL = "/market"
TICKER_PATH_URL = "/ticker"
TICKERS_PATH_URL = "/tickers"
DEPTH_PATH_URL = "/depth"
KLINES_PATH_URL = "/klines"
TRADES_PATH_URL = "/trades"
TRADES_HISTORY_PATH_URL = "/trades/history"
MARK_PRICES_PATH_URL = "/markPrices"
FUNDING_RATES_PATH_URL = "/fundingRates"
OPEN_INTEREST_PATH_URL = "/openInterest"

# Private API Endpoints
ACCOUNT_PATH_URL = "/account"
CAPITAL_PATH_URL = "/capital"
WALLETS_PATH_URL = "/wallets"
ASSETS_PATH_URL = "/assets"
COLLATERAL_PATH_URL = "/collateral"
POSITION_PATH_URL = "/position"
ORDER_PATH_URL = "/order"
ORDERS_PATH_URL = "/orders"

# WAPI (Wallet API) Endpoints
DEPOSITS_PATH_URL = "/capital/deposits"
DEPOSIT_ADDRESS_PATH_URL = "/capital/deposit/address"
WITHDRAWALS_PATH_URL = "/capital/withdrawals"

# History Endpoints
HISTORY_ORDERS_PATH_URL = "/history/orders"
HISTORY_FILLS_PATH_URL = "/history/fills"
HISTORY_FUNDING_PATH_URL = "/history/funding"
HISTORY_PNL_PATH_URL = "/history/pnl"
HISTORY_DUST_PATH_URL = "/history/dust"
HISTORY_BORROW_LEND_PATH_URL = "/history/borrowLend"

# Borrow/Lend Endpoints
BORROW_LEND_POSITIONS_PATH_URL = "/borrowLend/positions"
BORROW_LEND_MARKETS_PATH_URL = "/borrowLend/markets"

# Account Limits
ACCOUNT_LIMITS_BORROW_PATH_URL = "/account/limits/borrow"
ACCOUNT_LIMITS_ORDER_PATH_URL = "/account/limits/order"
ACCOUNT_LIMITS_WITHDRAWAL_PATH_URL = "/account/limits/withdrawal"

# WebSocket Channels
WS_DEPTH_CHANNEL = "depth"
WS_TICKER_CHANNEL = "ticker"
WS_TRADES_CHANNEL = "trades"
WS_KLINES_CHANNEL = "klines"
WS_FILLS_CHANNEL = "fills"
WS_ORDERS_CHANNEL = "orders"
WS_ACCOUNT_CHANNEL = "account"

# Rate Limit IDs
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS_WEIGHT = "ORDERS_WEIGHT"
RAW_REQUESTS = "RAW_REQUESTS"

# Rate Limits (conservative initial values - will be adjusted based on testing)
RATE_LIMITS = [
    # Default rate limit for most endpoints
    RateLimit(limit_id=REQUEST_WEIGHT, limit=1000, time_interval=60, weight=1),

    # More restrictive limit for order operations
    RateLimit(limit_id=ORDERS_WEIGHT, limit=100, time_interval=10, weight=1),

    # Raw request limit (fallback)
    RateLimit(limit_id=RAW_REQUESTS, limit=6000, time_interval=60, weight=1),

    # Specific endpoint limits

    # Public endpoints (higher limits)
    RateLimit(limit_id=PING_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=MARKETS_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=TICKER_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=TRADES_PATH_URL, limit=1000, time_interval=60, weight=1),

    # Private endpoints (more restrictive)
    RateLimit(limit_id=ACCOUNT_PATH_URL, limit=100, time_interval=60, weight=1),
    RateLimit(limit_id=CAPITAL_PATH_URL, limit=100, time_interval=60, weight=1),
    RateLimit(limit_id=ORDERS_PATH_URL, limit=50, time_interval=10, weight=1),
    RateLimit(limit_id=ORDER_PATH_URL, limit=100, time_interval=10, weight=1),
    RateLimit(limit_id=POSITION_PATH_URL, limit=100, time_interval=60, weight=1),

    # History endpoints
    RateLimit(limit_id=HISTORY_ORDERS_PATH_URL, limit=100, time_interval=60, weight=1),
    RateLimit(limit_id=HISTORY_FILLS_PATH_URL, limit=100, time_interval=60, weight=1),
    RateLimit(limit_id=HISTORY_FUNDING_PATH_URL, limit=100, time_interval=60, weight=1),
]

# Authentication Instruction Mapping for API endpoints
# Format: (path, method) -> instruction_type
INSTRUCTION_MAPPING = {
    # Balance and wallet
    (CAPITAL_PATH_URL, "GET"): "balanceQuery",
    (DEPOSITS_PATH_URL, "GET"): "depositQueryAll",
    (DEPOSIT_ADDRESS_PATH_URL, "GET"): "depositAddressQuery",
    (WITHDRAWALS_PATH_URL, "GET"): "withdrawalQueryAll",
    (WITHDRAWALS_PATH_URL, "POST"): "withdraw",

    # Orders
    (ORDER_PATH_URL, "GET"): "orderQuery",
    (ORDER_PATH_URL, "POST"): "orderExecute",
    (ORDER_PATH_URL, "DELETE"): "orderCancel",
    (ORDERS_PATH_URL, "GET"): "orderQueryAll",
    (ORDERS_PATH_URL, "POST"): "orderExecute",  # Batch orders
    (ORDERS_PATH_URL, "DELETE"): "orderCancelAll",

    # History
    (HISTORY_ORDERS_PATH_URL, "GET"): "orderHistoryQueryAll",
    (HISTORY_FILLS_PATH_URL, "GET"): "fillHistoryQueryAll",
    (HISTORY_FUNDING_PATH_URL, "GET"): "fundingHistoryQueryAll",
    (HISTORY_PNL_PATH_URL, "GET"): "pnlHistoryQueryAll",
    (HISTORY_DUST_PATH_URL, "GET"): "dustHistoryQueryAll",
    (HISTORY_BORROW_LEND_PATH_URL, "GET"): "borrowHistoryQueryAll",

    # Account
    (ACCOUNT_PATH_URL, "GET"): "accountQuery",
    (ACCOUNT_PATH_URL, "PATCH"): "accountUpdate",
    ("/account/convertDust", "POST"): "convertDust",
    (ACCOUNT_LIMITS_BORROW_PATH_URL, "GET"): "maxBorrowQuantity",
    (ACCOUNT_LIMITS_ORDER_PATH_URL, "GET"): "maxOrderQuantity",
    (ACCOUNT_LIMITS_WITHDRAWAL_PATH_URL, "GET"): "maxWithdrawalQuantity",

    # Positions
    (POSITION_PATH_URL, "GET"): "positionQuery",
    (BORROW_LEND_POSITIONS_PATH_URL, "GET"): "borrowLendPositionQuery",

    # Collateral
    (COLLATERAL_PATH_URL, "GET"): "collateralQuery",

    # Assets
    (ASSETS_PATH_URL, "GET"): "assetQuery",
    (WALLETS_PATH_URL, "GET"): "walletQuery",
}

# Order Types Mapping
BACKPACK_ORDER_TYPE = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
    OrderType.LIMIT_MAKER: "Limit",  # Backpack uses Post-only for maker orders
}

HB_ORDER_TYPE = {
    "Limit": OrderType.LIMIT,
    "Market": OrderType.MARKET,
}

# Order Sides
BACKPACK_ORDER_SIDE = {
    "BUY": "Bid",
    "SELL": "Ask",
}

HB_ORDER_SIDE = {
    "Bid": "BUY",
    "Ask": "SELL",
}

# Order Status Mapping
BACKPACK_ORDER_STATUS = {
    "New": "NEW",
    "PartiallyFilled": "PARTIALLY_FILLED",
    "Filled": "FILLED",
    "Cancelled": "CANCELED",
    "Expired": "EXPIRED",
}

HB_ORDER_STATUS = {
    "NEW": "NEW",
    "PARTIALLY_FILLED": "PARTIALLY_FILLED",
    "FILLED": "FILLED",
    "CANCELED": "CANCELED",
    "EXPIRED": "EXPIRED",
}

ORDER_STATE = {
    "New": OrderState.OPEN,
    "PartiallyFilled": OrderState.PARTIALLY_FILLED,
    "Filled": OrderState.FILLED,
    "Cancelled": OrderState.CANCELED,  # Note: CANCELED not CANCELLED
    "Expired": OrderState.FAILED,
    "Rejected": OrderState.FAILED,
}

# Time in Force
BACKPACK_TIME_IN_FORCE = {
    "GTC": "GTC",  # Good Till Canceled
    "IOC": "IOC",  # Immediate or Cancel
    "FOK": "FOK",  # Fill or Kill
}

# Market Types
MARKET_TYPE_SPOT = "spot"
MARKET_TYPE_FUTURE = "future"

# Client Order ID Configuration
MAX_ORDER_ID_LEN = 32  # For string-based IDs (not used by Backpack)
HBOT_ORDER_ID_PREFIX = "HBOT"  # For string-based IDs (not used by Backpack)

# Backpack uses 32-bit unsigned integer client IDs
MAX_CLIENT_ID_BITS = 32  # 32-bit unsigned integer (0 to 4294967295)

# Default Values
DEFAULT_WINDOW = 5000  # 5 seconds
DEFAULT_FEES = {
    "maker": 0.0,  # Will be updated from market info
    "taker": 0.0,  # Will be updated from market info
}

# Error Codes (from API documentation)
API_ERROR_CODES = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    429: "Too Many Requests",
    500: "Internal Server Error",
    503: "Service Unavailable",
}

# Minimum Order Sizes (will be updated from market info)
MIN_ORDER_SIZE = 0.001
MIN_NOTIONAL_SIZE = 1.0

# Price/Size Precision (will be updated from market info)
DEFAULT_PRICE_PRECISION = 8
DEFAULT_SIZE_PRECISION = 8

# WebSocket Configuration
WS_HEARTBEAT_TIME_INTERVAL = 30.0
WS_MESSAGE_TIMEOUT = 30.0
WS_CONNECTION_TIMEOUT = 10.0

# Supported Order Types
SUPPORTED_ORDER_TYPES = [OrderType.LIMIT, OrderType.MARKET]

# Connector Configuration
CONNECTOR_NAME = EXCHANGE_NAME
IS_CENTRALIZED = True
IS_DEX = False
