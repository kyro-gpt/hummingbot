"""
Backpack Perpetual Exchange Constants

This module contains all constants used by the Backpack perpetual/derivative exchange connector
including API URLs, rate limits, order types, funding configuration, and position management.

Based on spot trading constants with derivative-specific additions.
"""
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionMode
from hummingbot.core.data_type.in_flight_order import OrderState

# Exchange Information
EXCHANGE_NAME = "backpack_perpetual"
DEFAULT_DOMAIN = "backpack_perpetual"

# Base URLs (same as spot)
REST_URL = "https://api.backpack.exchange"
WS_URL = "wss://ws.backpack.exchange"

# API Version Prefixes (same as spot)
API_VERSION = "/api/v1"
WAPI_VERSION = "/wapi/v1"

# Public API Endpoints (reuse from spot)
PING_PATH_URL = "/api/v1/ping"
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

# Derivative-specific public endpoints
MARK_PRICES_PATH_URL = "/markPrices"        # ✅ CONFIRMED: Returns mark price + current funding rate
FUNDING_RATES_PATH_URL = "/fundingRates"    # ✅ CONFIRMED: Historical funding rates by symbol
OPEN_INTEREST_PATH_URL = "/openInterest"
MARK_PRICE_PATH_URL = "/markPrice"          # May not exist - check during implementation
INDEX_PRICE_PATH_URL = "/indexPrice"        # May not exist - check during implementation

# Private API Endpoints (reuse from spot)
ACCOUNT_PATH_URL = "/account"
CAPITAL_PATH_URL = "/capital"
WALLETS_PATH_URL = "/wallets"
ASSETS_PATH_URL = "/assets"
COLLATERAL_PATH_URL = "/collateral"
ORDER_PATH_URL = "/order"
ORDERS_PATH_URL = "/orders"

# Derivative-specific private endpoints
POSITION_PATH_URL = "/position"  # Critical for position management
POSITIONS_PATH_URL = "/positions"  # If available

# WAPI (Wallet API) Endpoints (reuse from spot)
DEPOSITS_PATH_URL = "/capital/deposits"
DEPOSIT_ADDRESS_PATH_URL = "/capital/deposit/address"
WITHDRAWALS_PATH_URL = "/capital/withdrawals"

# History Endpoints (extend from spot)
HISTORY_ORDERS_PATH_URL = "/history/orders"
HISTORY_FILLS_PATH_URL = "/history/fills"
HISTORY_FUNDING_PATH_URL = "/history/funding"  # Derivative-specific
HISTORY_PNL_PATH_URL = "/history/pnl"          # Derivative-specific
HISTORY_DUST_PATH_URL = "/history/dust"
HISTORY_BORROW_LEND_PATH_URL = "/history/borrowLend"

# Additional derivative endpoints (if available in Backpack)
LEVERAGE_PATH_URL = "/leverage"  # May not exist - will check during implementation
POSITION_MODE_PATH_URL = "/positionMode"  # May not exist - will check during implementation

# Borrow/Lend Endpoints (reuse from spot)
BORROW_LEND_POSITIONS_PATH_URL = "/borrowLend/positions"
BORROW_LEND_MARKETS_PATH_URL = "/borrowLend/markets"

# Account Limits (reuse from spot)
ACCOUNT_LIMITS_BORROW_PATH_URL = "/account/limits/borrow"
ACCOUNT_LIMITS_ORDER_PATH_URL = "/account/limits/order"
ACCOUNT_LIMITS_WITHDRAWAL_PATH_URL = "/account/limits/withdrawal"

# WebSocket Channels (extend from spot)
WS_DEPTH_CHANNEL = "depth"
WS_TICKER_CHANNEL = "ticker"
WS_TRADES_CHANNEL = "trades"
WS_KLINES_CHANNEL = "klines"
WS_FILLS_CHANNEL = "fills"
WS_ORDERS_CHANNEL = "orders"
WS_ACCOUNT_CHANNEL = "account"

# Derivative-specific WebSocket channels
WS_POSITION_CHANNEL = "position"       # Position updates (may not exist - verify)
# ❌ NO FUNDING WEBSOCKET STREAMS FOUND IN OPENAPI SPEC
# WS_FUNDING_CHANNEL = "funding"         # Funding rate updates - NOT AVAILABLE
# WS_MARK_PRICE_CHANNEL = "markPrice"    # Mark price updates - NOT AVAILABLE
# WS_LIQUIDATION_CHANNEL = "liquidation" # Liquidation notifications - NOT AVAILABLE

# Note: Funding data will be retrieved via REST API polling only

# Funding Configuration
FUNDING_FEE_POLL_INTERVAL = 300  # 5 minutes (300 seconds) - conservative starting point
FUNDING_SETTLEMENT_DURATION = (30, 30)  # 30 seconds before/after funding time

# Position Configuration
SUPPORTED_POSITION_MODES = [PositionMode.ONEWAY]  # Start with ONEWAY, add HEDGE if supported
DEFAULT_POSITION_MODE = PositionMode.ONEWAY

# Leverage Configuration (will be updated based on Backpack limits)
DEFAULT_LEVERAGE = 1
MAX_LEVERAGE = 20  # Conservative default - will be updated from API
MIN_LEVERAGE = 1

# Rate Limit IDs (reuse from spot)
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS_WEIGHT = "ORDERS_WEIGHT"
RAW_REQUESTS = "RAW_REQUESTS"
POSITIONS_WEIGHT = "POSITIONS_WEIGHT"  # New for derivatives
FUNDING_WEIGHT = "FUNDING_WEIGHT"      # New for derivatives

# Rate Limits (conservative values - extend from spot)
RATE_LIMITS = [
    # Default rate limit for most endpoints
    RateLimit(limit_id=REQUEST_WEIGHT, limit=1000, time_interval=60, weight=1),

    # More restrictive limit for order operations
    RateLimit(limit_id=ORDERS_WEIGHT, limit=100, time_interval=10, weight=1),

    # Position-specific limits (conservative)
    RateLimit(limit_id=POSITIONS_WEIGHT, limit=50, time_interval=60, weight=1),

    # Funding-specific limits
    RateLimit(limit_id=FUNDING_WEIGHT, limit=100, time_interval=60, weight=1),

    # Raw request limit (fallback)
    RateLimit(limit_id=RAW_REQUESTS, limit=6000, time_interval=60, weight=1),

    # Public endpoints (higher limits)
    RateLimit(limit_id=PING_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=MARKETS_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=TICKER_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=TRADES_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=MARK_PRICES_PATH_URL, limit=1000, time_interval=60, weight=1),
    RateLimit(limit_id=FUNDING_RATES_PATH_URL, limit=1000, time_interval=60, weight=1),

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
    RateLimit(limit_id=HISTORY_PNL_PATH_URL, limit=100, time_interval=60, weight=1),
]

# Authentication Instruction Mapping (extend from spot)
INSTRUCTION_MAPPING = {
    # Balance and wallet
    (CAPITAL_PATH_URL, "GET"): "balanceQuery",
    (DEPOSITS_PATH_URL, "GET"): "depositQueryAll",
    (DEPOSIT_ADDRESS_PATH_URL, "GET"): "depositAddressQuery",
    (WITHDRAWALS_PATH_URL, "GET"): "withdrawalQueryAll",
    (WITHDRAWALS_PATH_URL, "POST"): "withdraw",

    # Orders (same as spot)
    (ORDER_PATH_URL, "GET"): "orderQuery",
    (ORDER_PATH_URL, "POST"): "orderExecute",
    (ORDER_PATH_URL, "DELETE"): "orderCancel",
    (ORDERS_PATH_URL, "GET"): "orderQueryAll",
    (ORDERS_PATH_URL, "POST"): "orderExecute",
    (ORDERS_PATH_URL, "DELETE"): "orderCancelAll",

    # History (extend from spot)
    (HISTORY_ORDERS_PATH_URL, "GET"): "orderHistoryQueryAll",
    (HISTORY_FILLS_PATH_URL, "GET"): "fillHistoryQueryAll",
    (HISTORY_FUNDING_PATH_URL, "GET"): "fundingHistoryQueryAll",
    (HISTORY_PNL_PATH_URL, "GET"): "pnlHistoryQueryAll",
    (HISTORY_DUST_PATH_URL, "GET"): "dustHistoryQueryAll",
    (HISTORY_BORROW_LEND_PATH_URL, "GET"): "borrowHistoryQueryAll",

    # Account (same as spot)
    (ACCOUNT_PATH_URL, "GET"): "accountQuery",
    (ACCOUNT_PATH_URL, "PATCH"): "accountUpdate",
    ("/account/convertDust", "POST"): "convertDust",
    (ACCOUNT_LIMITS_BORROW_PATH_URL, "GET"): "maxBorrowQuantity",
    (ACCOUNT_LIMITS_ORDER_PATH_URL, "GET"): "maxOrderQuantity",
    (ACCOUNT_LIMITS_WITHDRAWAL_PATH_URL, "GET"): "maxWithdrawalQuantity",

    # Positions (derivative-specific)
    (POSITION_PATH_URL, "GET"): "positionQuery",
    (BORROW_LEND_POSITIONS_PATH_URL, "GET"): "borrowLendPositionQuery",

    # Collateral (same as spot)
    (COLLATERAL_PATH_URL, "GET"): "collateralQuery",

    # Assets (same as spot)
    (ASSETS_PATH_URL, "GET"): "assetQuery",
    (WALLETS_PATH_URL, "GET"): "walletQuery",

    # Additional derivative instructions (if available)
    # Note: These may not exist in Backpack API - will verify during implementation
    # (LEVERAGE_PATH_URL, "POST"): "leverageUpdate",
    # (POSITION_MODE_PATH_URL, "POST"): "positionModeUpdate",
}

# Order Types Mapping (same as spot)
BACKPACK_ORDER_TYPE = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
    OrderType.LIMIT_MAKER: "Limit",  # Backpack uses Post-only for maker orders
}

HB_ORDER_TYPE = {
    "Limit": OrderType.LIMIT,
    "Market": OrderType.MARKET,
}

# Order Sides (same as spot)
BACKPACK_ORDER_SIDE = {
    "BUY": "Bid",
    "SELL": "Ask",
}

HB_ORDER_SIDE = {
    "Bid": "BUY",
    "Ask": "SELL",
}

# Position Sides (derivative-specific)
BACKPACK_POSITION_SIDE = {
    "LONG": "Long",
    "SHORT": "Short",
    "BOTH": "Both",  # For hedge mode
}

HB_POSITION_SIDE = {
    "Long": "LONG",
    "Short": "SHORT",
    "Both": "BOTH",
}

# Order Status Mapping (same as spot)
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
    "Cancelled": OrderState.CANCELED,
    "Expired": OrderState.FAILED,
    "Rejected": OrderState.FAILED,
}

# Time in Force (same as spot)
BACKPACK_TIME_IN_FORCE = {
    "GTC": "GTC",  # Good Till Canceled
    "IOC": "IOC",  # Immediate or Cancel
    "FOK": "FOK",  # Fill or Kill
}

# Market Types (extend from spot)
MARKET_TYPE_SPOT = "spot"
MARKET_TYPE_FUTURE = "future"
MARKET_TYPE_PERPETUAL = "perpetual"

# Client Order ID Configuration (same as spot)
MAX_ORDER_ID_LEN = 32
HBOT_ORDER_ID_PREFIX = "HBOT"
MAX_CLIENT_ID_BITS = 32

# Default Values (extend from spot)
DEFAULT_WINDOW = 5000  # 5 seconds
DEFAULT_FEES = {
    "maker": 0.0002,  # 0.02% - typical derivative fees (higher than spot)
    "taker": 0.0004,  # 0.04% - typical derivative fees (higher than spot)
}

# Error Codes (same as spot)
ORDER_NOT_FOUND_ERROR_CODE = "INVALID_CLIENT_REQUEST"
ORDER_NOT_FOUND_MESSAGE = "Order not found"

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

# WebSocket Configuration (same as spot)
WS_HEARTBEAT_TIME_INTERVAL = 30.0
WS_MESSAGE_TIMEOUT = 30.0
WS_CONNECTION_TIMEOUT = 10.0

# Supported Order Types (same as spot)
SUPPORTED_ORDER_TYPES = [OrderType.LIMIT, OrderType.MARKET]

# Connector Configuration
CONNECTOR_NAME = EXCHANGE_NAME
IS_CENTRALIZED = True
IS_DEX = False

# Derivative-specific configuration
IS_PERPETUAL = True
SUPPORTS_POSITION_MODES = True  # Will be updated based on API availability
SUPPORTS_LEVERAGE = True        # Will be updated based on API availability
SUPPORTS_FUNDING = True

# Collateral Token (will be updated from API - likely USDC for Backpack)
DEFAULT_COLLATERAL_TOKEN = "USDC"

# Broker/Client ID for orders
BROKER_ID = "x-HBOT-PERP"  # Derivative-specific broker ID
