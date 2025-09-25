from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "aster_perpetual"
BROKER_ID = "x-aster-hb"  # To be assigned by Aster team
MAX_ORDER_ID_LEN = 32

# API Version Control
API_VERSION_V1 = "v1"
API_VERSION_V3 = "v3"
DEFAULT_API_VERSION = API_VERSION_V1  # Default to v1 (working HMAC auth)

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "aster_perpetual_testnet"

# Aster API URLs - based on Aster documentation
PERPETUAL_BASE_URL = "https://fapi.asterdex.com/fapi/"
TESTNET_BASE_URL = "https://testnet.asterdex.com/fapi/"  # To be confirmed

# Aster WebSocket URLs
PERPETUAL_WS_URL = "wss://fstream.asterdex.com/"
TESTNET_WS_URL = "wss://testnet-stream.asterdex.com/"  # To be confirmed

PUBLIC_WS_ENDPOINT = "stream"
PRIVATE_WS_ENDPOINT = "ws"

# Time in Force - Same as Binance
TIME_IN_FORCE_GTC = "GTC"  # Good till cancelled
TIME_IN_FORCE_GTX = "GTX"  # Good Till Crossing
TIME_IN_FORCE_IOC = "IOC"  # Immediate or cancel
TIME_IN_FORCE_FOK = "FOK"  # Fill or kill

# Version-specific endpoint mappings
ENDPOINTS = {
    API_VERSION_V1: {
        # Public endpoints
        "SNAPSHOT_REST_URL": "v1/depth",
        "TICKER_PRICE_URL": "v1/ticker/bookTicker",
        "TICKER_PRICE_CHANGE_URL": "v1/ticker/24hr",
        "EXCHANGE_INFO_URL": "v1/exchangeInfo",
        "RECENT_TRADES_URL": "v1/trades",
        "PING_URL": "v1/ping",
        "MARK_PRICE_URL": "v1/premiumIndex",
        "SERVER_TIME_PATH_URL": "v1/time",

        # Private endpoints (HMAC SHA256 auth)
        "ORDER_URL": "v1/order",
        "CANCEL_ALL_OPEN_ORDERS_URL": "v1/allOpenOrders",
        "ACCOUNT_TRADE_LIST_URL": "v1/userTrades",
        "SET_LEVERAGE_URL": "v1/leverage",
        "GET_INCOME_HISTORY_URL": "v1/income",
        "CHANGE_POSITION_MODE_URL": "v1/positionSide/dual",
        "ACCOUNT_INFO_URL": "v2/account",  # v1 uses v2 for account info
        "POSITION_INFORMATION_URL": "v2/positionRisk",
    },
    API_VERSION_V3: {
        # Public endpoints
        "SNAPSHOT_REST_URL": "v3/depth",
        "TICKER_PRICE_URL": "v3/ticker/bookTicker",
        "TICKER_PRICE_CHANGE_URL": "v3/ticker/24hr",
        "EXCHANGE_INFO_URL": "v3/exchangeInfo",
        "RECENT_TRADES_URL": "v3/trades",
        "PING_URL": "v3/ping",
        "MARK_PRICE_URL": "v3/premiumIndex",
        "SERVER_TIME_PATH_URL": "v3/time",

        # Private endpoints (Web3 ECDSA auth)
        "ORDER_URL": "v3/order",
        "CANCEL_ALL_OPEN_ORDERS_URL": "v3/allOpenOrders",
        "ACCOUNT_TRADE_LIST_URL": "v3/userTrades",
        "SET_LEVERAGE_URL": "v3/leverage",
        "GET_INCOME_HISTORY_URL": "v3/income",
        "CHANGE_POSITION_MODE_URL": "v3/positionSide/dual",
        "ACCOUNT_INFO_URL": "v3/account",
        "POSITION_INFORMATION_URL": "v3/positionRisk",
    }
}

# Helper function to get endpoint for current version


def get_endpoint(endpoint_name: str, api_version: str = DEFAULT_API_VERSION) -> str:
    return ENDPOINTS[api_version][endpoint_name]


# Backward compatibility - use default version
SNAPSHOT_REST_URL = get_endpoint("SNAPSHOT_REST_URL")
TICKER_PRICE_URL = get_endpoint("TICKER_PRICE_URL")
TICKER_PRICE_CHANGE_URL = get_endpoint("TICKER_PRICE_CHANGE_URL")
EXCHANGE_INFO_URL = get_endpoint("EXCHANGE_INFO_URL")
RECENT_TRADES_URL = get_endpoint("RECENT_TRADES_URL")
PING_URL = get_endpoint("PING_URL")
MARK_PRICE_URL = get_endpoint("MARK_PRICE_URL")
SERVER_TIME_PATH_URL = get_endpoint("SERVER_TIME_PATH_URL")
ORDER_URL = get_endpoint("ORDER_URL")
CANCEL_ALL_OPEN_ORDERS_URL = get_endpoint("CANCEL_ALL_OPEN_ORDERS_URL")
ACCOUNT_TRADE_LIST_URL = get_endpoint("ACCOUNT_TRADE_LIST_URL")
SET_LEVERAGE_URL = get_endpoint("SET_LEVERAGE_URL")
GET_INCOME_HISTORY_URL = get_endpoint("GET_INCOME_HISTORY_URL")
CHANGE_POSITION_MODE_URL = get_endpoint("CHANGE_POSITION_MODE_URL")

POST_POSITION_MODE_LIMIT_ID = f"POST{CHANGE_POSITION_MODE_URL}"
GET_POSITION_MODE_LIMIT_ID = f"GET{CHANGE_POSITION_MODE_URL}"

# Account and Position endpoints - use default version
ACCOUNT_INFO_URL = get_endpoint("ACCOUNT_INFO_URL")
POSITION_INFORMATION_URL = get_endpoint("POSITION_INFORMATION_URL")

# User Stream Endpoint - Same pattern as Binance
ASTER_PERPETUAL_USER_STREAM_ENDPOINT = get_endpoint("ORDER_URL").replace("/order", "/listenKey")

# Funding Settlement Time Span
FUNDING_SETTLEMENT_DURATION = (0, 30)  # seconds before snapshot, seconds after snapshot

# Order Statuses - Same as Binance
ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "CANCELED": OrderState.CANCELED,
    "EXPIRED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
}

# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS_1MIN = "ORDERS_1MIN"
ORDERS_1SEC = "ORDERS_1SEC"

# WebSocket Stream IDs and Configuration
DIFF_STREAM_ID = 1
TRADE_STREAM_ID = 2
FUNDING_INFO_STREAM_ID = 3
HEARTBEAT_TIME_INTERVAL = 30.0

# WebSocket Channels - Optimized for Aster
# NOTE: Using @depth@100ms for 2.5x faster updates than Binance's default @depth (250ms)
# ASTER OPTIONS: "@depth" (250ms), "@depth@500ms" (500ms), "@depth@100ms" (100ms)
# BINANCE USES: "@depth" (250ms default)
#
# FALLBACK INSTRUCTIONS: If rate limiting issues occur:
# 1. Change line below: WS_DEPTH_CHANNEL = "@depth"  # Revert to Binance default
# 2. Update subscription logs in order book data source
WS_DEPTH_CHANNEL = "@depth@100ms"  # Current: 100ms (2.5x faster than Binance)
WS_TRADE_CHANNEL = "@aggTrade"     # Same as Binance
WS_FUNDING_CHANNEL = "@markPrice"  # Same as Binance

# Rate Limit time intervals
ONE_HOUR = 3600
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

# TODO: Verify these limits with Aster documentation
MAX_REQUEST = 2400

# Rate Limits - Starting with Binance limits, to be adjusted based on Aster specs
RATE_LIMITS = [
    # Pool Limits
    RateLimit(limit_id=REQUEST_WEIGHT, limit=2400, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS_1MIN, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS_1SEC, limit=300, time_interval=10),
    # Weight Limits for individual endpoints
    RateLimit(limit_id=SNAPSHOT_REST_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=20)]),
    RateLimit(limit_id=TICKER_PRICE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=2)]),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=EXCHANGE_INFO_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=40)]),
    RateLimit(limit_id=RECENT_TRADES_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=ASTER_PERPETUAL_USER_STREAM_ENDPOINT, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=PING_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=ORDER_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1),
                             LinkedLimitWeightPair(ORDERS_1MIN, weight=1),
                             LinkedLimitWeightPair(ORDERS_1SEC, weight=1)]),
    RateLimit(limit_id=CANCEL_ALL_OPEN_ORDERS_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=ACCOUNT_TRADE_LIST_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=5)]),
    RateLimit(limit_id=SET_LEVERAGE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=GET_INCOME_HISTORY_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=30)]),
    RateLimit(limit_id=POST_POSITION_MODE_LIMIT_ID, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=GET_POSITION_MODE_LIMIT_ID, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=30)]),
    RateLimit(limit_id=ACCOUNT_INFO_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=5)]),
    RateLimit(limit_id=POSITION_INFORMATION_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE, weight=5,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=5)]),
    RateLimit(limit_id=MARK_PRICE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE, weight=1,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
]

# Error codes - Starting with Binance codes, to be adjusted
ORDER_NOT_EXIST_ERROR_CODE = -2013
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = -2011
UNKNOWN_ORDER_MESSAGE = "Unknown order sent"
