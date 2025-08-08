"""
Backpack Perpetual Exchange Web Utilities

This module provides utilities for building URLs and creating web assistants
for the Backpack perpetual/derivative exchange connector.

95% reused from spot trading with minor additions for derivative endpoints.
"""

import time
from typing import Optional

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Create a full REST API URL for public endpoints.

    Args:
        path_url: The endpoint path
        domain: The domain (unused for Backpack, kept for consistency)

    Returns:
        Complete URL for the public endpoint
    """
    base_url = CONSTANTS.REST_URL

    # Add API version prefix if not already included
    if not path_url.startswith(CONSTANTS.API_VERSION) and not path_url.startswith(CONSTANTS.WAPI_VERSION):
        if path_url.startswith("/"):
            path_url = CONSTANTS.API_VERSION + path_url
        else:
            path_url = CONSTANTS.API_VERSION + "/" + path_url

    return base_url + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Create a full REST API URL for private endpoints.

    Args:
        path_url: The endpoint path
        domain: The domain (unused for Backpack, kept for consistency)

    Returns:
        Complete URL for the private endpoint
    """
    base_url = CONSTANTS.REST_URL

    # Add appropriate API version prefix
    if not path_url.startswith(CONSTANTS.API_VERSION) and not path_url.startswith(CONSTANTS.WAPI_VERSION):
        # Determine if this should use WAPI based on the path
        wapi_paths = [
            CONSTANTS.DEPOSITS_PATH_URL,
            CONSTANTS.WITHDRAWALS_PATH_URL,
            CONSTANTS.DEPOSIT_ADDRESS_PATH_URL,
            "/history/"  # All history endpoints use WAPI
        ]
        if any(wapi_path in path_url for wapi_path in wapi_paths):
            if path_url.startswith("/"):
                path_url = CONSTANTS.WAPI_VERSION + path_url
            else:
                path_url = CONSTANTS.WAPI_VERSION + "/" + path_url
        else:
            if path_url.startswith("/"):
                path_url = CONSTANTS.API_VERSION + path_url
            else:
                path_url = CONSTANTS.API_VERSION + "/" + path_url

    return base_url + path_url


def ws_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Create WebSocket URL for real-time data.

    Args:
        domain: The domain (unused for Backpack, kept for consistency)

    Returns:
        WebSocket URL
    """
    return CONSTANTS.WS_URL


def create_throttler() -> AsyncThrottler:
    """Create a throttler for Backpack perpetual exchange."""
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def build_api_factory(throttler=None, time_synchronizer=None, domain: str = CONSTANTS.DEFAULT_DOMAIN,
                      time_provider=None, auth=None) -> WebAssistantsFactory:
    """
    Create a WebAssistantsFactory configured for Backpack perpetual exchange.

    Args:
        throttler: Rate limiter instance
        time_synchronizer: Time synchronizer instance (not used for Backpack)
        domain: The domain (unused for Backpack, kept for consistency)
        time_provider: Time provider instance
        auth: Authentication instance

    Returns:
        Configured WebAssistantsFactory
    """
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[],
        ws_pre_processors=[],
    )
    return api_factory


def build_rate_limits_by_tier() -> list:
    """
    Build rate limits based on account tier.

    For Backpack Perpetual, we start with conservative limits and adjust based on
    actual rate limit responses from the API.

    Returns:
        List of rate limits
    """
    return CONSTANTS.RATE_LIMITS.copy()


def is_private_endpoint(path_url: str) -> bool:
    """
    Determine if an endpoint requires authentication.

    Args:
        path_url: The endpoint path

    Returns:
        True if the endpoint requires authentication
    """
    private_paths = [
        CONSTANTS.ACCOUNT_PATH_URL,
        CONSTANTS.CAPITAL_PATH_URL,
        CONSTANTS.WALLETS_PATH_URL,
        CONSTANTS.ASSETS_PATH_URL,
        CONSTANTS.COLLATERAL_PATH_URL,
        CONSTANTS.POSITION_PATH_URL,  # Derivative-specific
        CONSTANTS.ORDER_PATH_URL,
        CONSTANTS.ORDERS_PATH_URL,
        CONSTANTS.BORROW_LEND_POSITIONS_PATH_URL,
        "/history/",  # All history endpoints
        "/limits/",   # All limit endpoints
        "/convertDust",
        # Derivative-specific paths
        "/leverage",  # If available
        "/positionMode",  # If available
    ]

    return any(private_path in path_url for private_path in private_paths)


def is_public_endpoint(path_url: str) -> bool:
    """
    Determine if an endpoint is public (no authentication required).

    Args:
        path_url: The endpoint path

    Returns:
        True if the endpoint is public
    """
    public_paths = [
        CONSTANTS.PING_PATH_URL,
        CONSTANTS.STATUS_PATH_URL,
        CONSTANTS.TIME_PATH_URL,
        CONSTANTS.MARKETS_PATH_URL,
        CONSTANTS.MARKET_PATH_URL,
        CONSTANTS.TICKER_PATH_URL,
        CONSTANTS.TICKERS_PATH_URL,
        CONSTANTS.DEPTH_PATH_URL,
        CONSTANTS.KLINES_PATH_URL,
        CONSTANTS.TRADES_PATH_URL,
        # Derivative-specific public endpoints
        CONSTANTS.MARK_PRICES_PATH_URL,
        CONSTANTS.FUNDING_RATES_PATH_URL,
        CONSTANTS.OPEN_INTEREST_PATH_URL,
    ]

    return any(public_path in path_url for public_path in public_paths)


def get_rest_url_for_endpoint(endpoint: str, trading_pair: str = None, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Get the complete REST URL for a specific endpoint.

    Args:
        endpoint: The endpoint identifier from constants
        trading_pair: Trading pair if required by endpoint
        domain: The domain (unused for Backpack, kept for consistency)

    Returns:
        Complete URL for the endpoint
    """
    # Map endpoint constants to actual paths (extend from spot)
    endpoint_mapping = {
        # Public endpoints
        "PING": CONSTANTS.PING_PATH_URL,
        "STATUS": CONSTANTS.STATUS_PATH_URL,
        "TIME": CONSTANTS.TIME_PATH_URL,
        "MARKETS": CONSTANTS.MARKETS_PATH_URL,
        "MARKET": CONSTANTS.MARKET_PATH_URL,
        "TICKER": CONSTANTS.TICKER_PATH_URL,
        "TICKERS": CONSTANTS.TICKERS_PATH_URL,
        "DEPTH": CONSTANTS.DEPTH_PATH_URL,
        "KLINES": CONSTANTS.KLINES_PATH_URL,
        "TRADES": CONSTANTS.TRADES_PATH_URL,

        # Derivative-specific public endpoints
        "MARK_PRICES": CONSTANTS.MARK_PRICES_PATH_URL,
        "FUNDING_RATES": CONSTANTS.FUNDING_RATES_PATH_URL,
        "OPEN_INTEREST": CONSTANTS.OPEN_INTEREST_PATH_URL,

        # Private endpoints
        "ACCOUNT": CONSTANTS.ACCOUNT_PATH_URL,
        "CAPITAL": CONSTANTS.CAPITAL_PATH_URL,
        "WALLETS": CONSTANTS.WALLETS_PATH_URL,
        "POSITION": CONSTANTS.POSITION_PATH_URL,  # Derivative-specific
        "ORDER": CONSTANTS.ORDER_PATH_URL,
        "ORDERS": CONSTANTS.ORDERS_PATH_URL,

        # History endpoints
        "HISTORY_FILLS": CONSTANTS.HISTORY_FILLS_PATH_URL,
        "HISTORY_ORDERS": CONSTANTS.HISTORY_ORDERS_PATH_URL,
        "HISTORY_FUNDING": CONSTANTS.HISTORY_FUNDING_PATH_URL,  # Derivative-specific
        "HISTORY_PNL": CONSTANTS.HISTORY_PNL_PATH_URL,          # Derivative-specific
    }

    path = endpoint_mapping.get(endpoint, endpoint)

    # Handle trading pair substitution if needed
    if trading_pair and "{symbol}" in path:
        path = path.replace("{symbol}", trading_pair)

    # Determine if private or public
    if is_private_endpoint(path):
        return private_rest_url(path, domain)
    else:
        return public_rest_url(path, domain)


def get_ws_message_frame() -> dict:
    """
    Get the basic WebSocket message frame for Backpack Perpetual.

    Returns:
        Basic WebSocket message structure
    """
    return {
        "method": "SUBSCRIBE",
        "params": [],
        "signature": None  # Will be added by auth if needed
    }


def format_trading_pair(trading_pair: str) -> str:
    """
    Convert Hummingbot trading pair format to Backpack perpetual format.

    Args:
        trading_pair: Trading pair in Hummingbot format (e.g., "SOL-USDC")

    Returns:
        Trading pair in Backpack perpetual format (e.g., "SOL_USDC_PERP")
    """
    return trading_pair.replace("-", "_") + "_PERP"


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    """
    Convert Backpack perpetual trading pair format to Hummingbot format.

    Args:
        exchange_trading_pair: Trading pair in Backpack perpetual format (e.g., "SOL_USDC_PERP")

    Returns:
        Trading pair in Hummingbot format (e.g., "SOL-USDC")
    """
    # Remove _PERP suffix and replace underscores with hyphens
    if exchange_trading_pair.endswith("_PERP"):
        base_pair = exchange_trading_pair[:-5]  # Remove "_PERP"
        return base_pair.replace("_", "-")
    else:
        # Fallback for symbols without _PERP suffix
        return exchange_trading_pair.replace("_", "-")


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    """
    Convert Hummingbot trading pair format to Backpack perpetual format.

    Args:
        hb_trading_pair: Trading pair in Hummingbot format (e.g., "SOL-USDC")

    Returns:
        Trading pair in Backpack perpetual format (e.g., "SOL_USDC_PERP")
    """
    return hb_trading_pair.replace("-", "_") + "_PERP"


def wss_url(base_ws_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Create a WebSocket URL for the given base URL and domain.

    Args:
        base_ws_url: The base WebSocket URL
        domain: The domain to use

    Returns:
        Complete WebSocket URL
    """
    return base_ws_url


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Get the current server time from Backpack's /time endpoint.

    Args:
        throttler: Optional throttler for rate limiting
        domain: Optional domain (unused but kept for interface compatibility)

    Returns:
        Current server time in seconds as a float
    """
    from hummingbot.core.web_assistant.connections.data_types import RESTMethod
    from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()

    try:
        response = await rest_assistant.execute_request(
            url=public_rest_url(path_url=CONSTANTS.TIME_PATH_URL, domain=domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.TIME_PATH_URL,
        )
        # Backpack returns time in milliseconds, convert to seconds
        server_time_ms = int(response)
        return float(server_time_ms / 1000.0)
    except Exception:
        # Fallback to local time if server time fetch fails
        return time.time()


# Derivative-specific utility functions

def get_funding_info_url(trading_pair: str = None, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Get the URL for funding rate information.

    Args:
        trading_pair: Optional trading pair for specific funding info
        domain: Domain (unused but kept for consistency)

    Returns:
        URL for funding rate endpoint
    """
    path = CONSTANTS.FUNDING_RATES_PATH_URL
    if trading_pair:
        # Add trading pair as query parameter if needed
        path = f"{path}?symbol={format_trading_pair(trading_pair)}"
    return public_rest_url(path, domain)


def get_position_url(trading_pair: str = None, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Get the URL for position information.

    Args:
        trading_pair: Optional trading pair for specific position
        domain: Domain (unused but kept for consistency)

    Returns:
        URL for position endpoint
    """
    path = CONSTANTS.POSITION_PATH_URL
    if trading_pair:
        # Add trading pair as query parameter if needed
        path = f"{path}?symbol={format_trading_pair(trading_pair)}"
    return private_rest_url(path, domain)


def get_mark_price_url(trading_pair: str = None, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Get the URL for mark price information.

    Args:
        trading_pair: Optional trading pair for specific mark price
        domain: Domain (unused but kept for consistency)

    Returns:
        URL for mark price endpoint
    """
    path = CONSTANTS.MARK_PRICES_PATH_URL
    if trading_pair:
        # Add trading pair as query parameter if needed
        path = f"{path}?symbol={format_trading_pair(trading_pair)}"
    return public_rest_url(path, domain)
