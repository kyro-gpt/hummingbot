"""
Lighter Perpetual Web Utilities

This module contains utility functions for building URLs, handling requests,
and managing web assistant factories for the Lighter Perpetual exchange connector.
"""
from typing import Any, Dict, Optional

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for public REST API endpoints
    
    :param path_url: The API endpoint path
    :param domain: The domain to use (mainnet or testnet)
    :return: Full URL string
    """
    base_url = CONSTANTS.REST_URL[domain]
    api_version = CONSTANTS.API_VERSION
    return f"{base_url}{api_version}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for private REST API endpoints
    
    :param path_url: The API endpoint path  
    :param domain: The domain to use (mainnet or testnet)
    :return: Full URL string
    """
    base_url = CONSTANTS.REST_URL[domain]
    api_version = CONSTANTS.API_VERSION
    return f"{base_url}{api_version}{path_url}"


def wss_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates WebSocket URL for the given domain
    
    :param domain: The domain to use (mainnet or testnet)
    :return: WebSocket URL string
    """
    return CONSTANTS.WS_URL[domain]


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """
    Builds the web assistants factory for Lighter API requests
    
    :param throttler: The throttler to use for rate limiting
    :param auth: The authentication handler
    :return: WebAssistantsFactory instance
    """
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[],
    )
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Creates the default throttler for Lighter API
    
    :return: AsyncThrottler instance with Lighter rate limits
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


class LighterPerpetualRESTPreProcessor(RESTPreProcessorBase):
    """
    Pre-processor for Lighter Perpetual REST requests to handle authentication headers
    """
    
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        """
        Pre-process REST requests to add necessary headers
        
        :param request: The REST request to process
        :return: The processed REST request
        """
        # Add common headers
        if request.headers is None:
            request.headers = {}
            
        request.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        
        return request


def format_trading_pair_to_market_id(trading_pair: str) -> int:
    """
    Convert Hummingbot trading pair format to Lighter market ID
    
    :param trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")
    :return: Market ID as integer
    """
    # This will need to be implemented based on actual market mapping
    # For now, return a placeholder
    # TODO: Implement actual market ID mapping from API response
    # Based on real API testing: ETH is market_id 0
    market_mapping = {
        "ETH-USDC": 0,  # Confirmed from mainnet API
        "BTC-USDC": 1,  # TODO: Verify actual BTC market ID
        "SOL-USDC": 2,  # TODO: Verify actual SOL market ID
        # Add more mappings as needed
    }
    return market_mapping.get(trading_pair, 0)


def format_market_id_to_trading_pair(market_id: int) -> str:
    """
    Convert Lighter market ID to Hummingbot trading pair format
    
    :param market_id: Market ID as integer
    :return: Trading pair in Hummingbot format (e.g., "BTC-USDC")
    """
    # This will need to be implemented based on actual market mapping
    # For now, return a placeholder
    # TODO: Implement actual trading pair mapping from API response
    id_mapping = {
        0: "BTC-USDC",
        1: "ETH-USDC", 
        2: "SOL-USDC",
        # Add more mappings as needed
    }
    return id_mapping.get(market_id, "UNKNOWN-USDC")


def convert_client_order_id_to_index(client_order_id: str) -> int:
    """
    Convert string client_order_id to integer client_order_index for Lighter API
    
    :param client_order_id: String client order ID from Hummingbot
    :return: Integer client order index for Lighter API
    """
    # Use CRC32 hash to convert string to consistent integer
    import zlib
    # Remove prefix if present and hash the remaining part
    clean_id = client_order_id.replace(CONSTANTS.CLIENT_ORDER_ID_PREFIX, "")
    # Use CRC32 to get a 32-bit integer
    return zlib.crc32(clean_id.encode()) & 0xffffffff


def convert_client_order_index_to_id(client_order_index: int, prefix: str = CONSTANTS.CLIENT_ORDER_ID_PREFIX) -> str:
    """
    Convert integer client_order_index back to string client_order_id
    
    :param client_order_index: Integer client order index from Lighter API
    :param prefix: Prefix to add to the client order ID
    :return: String client order ID for Hummingbot
    """
    # For reverse mapping, we'll need to maintain a mapping table
    # This is a simplified version - in practice, you'd maintain a bidirectional mapping
    return f"{prefix}{client_order_index}"


def get_rest_url_for_endpoint(endpoint: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Get the full REST URL for a given endpoint
    
    :param endpoint: The API endpoint path
    :param domain: The domain to use
    :return: Full URL string
    """
    # Determine if endpoint is public or private based on common patterns
    private_endpoints = [
        CONSTANTS.ACCOUNT_PATH_URL,
        CONSTANTS.ACCOUNT_ACTIVE_ORDERS_PATH_URL,
        CONSTANTS.SEND_TX_PATH_URL,
        CONSTANTS.NEXT_NONCE_PATH_URL,
    ]
    
    if endpoint in private_endpoints:
        return private_rest_url(endpoint, domain)
    else:
        return public_rest_url(endpoint, domain)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Validate exchange information response
    
    :param exchange_info: Exchange information dictionary
    :return: True if valid, False otherwise
    """
    required_fields = ["markets", "status"]  # Adjust based on actual API response
    return all(field in exchange_info for field in required_fields)
