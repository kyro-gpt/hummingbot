"""
Backpack Exchange Authentication Module

This module provides authentication functionality for the Backpack Exchange API
using Ed25519 signatures, ported from the Rust bpx-api-client implementation.
"""

import base64
import json
import time
from typing import Any, Dict, Optional

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BackpackAuth(AuthBase):
    """
    Authentication handler for Backpack Exchange API using Ed25519 signatures.

    This class handles the signing of requests according to Backpack's authentication
    requirements, which include Ed25519 signatures and specific instruction types.
    """

    # Default time window for request validity (5 seconds)
    DEFAULT_WINDOW = CONSTANTS.DEFAULT_WINDOW

    def __init__(self, api_key: str, secret_key: str, time_provider=None):
        """
        Initialize the BackpackAuth instance.

        Args:
            api_key: The base64-encoded public key (verifying key)
            secret_key: The base64-encoded private key (signing key)
            time_provider: Optional time provider for testing
        """
        self.api_key = api_key
        self.time_provider = time_provider

        try:
            # Decode the base64 secret key and create private key
            secret_bytes = base64.b64decode(secret_key)
            self.private_key = Ed25519PrivateKey.from_private_bytes(secret_bytes)

            # Derive public key and verify that the provided API key matches
            public_key_bytes = self.private_key.public_key().public_bytes_raw()
            expected_api_key = base64.b64encode(public_key_bytes).decode()
            if api_key != expected_api_key:
                raise ValueError("API key does not match the derived public key from secret key")

        except Exception as e:
            raise ValueError(f"Invalid secret key format: {e}")

    def _get_timestamp(self) -> int:
        """Get current timestamp in milliseconds."""
        if self.time_provider:
            return int(self.time_provider.time() * 1000)
        return int(time.time() * 1000)

    def _get_instruction_type(self, path: str, method: str) -> Optional[str]:
        """
        Get the instruction type for a given path and method.

        Args:
            path: The API endpoint path (with or without API version prefix)
            method: The HTTP method

        Returns:
            The instruction type string or None if not found
        """
        # Strip API version prefix to match our constants
        clean_path = path
        if path.startswith(CONSTANTS.API_VERSION):
            clean_path = path[len(CONSTANTS.API_VERSION):]
        elif path.startswith(CONSTANTS.WAPI_VERSION):
            clean_path = path[len(CONSTANTS.WAPI_VERSION):]

        return CONSTANTS.INSTRUCTION_MAPPING.get((clean_path, method.upper()))

    def _build_signing_string(self, instruction: str, query_params: Dict[str, Any],
                              body_params: Dict[str, Any], timestamp: int, window: int = None) -> str:
        """
        Build the string to be signed according to Backpack's specification.

        The format is: instruction={instruction}&{query_params}&{body_params}&timestamp={timestamp}&window={window}

        Args:
            instruction: The instruction type
            query_params: Query parameters as key-value pairs
            body_params: Body parameters as key-value pairs
            timestamp: Timestamp in milliseconds
            window: Time window in milliseconds (defaults to DEFAULT_WINDOW)

        Returns:
            The string to be signed
        """
        if window is None:
            window = self.DEFAULT_WINDOW

        # Start with instruction
        parts = [f"instruction={instruction}"]

        # Add query parameters (already sorted by URL encoding)
        for key, value in sorted(query_params.items()):
            parts.append(f"{key}={value}")

        # Add body parameters (sorted alphabetically)
        for key, value in sorted(body_params.items()):
            # Convert value to string and remove quotes from JSON string values
            # as per Rust implementation: v.trim_start_matches('"').trim_end_matches('"')
            if isinstance(value, bool):
                # Convert Python boolean to lowercase string for JSON compatibility
                value_str = str(value).lower()
            else:
                value_str = str(value)
                if value_str.startswith('"') and value_str.endswith('"'):
                    value_str = value_str[1:-1]  # Remove surrounding quotes
            parts.append(f"{key}={value_str}")

        # Add timestamp and window
        parts.append(f"timestamp={timestamp}")
        parts.append(f"window={window}")

        return "&".join(parts)

    def _generate_signature(self, message: str) -> str:
        """
        Generate Ed25519 signature for the given message.

        Args:
            message: The message to sign

        Returns:
            Base64-encoded signature
        """
        signature_bytes = self.private_key.sign(message.encode())
        return base64.b64encode(signature_bytes).decode()

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Add authentication headers to a REST request.

        Args:
            request: The request to authenticate

        Returns:
            The authenticated request with required headers
        """
        # Extract path from URL
        if hasattr(request.url, 'path'):
            path = request.url.path
        else:
            # Handle case where url is a string
            from urllib.parse import urlparse
            parsed_url = urlparse(str(request.url))
            path = parsed_url.path

        method = request.method.value

        # Get instruction type
        instruction = self._get_instruction_type(path, method)
        if not instruction:
            # For endpoints without specific instruction, don't sign
            return request

        # Parse query parameters
        query_params = {}
        if request.params:
            query_params = dict(request.params)

        # Parse body parameters
        body_params = {}
        if request.data:
            if isinstance(request.data, str):
                try:
                    data = json.loads(request.data)
                    if isinstance(data, dict):
                        body_params = data
                except json.JSONDecodeError:
                    pass
            elif isinstance(request.data, dict):
                body_params = request.data

        # Get timestamp and build signing string
        timestamp = self._get_timestamp()
        signing_string = self._build_signing_string(
            instruction, query_params, body_params, timestamp
        )

        # Generate signature
        signature = self._generate_signature(signing_string)

        # Add authentication headers
        headers = {}
        if request.headers:
            headers.update(request.headers)

        headers.update({
            "X-API-Key": self.api_key,
            "X-Timestamp": str(timestamp),
            "X-Window": str(self.DEFAULT_WINDOW),
            "X-Signature": signature,
            "User-Agent": "bpx-python-client"
        })

        # Don't set Content-Type here - let RESTAssistant handle it
        # This avoids signature mismatches when RESTAssistant overwrites our Content-Type

        request.headers = headers
        return request

    def websocket_login_parameters(self) -> Dict[str, Any]:
        """
        Generate WebSocket authentication parameters for Backpack.

        Returns:
            Dictionary containing authentication parameters for WebSocket login
        """
        timestamp = self._get_timestamp()

        # WebSocket authentication uses "subscribe" instruction format
        signing_string = f"instruction=subscribe&timestamp={timestamp}&window={self.DEFAULT_WINDOW}"
        signature = self._generate_signature(signing_string)

        # Get the public key (verifying key) in base64 format
        # Use the same API key that was validated during initialization
        verifying_key = self.api_key

        return {
            "method": "SUBSCRIBE",
            "params": [],  # Will be set by the user stream
            "signature": [verifying_key, signature, str(timestamp), str(self.DEFAULT_WINDOW)]
        }

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Add authentication to WebSocket requests.

        For Backpack WebSocket authentication, we need to send a subscribe message
        with the appropriate signature.

        Args:
            request: The WebSocket request

        Returns:
            The authenticated WebSocket request
        """
        # WebSocket authentication uses the "subscribe" instruction
        timestamp = self._get_timestamp()
        signing_string = self._build_signing_string(
            "subscribe", {}, {}, timestamp
        )
        signature = self._generate_signature(signing_string)

        # Add authentication payload to the request
        auth_payload = {
            "method": "SUBSCRIBE",
            "params": request.payload.get("params", []),
            "signature": {
                "apiKey": self.api_key,
                "timestamp": timestamp,
                "window": self.DEFAULT_WINDOW,
                "signature": signature
            }
        }

        request.payload = auth_payload
        return request
