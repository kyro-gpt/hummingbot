import hashlib
import hmac
import json
import math
import time
from typing import Any, Dict
from urllib.parse import urlencode

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants import (
    API_VERSION_V1,
    API_VERSION_V3,
    DEFAULT_API_VERSION,
)
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class AsterPerpetualAuth(AuthBase):
    """
    Auth class for Aster Perpetual API supporting both v1 HMAC and v3 Web3 authentication

    Automatically detects authentication method based on provided credentials:
    - v1: api_key + secret_key → HMAC SHA256 authentication
    - v3: user_wallet + private_key → Web3 ECDSA authentication
    """

    def __init__(self,
                 user_wallet: str,
                 api_key: str,
                 secret_key: str,
                 api_version: str = DEFAULT_API_VERSION):
        """
        Initialize Aster Perpetual authentication with unified credentials

        Args:
            user_wallet: User wallet address (used for v3, ignored for v1)
            api_key: API key (used for both v1 and v3 headers)
            secret_key: Secret key (used as HMAC secret for v1, as private key for v3)
            api_version: "v1" for HMAC SHA256, "v3" for Web3 ECDSA
        """
        # Store common credentials
        self.api_version = api_version
        self.api_key = api_key
        self.secret_key = secret_key
        self._user_wallet = user_wallet

        # Validate required credentials
        if not api_key or not secret_key:
            raise ValueError("api_key and secret_key are required")

        if api_version == API_VERSION_V1:
            # v1 HMAC authentication - only needs api_key + secret_key
            # user_wallet is ignored but stored for potential future use
            self._signer_wallet = None
            self._private_key = None

        elif api_version == API_VERSION_V3:
            # v3 Web3 authentication - secret_key is treated as private_key
            if not user_wallet:
                raise ValueError("v3 authentication requires user_wallet")

            # Validate user wallet address
            self._validate_address(user_wallet, "user_wallet")

            # Treat secret_key as private_key for v3
            private_key = secret_key

            # Ensure private key has 0x prefix
            self._private_key = private_key if private_key.startswith('0x') else f'0x{private_key}'

            # Derive signer address from private key
            try:
                account = Account.from_key(self._private_key)
                self._signer_wallet = account.address
            except Exception as e:
                raise ValueError(f"Invalid private key (secret_key): {e}")

            # Ensure user and signer are different (critical for Aster API)
            if self._user_wallet.lower() == self._signer_wallet.lower():
                raise ValueError(
                    f"User wallet and signer wallet cannot be the same address!\n"
                    f"  User:   {self._user_wallet}\n"
                    f"  Signer: {self._signer_wallet} (derived from secret_key)\n"
                    f"  You need to provide a different user wallet address than the one derived from your secret_key."
                )
        else:
            raise ValueError(f"Unsupported API version: {api_version}")

    def _validate_address(self, address: str, field_name: str):
        """
        Validate Ethereum address format

        :param address: The address to validate
        :param field_name: Name of the field for error messages
        :raises ValueError: If address format is invalid
        """
        if not isinstance(address, str):
            raise ValueError(f"{field_name} must be a string")

        if not address.startswith('0x'):
            raise ValueError(f"{field_name} must start with '0x'. Got: {address}")

        if len(address) != 42:
            raise ValueError(
                f"{field_name} must be exactly 42 characters (0x + 40 hex chars). "
                f"Got {len(address)} characters: {address}"
            )

        # Check if the address contains only valid hex characters
        try:
            int(address[2:], 16)
        except ValueError:
            raise ValueError(f"{field_name} contains invalid hex characters: {address}")

    def _generate_nonce(self) -> int:
        """Generate microsecond timestamp nonce"""
        return math.trunc(time.time() * 1000000)

    def _trim_dict(self, my_dict: Dict[str, Any]) -> Dict[str, str]:
        """
        Convert all dictionary values to strings, handling nested structures
        Based on Aster's _trim_dict function
        """
        result_dict = {}
        for key, value in my_dict.items():
            if isinstance(value, list):
                new_value = []
                for item in value:
                    if isinstance(item, dict):
                        new_value.append(json.dumps(self._trim_dict(item)))
                    else:
                        new_value.append(str(item))
                result_dict[key] = json.dumps(new_value)
            elif isinstance(value, dict):
                result_dict[key] = json.dumps(self._trim_dict(value))
            else:
                result_dict[key] = str(value)
        return result_dict

    def _create_signature_payload(self, params: Dict[str, Any], nonce: int) -> str:
        """
        Create the payload for signature generation

        Process:
        1. Trim all parameters to strings
        2. Create sorted JSON string
        3. ABI encode with user, signer, nonce
        4. Generate Keccak hash
        """
        # Trim parameters to strings
        trimmed_params = self._trim_dict(params)

        # Create sorted JSON string (no spaces, double quotes)
        json_str = json.dumps(trimmed_params, sort_keys=True).replace(' ', '').replace("'", '"')

        # ABI encode: [json_str, user, signer, nonce]
        encoded = encode(
            ['string', 'address', 'address', 'uint256'],
            [json_str, self._user_wallet, self._signer_wallet, nonce]
        )

        # Generate Keccak hash and ensure it's a hex string with 0x prefix
        keccak_hash = Web3.keccak(encoded)
        keccak_hex = '0x' + keccak_hash.hex()

        return keccak_hex

    def _generate_signature(self, payload_hash: str) -> str:
        """
        Generate ECDSA signature for the payload hash
        """
        # Create signable message
        signable_msg = encode_defunct(hexstr=payload_hash)

        # Sign with private key
        signed_message = Account.sign_message(
            signable_message=signable_msg,
            private_key=self._private_key
        )

        # Return signature with 0x prefix
        return '0x' + signed_message.signature.hex()

    def _add_v1_hmac_auth(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add v1 HMAC SHA256 authentication to parameters"""
        # Remove None values
        clean_params = {key: value for key, value in params.items() if value is not None}

        # Add timestamp and recvWindow if not present
        if 'timestamp' not in clean_params:
            clean_params['timestamp'] = int(time.time() * 1000)
        if 'recvWindow' not in clean_params:
            clean_params['recvWindow'] = 5000

        # Create query string for signing (don't sort - use original order)
        query_string = urlencode(clean_params)

        # Create HMAC SHA256 signature
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        # Add signature to parameters
        clean_params['signature'] = signature

        return clean_params

    def _add_v3_web3_auth(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add v3 Web3 ECDSA authentication to parameters"""
        # Remove None values (as done in Aster example)
        clean_params = {key: value for key, value in params.items() if value is not None}

        # Add required timing parameters
        clean_params['recvWindow'] = 50000  # 50 second window as in example
        clean_params['timestamp'] = int(round(time.time() * 1000))  # milliseconds

        # Generate nonce (microseconds)
        nonce = self._generate_nonce()

        # Create signature payload and generate signature
        payload_hash = self._create_signature_payload(clean_params, nonce)
        signature = self._generate_signature(payload_hash)

        # Add Web3 authentication parameters
        clean_params['nonce'] = nonce
        clean_params['user'] = self._user_wallet
        clean_params['signer'] = self._signer_wallet
        clean_params['signature'] = signature

        return clean_params

    def add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add authentication parameters based on configured version

        :param params: Original request parameters
        :return: Parameters with authentication data added
        """
        if self.api_version == API_VERSION_V1:
            return self._add_v1_hmac_auth(params)
        elif self.api_version == API_VERSION_V3:
            return self._add_v3_web3_auth(params)
        else:
            raise ValueError(f"Unsupported API version: {self.api_version}")

    def get_headers(self) -> Dict[str, str]:
        """Get headers required for authentication"""
        headers = {
            'User-Agent': 'HummingBot/1.0',
        }

        if self.api_key:
            headers['X-MBX-APIKEY'] = self.api_key

        return headers

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Authenticate REST request with Web3 signature
        """
        if request.method == RESTMethod.POST:
            # For POST requests, parse JSON data and authenticate
            if request.data:
                if isinstance(request.data, str):
                    params = json.loads(request.data)
                else:
                    params = request.data or {}
            else:
                params = {}

            authenticated_params = self.add_auth_to_params(params)
            request.data = authenticated_params

        else:
            # For GET/DELETE requests, authenticate URL parameters
            params = request.params or {}
            authenticated_params = self.add_auth_to_params(params)
            request.params = authenticated_params

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        WebSocket authentication (if needed by Aster)
        Note: This may need to be implemented based on Aster's WebSocket auth requirements
        """
        # For now, pass through - WebSocket auth may be different or not required
        return request
