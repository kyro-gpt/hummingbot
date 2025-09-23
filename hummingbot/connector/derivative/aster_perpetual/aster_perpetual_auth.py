import json
import math
import time
from typing import Any, Dict

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class AsterPerpetualAuth(AuthBase):
    """
    Auth class for Aster Perpetual API using Web3 ECDSA signatures

    Based on Aster's Web3 authentication requirements:
    - Uses user wallet, signer wallet, and private key
    - Generates signatures using eth_account and web3
    - ABI encodes parameters with Keccak hashing
    """

    def __init__(self, user_wallet: str, signer_wallet: str, private_key: str):
        """
        Initialize Aster Perpetual authentication

        :param user_wallet: Main account wallet address (user)
        :param signer_wallet: API wallet address (signer)
        :param private_key: Private key for signer wallet (without 0x prefix handling)
        """
        self._user_wallet: str = user_wallet
        self._signer_wallet: str = signer_wallet
        self._private_key: str = private_key if private_key.startswith('0x') else f'0x{private_key}'

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

    def add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add Aster Web3 authentication parameters to request

        :param params: Original request parameters
        :return: Parameters with authentication data added
        """
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
