"""
Lighter Authentication

This module handles authentication for the Lighter exchange API using private key signing.
Based on the lighter-python SDK but simplified to avoid binary dependencies.
"""
import json
import time
from typing import Dict, Optional, Tuple, Any
import logging

from eth_account import Account
from eth_account.messages import encode_defunct

from hummingbot.connector.derivative.lighter import lighter_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest

logger = logging.getLogger(__name__)


class LighterAuth(AuthBase):
    """
    Authentication class for Lighter API using private key signing
    """
    
    def __init__(self, private_key: str, account_index: int, api_key_index: int = 1):
        """
        Initialize Lighter authentication
        
        :param private_key: Ethereum private key in hex format (with or without 0x prefix)
        :param account_index: Account index on Lighter
        :param api_key_index: API key index (default: 1)
        """
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index
        
        # Ensure private key has 0x prefix
        if not self.private_key.startswith('0x'):
            self.private_key = '0x' + self.private_key
            
        # Create eth_account instance for signing
        try:
            self.account = Account.from_key(self.private_key)
            self.address = self.account.address
        except Exception as e:
            raise ValueError(f"Invalid private key: {e}")
            
        logger.info(f"Initialized Lighter auth for account {self.account_index}, address {self.address}")
    
    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Authenticate REST requests
        
        :param request: The REST request to authenticate
        :return: The authenticated REST request
        """
        if request.method == RESTMethod.POST and request.url.endswith(CONSTANTS.SEND_TX_PATH_URL):
            # For sendTx requests, we need to sign the transaction
            if request.data:
                request.data = await self._sign_send_tx_request(request.data)
        
        # Add authentication headers if needed
        if request.headers is None:
            request.headers = {}
            
        # Add common headers
        request.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        
        return request
    
    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Authenticate WebSocket requests
        
        :param request: The WebSocket request to authenticate
        :return: The authenticated WebSocket request
        """
        # WebSocket authentication may require auth token
        # For now, pass through - implement if needed
        return request
    
    async def _sign_send_tx_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sign a sendTx request payload
        
        :param request_data: The request data to sign
        :return: The signed request data
        """
        try:
            # Extract transaction info from request
            tx_type = request_data.get("tx_type")
            tx_info = request_data.get("tx_info")
            
            if tx_type is None or tx_info is None:
                logger.error("Missing tx_type or tx_info in request data")
                return request_data
            
            # Parse tx_info if it's a string
            if isinstance(tx_info, str):
                tx_info_dict = json.loads(tx_info)
            else:
                tx_info_dict = tx_info
            
            # Add account and API key info
            tx_info_dict["account_index"] = self.account_index
            tx_info_dict["api_key_index"] = self.api_key_index
            
            # Generate nonce if not present
            if "nonce" not in tx_info_dict:
                tx_info_dict["nonce"] = int(time.time() * 1000)  # milliseconds
            
            # Sign the transaction
            signature = await self._sign_transaction(tx_type, tx_info_dict)
            
            # Update request data with signature
            request_data.update({
                "tx_type": tx_type,
                "tx_info": json.dumps(tx_info_dict),
                "signature": signature,
                "account_index": self.account_index,
                "api_key_index": self.api_key_index,
            })
            
            return request_data
            
        except Exception as e:
            logger.error(f"Error signing sendTx request: {e}")
            return request_data
    
    async def _sign_transaction(self, tx_type: int, tx_info: Dict[str, Any]) -> str:
        """
        Sign a transaction using Ethereum private key
        
        :param tx_type: Transaction type
        :param tx_info: Transaction information
        :return: Signature string
        """
        try:
            # Create message to sign based on transaction type and info
            message_parts = [
                str(tx_type),
                str(self.account_index),
                str(self.api_key_index),
                str(tx_info.get("nonce", int(time.time() * 1000)))
            ]
            
            # Add transaction-specific fields based on type
            if tx_type == CONSTANTS.TX_TYPE_CREATE_ORDER:
                message_parts.extend([
                    str(tx_info.get("market_index", 0)),
                    str(tx_info.get("client_order_index", 0)),
                    str(tx_info.get("base_amount", 0)),
                    str(tx_info.get("price", 0)),
                    str(tx_info.get("is_ask", 0)),
                    str(tx_info.get("order_type", 0)),
                ])
            elif tx_type == CONSTANTS.TX_TYPE_CANCEL_ORDER:
                message_parts.extend([
                    str(tx_info.get("market_index", 0)),
                    str(tx_info.get("order_index", 0)),
                ])
            
            # Create message string
            message_to_sign = "|".join(message_parts)
            
            # Sign the message
            message = encode_defunct(text=message_to_sign)
            signed_message = self.account.sign_message(message)
            
            return signed_message.signature.hex()
            
        except Exception as e:
            logger.error(f"Error signing transaction: {e}")
            raise
    
    def create_order_tx_info(
        self,
        market_index: int,
        client_order_index: int,
        base_amount: int,
        price: int,
        is_ask: bool,
        order_type: int = CONSTANTS.ORDER_TYPE_LIMIT,
        time_in_force: int = CONSTANTS.TIME_IN_FORCE_GOOD_TILL_TIME,
        reduce_only: bool = False,
        trigger_price: int = 0,
    ) -> Dict[str, Any]:
        """
        Create transaction info for order creation
        
        :param market_index: Market index
        :param client_order_index: Client order index
        :param base_amount: Base amount in smallest units
        :param price: Price in smallest units
        :param is_ask: True for sell orders, False for buy orders
        :param order_type: Order type (default: limit)
        :param time_in_force: Time in force (default: GTT)
        :param reduce_only: Whether this is a reduce-only order
        :param trigger_price: Trigger price for stop orders
        :return: Transaction info dictionary
        """
        return {
            "market_index": market_index,
            "client_order_index": client_order_index,
            "base_amount": base_amount,
            "price": price,
            "is_ask": 1 if is_ask else 0,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "reduce_only": 1 if reduce_only else 0,
            "trigger_price": trigger_price,
            "nonce": int(time.time() * 1000),
        }
    
    def create_cancel_order_tx_info(
        self,
        market_index: int,
        order_index: int,
    ) -> Dict[str, Any]:
        """
        Create transaction info for order cancellation
        
        :param market_index: Market index
        :param order_index: Order index to cancel
        :return: Transaction info dictionary
        """
        return {
            "market_index": market_index,
            "order_index": order_index,
            "nonce": int(time.time() * 1000),
        }
    
    def create_auth_token(self, expiry_minutes: int = 10) -> Tuple[str, Optional[str]]:
        """
        Create authentication token for API access
        
        :param expiry_minutes: Token expiry in minutes
        :return: Tuple of (auth_token, error)
        """
        try:
            deadline = int(time.time() + expiry_minutes * 60)
            
            # Create message to sign for auth token
            message_to_sign = f"auth_token:{self.account_index}:{self.api_key_index}:{deadline}"
            
            # Sign the message
            message = encode_defunct(text=message_to_sign)
            signed_message = self.account.sign_message(message)
            
            auth_token = {
                "account_index": self.account_index,
                "api_key_index": self.api_key_index,
                "deadline": deadline,
                "signature": signed_message.signature.hex(),
            }
            
            return json.dumps(auth_token), None
            
        except Exception as e:
            logger.error(f"Error creating auth token: {e}")
            return "", str(e)
