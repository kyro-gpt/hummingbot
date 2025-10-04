"""
Lighter SDK Utilities

Simplified version of lighter SDK components without binary dependencies.
Contains essential transaction structures and utilities copied from lighter-python SDK.
"""
import json
import time
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class CreateOrder:
    """
    Create order transaction structure
    Based on lighter.transactions.CreateOrder
    """
    account_index: Optional[int] = None
    order_book_index: Optional[int] = None  # market_index
    base_amount: Optional[int] = None
    price: Optional[int] = None
    is_ask: Optional[int] = None
    order_type: Optional[int] = None
    expired_at: Optional[int] = None
    nonce: Optional[int] = None
    sig: Optional[str] = None
    
    @classmethod
    def from_json(cls, json_str: str) -> 'CreateOrder':
        """Create from JSON string"""
        params = json.loads(json_str)
        return cls(
            account_index=params.get('AccountIndex'),
            order_book_index=params.get('OrderBookIndex'),
            base_amount=params.get('BaseAmount'),
            price=params.get('Price'),
            is_ask=params.get('IsAsk'),
            order_type=params.get('OrderType'),
            expired_at=params.get('ExpiredAt'),
            nonce=params.get('Nonce'),
            sig=params.get('Sig')
        )
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        data = {
            'AccountIndex': self.account_index,
            'OrderBookIndex': self.order_book_index,
            'BaseAmount': self.base_amount,
            'Price': self.price,
            'IsAsk': self.is_ask,
            'OrderType': self.order_type,
            'ExpiredAt': self.expired_at,
            'Nonce': self.nonce,
            'Sig': self.sig,
        }
        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}
        return json.dumps(data)


@dataclass
class CancelOrder:
    """
    Cancel order transaction structure
    Based on lighter.transactions.CancelOrder
    """
    account_index: Optional[int] = None
    order_book_index: Optional[int] = None  # market_index
    order_index: Optional[int] = None
    nonce: Optional[int] = None
    sig: Optional[str] = None
    
    @classmethod
    def from_json(cls, json_str: str) -> 'CancelOrder':
        """Create from JSON string"""
        params = json.loads(json_str)
        return cls(
            account_index=params.get('AccountIndex'),
            order_book_index=params.get('OrderBookIndex'),
            order_index=params.get('OrderIndex'),
            nonce=params.get('Nonce'),
            sig=params.get('Sig')
        )
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        data = {
            'AccountIndex': self.account_index,
            'OrderBookIndex': self.order_book_index,
            'OrderIndex': self.order_index,
            'Nonce': self.nonce,
            'Sig': self.sig,
        }
        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}
        return json.dumps(data)


class NonceManager:
    """
    Simplified nonce manager for API key rotation
    Based on lighter.nonce_manager but without API client dependency
    """
    
    def __init__(self, account_index: int, start_api_key: int = 1, end_api_key: Optional[int] = None):
        """
        Initialize nonce manager
        
        :param account_index: Account index
        :param start_api_key: Starting API key index
        :param end_api_key: Ending API key index (defaults to start_api_key)
        """
        if end_api_key is None:
            end_api_key = start_api_key
            
        if start_api_key > end_api_key or start_api_key >= 255 or end_api_key >= 255:
            raise ValueError(f"Invalid range {start_api_key=} {end_api_key=}")
            
        self.start_api_key = start_api_key
        self.end_api_key = end_api_key
        self.current_api_key = end_api_key
        self.account_index = account_index
        
        # Initialize nonces (will be fetched from API when needed)
        self.nonce = {}
        for api_key_index in range(start_api_key, end_api_key + 1):
            self.nonce[api_key_index] = int(time.time() * 1000)  # Start with current timestamp
    
    def next_nonce(self) -> Tuple[int, int]:
        """
        Get next nonce and API key
        
        :return: Tuple of (api_key_index, nonce)
        """
        # Simple round-robin API key selection
        self.current_api_key += 1
        if self.current_api_key > self.end_api_key:
            self.current_api_key = self.start_api_key
        
        # Increment nonce for this API key
        self.nonce[self.current_api_key] += 1
        
        return self.current_api_key, self.nonce[self.current_api_key]
    
    def acknowledge_failure(self, api_key_index: int):
        """
        Acknowledge a failed transaction (rollback nonce)
        
        :param api_key_index: API key index that failed
        """
        if api_key_index in self.nonce and self.nonce[api_key_index] > 0:
            self.nonce[api_key_index] -= 1
    
    def hard_refresh_nonce(self, api_key_index: int, new_nonce: int):
        """
        Hard refresh nonce from API
        
        :param api_key_index: API key index
        :param new_nonce: New nonce value from API
        """
        self.nonce[api_key_index] = new_nonce


class LighterSDKHelper:
    """
    Helper class for Lighter SDK operations without binary dependencies
    """
    
    # Order types
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TYPE_STOP_LOSS = 2
    ORDER_TYPE_TAKE_PROFIT = 3
    
    # Time in force
    ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 0
    ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 1
    ORDER_TIME_IN_FORCE_FILL_OR_KILL = 2
    ORDER_TIME_IN_FORCE_GOOD_TILL_CANCELLED = 3
    
    # Transaction types (CORRECT VALUES FROM SDK)
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
    
    def __init__(self, account_index: int, api_key_index: int = 1):
        """
        Initialize SDK helper
        
        :param account_index: Account index
        :param api_key_index: API key index
        """
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.nonce_manager = NonceManager(account_index, api_key_index)
    
    def create_order_tx_info(
        self,
        market_index: int,
        client_order_index: int,
        base_amount: int,
        price: int,
        is_ask: bool,
        order_type: int = ORDER_TYPE_LIMIT,
        time_in_force: int = ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
        reduce_only: bool = False,
        trigger_price: int = 0,
    ) -> str:
        """
        Create order transaction info JSON
        
        :param market_index: Market index
        :param client_order_index: Client order index
        :param base_amount: Base amount in smallest units
        :param price: Price in smallest units
        :param is_ask: True for sell orders
        :param order_type: Order type
        :param time_in_force: Time in force
        :param reduce_only: Reduce only flag
        :param trigger_price: Trigger price for stop orders
        :return: JSON string for tx_info
        """
        api_key_index, nonce = self.nonce_manager.next_nonce()
        
        order = CreateOrder(
            account_index=self.account_index,
            order_book_index=market_index,
            base_amount=base_amount,
            price=price,
            is_ask=1 if is_ask else 0,
            order_type=order_type,
            expired_at=int(time.time() + 3600),  # 1 hour expiry
            nonce=nonce,
        )
        
        return order.to_json()
    
    def create_cancel_order_tx_info(
        self,
        market_index: int,
        order_index: int,
    ) -> str:
        """
        Create cancel order transaction info JSON
        
        :param market_index: Market index
        :param order_index: Order index to cancel
        :return: JSON string for tx_info
        """
        api_key_index, nonce = self.nonce_manager.next_nonce()
        
        cancel = CancelOrder(
            account_index=self.account_index,
            order_book_index=market_index,
            order_index=order_index,
            nonce=nonce,
        )
        
        return cancel.to_json()
    
    def format_send_tx_payload(
        self,
        tx_type: int,
        tx_info: str,
        price_protection: bool = True,
    ) -> Dict[str, Any]:
        """
        Format payload for sendTx API call
        
        :param tx_type: Transaction type
        :param tx_info: Transaction info JSON string
        :param price_protection: Price protection flag
        :return: Formatted payload dictionary
        """
        return {
            "tx_type": tx_type,
            "tx_info": tx_info,
            "price_protection": price_protection,
        }


def convert_amount_to_raw_units(amount: float, decimals: int = 6) -> int:
    """
    Convert decimal amount to raw units (smallest denomination)
    
    :param amount: Decimal amount
    :param decimals: Number of decimals (default: 6 for USDC)
    :return: Amount in raw units
    """
    return int(amount * (10 ** decimals))


def convert_raw_units_to_amount(raw_units: int, decimals: int = 6) -> float:
    """
    Convert raw units to decimal amount
    
    :param raw_units: Amount in raw units
    :param decimals: Number of decimals (default: 6 for USDC)
    :return: Decimal amount
    """
    return raw_units / (10 ** decimals)


def validate_private_key(private_key: str) -> bool:
    """
    Validate Ethereum private key format
    
    :param private_key: Private key string
    :return: True if valid format
    """
    try:
        # Remove 0x prefix if present
        key = private_key.replace('0x', '')
        
        # Check if it's 64 hex characters
        if len(key) != 64:
            return False
            
        # Check if it's valid hex
        int(key, 16)
        return True
        
    except (ValueError, TypeError):
        return False
