# Basic test for Avellaneda Perpetual Market Making Configuration
# This test validates the Pydantic config map for derivative trading
#
# KEY DIFFERENCES FROM SPOT VERSION:
# 1. Tests derivative field instead of exchange field
# 2. Tests leverage field validation
# 3. All other Avellaneda parameters remain identical

import unittest
from decimal import Decimal

from hummingbot.strategy.avellaneda_perpetual_market_making.avellaneda_perpetual_market_making_config_map_pydantic import (
    AvellanedaPerpetualMarketMakingConfigMap,
)


class TestAvellanedaPerpetualMarketMakingConfigMap(unittest.TestCase):
    def test_derivative_field_validation(self):
        """Test that derivative field validates correctly"""
        config = {
            "derivative": "backpack_perpetual",
            "market": "SOL-USDC",
            "leverage": 5,
            "execution_timeframe_mode": "infinite",
            "order_amount": Decimal("0.01"),
            "risk_factor": Decimal("1.0"),
            "order_refresh_time": 5.0
        }
        # This should not raise an exception
        config_map = AvellanedaPerpetualMarketMakingConfigMap(**config)
        self.assertEqual(config_map.derivative, "backpack_perpetual")

    def test_leverage_field_validation(self):
        """Test that leverage field validates correctly"""
        config = {
            "derivative": "backpack_perpetual",
            "market": "SOL-USDC",
            "leverage": 10,
            "execution_timeframe_mode": "infinite",
            "order_amount": Decimal("0.01"),
            "risk_factor": Decimal("1.0"),
            "order_refresh_time": 5.0
        }
        config_map = AvellanedaPerpetualMarketMakingConfigMap(**config)
        self.assertEqual(config_map.leverage, 10)

    def test_leverage_field_bounds(self):
        """Test that leverage field enforces bounds"""
        config = {
            "derivative": "backpack_perpetual",
            "market": "SOL-USDC",
            "leverage": 0,  # Invalid: too low
            "execution_timeframe_mode": "infinite",
            "order_amount": Decimal("0.01"),
            "risk_factor": Decimal("1.0"),
            "order_refresh_time": 5.0
        }
        # This should raise a validation error
        with self.assertRaises(ValueError):
            AvellanedaPerpetualMarketMakingConfigMap(**config)

    def test_avellaneda_parameters_identical_to_spot(self):
        """Test that core Avellaneda parameters work identically to spot version"""
        config = {
            "derivative": "backpack_perpetual",
            "market": "SOL-USDC",
            "leverage": 5,
            "execution_timeframe_mode": "infinite",
            "order_amount": Decimal("0.01"),
            "risk_factor": Decimal("2.5"),  # γ parameter
            "order_amount_shape_factor": Decimal("0.3"),  # η parameter
            "min_spread": Decimal("0.1"),
            "order_refresh_time": 10.0,
            "inventory_target_base_pct": Decimal("40"),
            "volatility_buffer_size": 250,
            "trading_intensity_buffer_size": 250,
        }
        config_map = AvellanedaPerpetualMarketMakingConfigMap(**config)

        # Verify all Avellaneda-Stoikov parameters are preserved
        self.assertEqual(config_map.risk_factor, Decimal("2.5"))
        self.assertEqual(config_map.order_amount_shape_factor, Decimal("0.3"))
        self.assertEqual(config_map.inventory_target_base_pct, Decimal("40"))
        self.assertEqual(config_map.volatility_buffer_size, 250)


if __name__ == "__main__":
    unittest.main()
