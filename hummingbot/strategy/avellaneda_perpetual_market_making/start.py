# Avellaneda Perpetual Market Making Strategy Initialization
#
# This file is adapted from the spot Avellaneda start.py with changes for perpetual trading:
#
# KEY DIFFERENCES FROM SPOT VERSION:
# 1. Uses derivative connector instead of exchange
# 2. Imports perpetual-specific strategy class
# 3. Market initialization adapted for derivative trading
# 4. All other logic remains identical (same Avellaneda algorithm)

import os.path
from typing import List, Tuple

import pandas as pd

from hummingbot import data_path
from hummingbot.client.hummingbot_application import HummingbotApplication

# PERPETUAL DIFFERENCE: Import perpetual strategy class
from hummingbot.strategy.avellaneda_perpetual_market_making import AvellanedaPerpetualMarketMakingStrategy
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


def start(self):
    try:
        c_map = self.strategy_config_map.hb_config
        # PERPETUAL DIFFERENCE: Use derivative instead of exchange
        derivative = c_map.derivative
        raw_trading_pair = c_map.market

        trading_pair: str = raw_trading_pair
        base, quote = trading_pair.split("-")
        maker_assets: Tuple[str, str] = (base, quote)
        # PERPETUAL DIFFERENCE: Initialize with derivative connector
        market_names: List[Tuple[str, List[str]]] = [(derivative, [trading_pair])]
        self.initialize_markets(market_names)
        # PERPETUAL DIFFERENCE: Access markets using derivative name
        maker_data = [self.markets[derivative], trading_pair] + list(maker_assets)
        self.market_trading_pair_tuples = [MarketTradingPairTuple(*maker_data)]

        # Same logging options as spot version
        strategy_logging_options = AvellanedaPerpetualMarketMakingStrategy.OPTION_LOG_ALL

        # Same debug CSV path logic as spot version
        debug_csv_path = os.path.join(data_path(),
                                      HummingbotApplication.main_application().strategy_file_name.rsplit('.', 1)[0] +
                                      f"_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")

        # PERPETUAL DIFFERENCE: Use perpetual strategy class
        self.strategy = AvellanedaPerpetualMarketMakingStrategy()
        # Same initialization parameters as spot version (Avellaneda algorithm is identical)
        self.strategy.init_params(
            config_map=c_map,
            market_info=MarketTradingPairTuple(*maker_data),
            logging_options=strategy_logging_options,
            hb_app_notification=True,
            debug_csv_path=debug_csv_path,
            is_debug=False
        )
    except Exception as e:
        self.notify(str(e))
        self.logger().error("Unknown error during initialization.", exc_info=True)
