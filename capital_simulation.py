#!/usr/bin/env python3
# Grist Mill Capital -- Capital-Normalized Portfolio Simulation
# Full implementation staged Apr 5 2026. Run from trading_utilities/ directory.
# Runtime: ~30 mins. Parameters below are adjustable.
# Next session: implement full simulation and run $5K / 1-position scenario.

import os, sys

BASE     = os.path.expanduser('~/Desktop/Claude_Programs/Trading_Programs')
CAPITAL  = 50000  # change to 5000 for single-sleeve $5K sim
POS_SIZE = 5000   # per-trade position size
MAX_POS  = 10     # max simultaneous open positions

PERIODS = {
    'FULL_2025': ('2025-01-01', '2025-12-31'),
    '2025_PLUS': ('2025-01-01', '2026-03-30'),
    'YTD_2026':  ('2026-01-01', '2026-03-30'),
}

# Signal priority: 8K > PEAD > 13F > COT > SI > CEL
# Signals queue by entry date then priority; excess skipped when slots full.
# 8K uses pre-computed returns from backtest_results_v2.db (price-filtered).
# All other signals compute returns live via yfinance.

if __name__ == '__main__':
    print('Capital simulation stub -- full implementation next session.')
    print('CAPITAL=%s  POS_SIZE=%s  MAX_POS=%s' % (CAPITAL, POS_SIZE, MAX_POS))
    sys.exit(0)
