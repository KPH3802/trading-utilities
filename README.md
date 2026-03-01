# Trading Utilities

**Collection of standalone Python tools for market data collection, screening, and analysis.**

Each tool runs independently and stores data in local SQLite databases. Designed to feed into larger analysis pipelines or run as daily scheduled tasks.

---

## Tools

### 13F Institutional Holdings Detector (`13f_detector.py`)
Monitors SEC 13F filings to detect significant institutional position changes. Tracks when major funds open, close, or dramatically resize positions.

### FRED Economic Data Collector (`fred_collector.py`)
Pulls macroeconomic indicators from the Federal Reserve Economic Data API — interest rates, unemployment, CPI, GDP, and more. Enables cross-referencing market signals with macro conditions.

### Historical Price Collector (`price_collector.py`)
Builds a local SQLite database of daily OHLCV prices for S&P 500 constituents and custom ticker lists via yfinance. Powers backtesting across all other tools.

### Dividend Cut Scanner (`dividend_scanner.py`)
Detects dividend cuts from FMP calendar data and scores each cut using a 5-factor composite system. Validated by backtesting 324 events from 1995–2025 showing +6.68% alpha at 60 days (p<0.01). Sends color-coded HTML email alerts.

### Earnings Calendar Collector (`earnings_collector.py` / `earnings_main.py`)
Fetches upcoming earnings dates from Yahoo Finance for the full S&P 500 ticker list. Feeds pre-earnings trade detection in other scanners.

### Short Volume Collector (`short_volume_collector.py`)
Pulls daily short volume data from Fintel API. Tracks short interest trends for cross-signal detection with insider and congressional trading data.
- `finra_short_volume_backfill.py` — Backfills historical FINRA short volume
- `short_interest_backfill.py` — Backfills historical short interest data

### Trading Query Interface (`trading_query.py`)
Unified query tool that connects to all trading databases from one place. Interactive menu mode or direct SQL mode for ad-hoc analysis.

---

## Setup

```bash
git clone https://github.com/KPH3802/trading-utilities.git
cd trading-utilities

pip install requests yfinance pandas

# Each tool may require its own API key:
# - FRED: https://fred.stlouisfed.org/docs/api/api_key.html
# - FMP: https://financialmodelingprep.com/
# - Fintel: https://fintel.io/

# Run any tool directly
python3 13f_detector.py
python3 fred_collector.py
python3 price_collector.py
python3 trading_query.py
```

---

## Related Projects

These utilities support the main analysis tools in my other repos:
- [congress-trade-tracker](https://github.com/KPH3802/congress-trade-tracker) — Congressional stock trade analysis
- [form4-insider-scanner](https://github.com/KPH3802/form4-insider-scanner) — SEC Form 4 insider trading detection
- [options-volume-scanner](https://github.com/KPH3802/options-volume-scanner) — Options volume anomaly detection
- [natural-gas-weather-signals](https://github.com/KPH3802/natural-gas-weather-signals) — NG storage prediction
- [volatility-scanner](https://github.com/KPH3802/volatility-scanner) — HV/IV analysis

---

## Related Projects

This is part of a suite of quantitative research tools:

- [congress-trade-tracker](https://github.com/KPH3802/congress-trade-tracker) — Automated congressional stock trade tracking with 10 detection algorithms and 46K+ backtested signals
- [form4-insider-scanner](https://github.com/KPH3802/form4-insider-scanner) — SEC Form 4 insider transaction detection with cluster scoring and cross-signal enrichment
- [options-volume-scanner](https://github.com/KPH3802/options-volume-scanner) — Unusual options volume detection across S&P 500 stocks
- [volatility-scanner](https://github.com/KPH3802/volatility-scanner) — IV rank, HV patterns, and term structure tracking across 500+ instruments
- [natural-gas-weather-signals](https://github.com/KPH3802/natural-gas-weather-signals) — Weather-driven natural gas storage modeling and trading signals

---

## Connect

[![LinkedIn](https://img.shields.io/badge/LinkedIn-kevin--heaney-blue?logo=linkedin)](https://www.linkedin.com/in/kevin-heaney/)
[![Medium](https://img.shields.io/badge/Medium-@KPH3802-black?logo=medium)](https://medium.com/@KPH3802)

---

## License

MIT
