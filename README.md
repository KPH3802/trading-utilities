# Trading Utilities

**Collection of standalone Python tools for market data collection, screening, and analysis. Each tool feeds into a unified systematic trading pipeline.**

Each tool runs independently and stores data in local SQLite databases. Designed as building blocks for the larger signal detection and execution systems in this suite.

---

## Tools

### 13F Institutional Holdings Collector (`13f_detector.py`)
Downloads and parses SEC 13F quarterly filings from a curated universe of high-conviction funds (hedge funds and concentrated long-only managers). Detects institutional initiations — new positions opened by 3+ funds in the same ticker within a quarter. Backtested signal: **+5.26% alpha at 13 weeks (t=10.23), +10.39% at 26 weeks (t=12.55)** with VIX kill switch and 3+ initiator minimum filter required.

### FRED Economic Data Collector (`fred_collector.py`)
Pulls macroeconomic indicators from the Federal Reserve Economic Data API — interest rates, unemployment, CPI, GDP, and more. Enables cross-referencing market signals with macro conditions and regime filters.

### Historical Price Collector (`price_collector.py`)
Builds a local SQLite database of daily OHLCV prices for S&P 500 constituents and custom ticker lists via yfinance. Powers backtesting across all other tools.

### Dividend Cut Scanner (`dividend_scanner.py`)
Detects dividend cuts from FMP calendar data and scores each cut using a 5-factor composite system (yield impact, payout ratio, debt, earnings trend, sector). Score 3+ signals validated across multiple market eras. Sends color-coded HTML email alerts. Live on PythonAnywhere (23:30 UTC nightly). Used for monitoring and position tracking — not wired to IB execution.

### Dividend Initiation Scanner (`dividend_initiation_scanner.py`)
Detects first-ever dividend initiations from FMP data. Backtested signal: **+2.89% alpha at 60 days (p=0.009, n=229)** for true first-ever initiations (excludes reinstatements). Live on PythonAnywhere (23:45 UTC nightly).

### Earnings Calendar Collector (`earnings_collector.py` / `earnings_main.py`)
Fetches upcoming earnings dates from Yahoo Finance for the full S&P 500 ticker list. Feeds pre-earnings trade detection and PEAD (Post-Earnings Announcement Drift) signal generation in other scanners.

### Short Volume Collector (`short_volume_collector.py`)
Pulls daily short volume data from Fintel API. Tracks short interest trends for cross-signal detection with insider and congressional trading data. Short interest squeeze signal backtested at **+10.29% alpha at 4 weeks (t=30.47)** in small caps with inverted signal construction.
- `finra_short_volume_backfill.py` — Backfills historical FINRA short volume
- `short_interest_backfill.py` — Backfills historical short interest data

### Trading Query Interface (`trading_query.py`)
Unified query tool that connects to all trading databases from one place. Interactive menu mode or direct SQL mode for ad-hoc analysis.

### H2 Capital Simulation (`h2_cap_simulation.py`)
Simulates Event Alpha sleeve performance under varying MAX_TOTAL_OPEN cap values (20, 25, 30, 40) and pre/post Followup #3 timing fix. Uses historical signal_log + signal_benchmarks to estimate capture rates and capital-attributable alpha leak. Output feeds the H2 cap-attributable alpha leak memo. Run as one-off analysis, not scheduled.

### Layer C Heartbeat (`layer_c_heartbeat.py`)
Out-of-band scanner health watchdog. Pings Healthchecks.io hourly with freshness checks across signal_log, 5 macro DBs, and 4 cron logs. Fails (and pages via Pushover + email) when any source falls outside its weekday-aware SLA. Three-state alerting: success, fail, no-ping (Healthchecks.io alerts after 90 min grace). Production cron: hourly via crontab.

---

## Setup

```bash
git clone https://github.com/KPH3802/trading-utilities.git
cd trading-utilities
pip install requests yfinance pandas
python3 13f_detector.py
python3 fred_collector.py
python3 price_collector.py
python3 trading_query.py
```

API keys required per tool:
- FRED: https://fred.stlouisfed.org/docs/api/api_key.html
- FMP: https://financialmodelingprep.com/
- Fintel: https://fintel.io/

---

## Related Projects

- [congress-trade-tracker](https://github.com/KPH3802/congress-trade-tracker) — Congressional stock trade monitoring
- [form4-insider-scanner](https://github.com/KPH3802/form4-insider-scanner) — SEC Form 4 insider buying cluster detection
- [options-volume-scanner](https://github.com/KPH3802/options-volume-scanner) — Options volume anomaly research
- [natural-gas-weather-signals](https://github.com/KPH3802/natural-gas-weather-signals) — NG storage signal research
- [volatility-scanner](https://github.com/KPH3802/volatility-scanner) — HV/IV regime monitoring

---

## Disclaimer

This project is for **educational and research purposes only**. Nothing here constitutes financial advice. Backtested results reflect historical data and do not guarantee future performance.

---

## License

MIT
