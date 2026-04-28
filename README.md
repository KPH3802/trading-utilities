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

### YTD Performance Report (`ytd_performance_report.py`)
Generates dollar-P&L and win-rate breakdowns per signal source across three configurable periods (YTD 2026, 2025+, Full 2025). Pulls historical signal fires, applies $5K equal-weight position sizing, and prices each entry/exit via yfinance with cached price series. Outputs per-signal performance tables and summary statistics. Run as one-off analysis when reviewing fund or signal-level results.

### Transfer Coefficient Measurement (`measure_tc.py`)
Computes the GMC Transfer Coefficient per Clarke-de Silva-Thorley (2002), Eq 7 / A15 — the cross-sectional correlation between intended trade weights times sigma and forecast alpha divided by sigma squared. Calculates daily TC since fund inception, a pooled aggregate, and a counterfactual TC assuming no MAX_TOTAL_OPEN cap (used to size capital-attributable alpha leak). Also persists daily 60-day annualized sigma per ticker to `signal_intelligence.db::ticker_sigma_history` so the time series accumulates. Standalone, on-demand.

### Capital Simulation (`capital_simulation.py`)
Simulates portfolio P&L under varying capital and position-count assumptions (current default: $50K capital, $5K per position, 10 max simultaneous). Uses signal priority queue (8K > PEAD > 13F > COT > SI > CEL) to resolve cap-bound entry conflicts. 8K returns sourced from `backtest_results_v2.db`; all other signals priced live via yfinance. Run as one-off analysis. Currently staged — full simulation pending build-out for the $5K / 1-position single-sleeve scenario.

### Layer C Heartbeat (`layer_c_heartbeat.py`)
Out-of-band scanner health watchdog. Pings Healthchecks.io hourly with freshness checks across signal_log, 5 macro DBs, and 4 cron logs. Fails (and pages via Pushover + email) when any source falls outside its weekday-aware SLA. Three-state alerting: success, fail, no-ping (Healthchecks.io alerts after 90 min grace). Production cron: hourly via crontab.

### Scanner Health Monitor (`scanner_health_monitor.py`)
Two-layer execution monitor that catches scanner failures before they cause silent alpha loss. **L1** iterates every enabled PythonAnywhere scheduled task, fetches its log via the PA Files API, and parses the most recent `Completed task ... return code was X` line — alerts fire when return code != 0 or no completion within 90 minutes of expected fire time. **L2** queries `signal_intelligence.db` for `MAX(scan_date)` per scanner and flags drift past the expected weekday-daily / weekly-Tuesday cadence; scanners that should write but produce zero rows (e.g. `DIV_INITIATION`) surface here too. Alerts route through the Layer A Pushover wrapper, are deduplicated within a rolling 6-hour window via `~/.gmc_health_state.json`, and three or more concurrent failures collapse into one `[CRITICAL][GMC]` bundle. Script exits 0 unconditionally so cron stays green. Production cron: every 30 minutes via `~/run_scanner_health_monitor.

### Signal Intelligence Setup (`signal_intelligence_setup.py`)
One-time bootstrap that creates `~/gmc_data/signal_intelligence.db` and backfills historical signal data from every backtest DB in the suite (PEAD, SI, COT, CEL, 13F, 8K, DIV_CUT). This is the permanent signal archive — all future scanner runs append to it. Supports `--verify` mode to print summary statistics without inserting. Run once at fund setup or when rebuilding the signal archive from scratch.

### PA to Mac Signal Sync (sync_signal_intelligence.py)
Nightly consolidator that pulls PythonAnywhere's signal_intelligence.db and merges new rows into Mac Studio's local copy. Uses the PA Files API with atomic stage-and-merge so the live local DB is never half-written. Reports per-scanner fire counts and surfaces zero-fire scanners (a passive form of signal health monitoring). Production cron: 06:15 CT weekdays via ~/run_sync_signal_intelligence.sh.

### PA to Mac F4 Sync (sync_form4_db.py)
Nightly download of PA's form4_insider_trades.db to Mac Studio so ib_autotrader.py reads fresh insider-trading data each morning. Streams via the PA Files API with atomic replace -- the live local DB is only swapped after the staged file passes SQLite integrity, table-presence, and compare-against-incumbent checks (new file must have at least 90 percent of incumbent's sent_alerts rows AND MAX(alert_date) must not regress). Bootstrap mode handles first-usable-run when incumbent is missing or empty. Production cron: 07:50 CT weekdays via ~/run_sync_form4_db.sh, ahead of the 08:00 CT autotrader fire.sh`.

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
