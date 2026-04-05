#!/usr/bin/env python3
"""
Grist Mill Capital - YTD Performance Report
============================================
Periods:
  A: 2026 YTD     Jan 1, 2026 - Mar 30, 2026
  B: 2025+        Jan 1, 2025 - Mar 30, 2026
  C: Full 2025    Jan 1, 2025 - Dec 31, 2025

Signals: 8-K shorts, PEAD, SI Squeeze, COT, CEL, 13F, Div Cut
"""
import sqlite3, os, csv, sys, warnings
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yfinance as yf
warnings.filterwarnings('ignore')

BASE  = os.path.expanduser('~/Desktop/Claude_Programs/Trading_Programs')
SIZE  = 5000   # position size $

PERIODS = {
    'YTD_2026':  ('2026-01-01', '2026-03-30'),
    '2025_PLUS': ('2025-01-01', '2026-03-30'),
    'FULL_2025': ('2025-01-01', '2025-12-31'),
}

# ?? Price cache ?????????????????????????????????????????????????????????????
_px = {}
def prices(ticker):
    if ticker not in _px:
        d = yf.download(ticker, start='2024-12-15', end='2026-04-05',
                        progress=False, auto_adjust=True)
        if d is not None and not d.empty:
            s = d['Close'].squeeze().dropna()
            s.index = pd.to_datetime(s.index).tz_localize(None)
            _px[ticker] = s
        else:
            _px[ticker] = pd.Series(dtype=float)
    return _px[ticker]

def trade_return(ticker, signal_date, hold_days, direction, period_end):
    """Return for one trade: entry=close on signal_date, exit=close after hold_days.
    Capped at period_end. Returns (ret_pct, actual_entry, actual_exit, capped)."""
    px = prices(ticker)
    if px.empty: return None
    sd = pd.Timestamp(signal_date)
    pe = pd.Timestamp(period_end)
    fwd = px[px.index >= sd]
    if fwd.empty: return None
    entry_px = fwd.iloc[0]
    entry_dt = fwd.index[0]
    after = px[px.index > entry_dt]
    if after.empty: return None
    # Natural exit
    if len(after) >= hold_days:
        nat_exit_dt = after.index[hold_days - 1]
        nat_exit_px = after.iloc[hold_days - 1]
    else:
        nat_exit_dt = after.index[-1]
        nat_exit_px = after.iloc[-1]
    # Cap at period_end
    capped = (nat_exit_dt > pe)
    if capped:
        capped_after = after[after.index <= pe]
        if capped_after.empty: return None
        exit_dt = capped_after.index[-1]
        exit_px = capped_after.iloc[-1]
    else:
        exit_dt = nat_exit_dt
        exit_px = nat_exit_px
    raw = (exit_px - entry_px) / entry_px
    ret = -raw if direction == 'SHORT' else raw
    spy_px = prices('SPY')
    spy_fwd = spy_px[spy_px.index >= entry_dt] if not spy_px.empty else pd.Series(dtype=float)
    if not spy_fwd.empty:
        spy_entry = spy_fwd.iloc[0]
        if capped:
            spy_cap = spy_fwd[spy_fwd.index <= pe]
            spy_exit = spy_cap.iloc[-1] if not spy_cap.empty else spy_fwd.iloc[min(hold_days-1,len(spy_fwd)-1)]
        else:
            spy_exit = spy_fwd.iloc[min(hold_days-1, len(spy_fwd)-1)]
        spy_ret = (spy_exit - spy_entry) / spy_entry
    else:
        spy_ret = 0.0
    return {'ret': float(ret), 'spy': float(spy_ret), 'alpha': float(ret - spy_ret),
            'win': ret > 0, 'entry_dt': str(entry_dt.date()), 'exit_dt': str(exit_dt.date()),
            'capped': capped, 'direction': direction}

def summarize(trades):
    """Summarize list of trade dicts."""
    trades = [t for t in trades if t is not None]
    if not trades: return None
    rets  = [t['ret']   for t in trades]
    alphas= [t['alpha'] for t in trades]
    wins  = [t['win']   for t in trades]
    pnl   = sum(r * SIZE for r in rets)
    return {
        'n': len(trades), 'win_pct': 100*sum(wins)/len(wins),
        'avg_ret': 100*np.mean(rets), 'med_ret': 100*np.median(rets),
        'avg_alpha': 100*np.mean(alphas), 'total_pnl': pnl,
        'total_alpha_pnl': sum(a * SIZE for a in alphas),
    }
# ── Batch prefetch patch ─────────────────────────────────────────────────────
# Replace the single-ticker price cache with a batch downloader.
# Called once per signal with all tickers needed, populates _px cache in bulk.

def prefetch(tickers, start='2024-12-15', end='2026-04-05'):
    """Batch download prices for a list of tickers into _px cache."""
    to_fetch = [t for t in tickers if t not in _px and t and isinstance(t, str)]
    if not to_fetch:
        return
    import pandas as pd
    # yfinance batch download
    chunk_size = 100
    for i in range(0, len(to_fetch), chunk_size):
        chunk = to_fetch[i:i+chunk_size]
        try:
            raw = yf.download(chunk, start=start, end=end,
                              progress=False, auto_adjust=True)
            if raw is None or raw.empty:
                continue
            # Multi-ticker returns MultiIndex columns (Price, Ticker)
            if isinstance(raw.columns, pd.MultiIndex):
                close = raw['Close']
            else:
                close = raw[['Close']] if 'Close' in raw.columns else raw
            for tkr in chunk:
                if tkr in close.columns:
                    s = close[tkr].dropna()
                    s.index = pd.to_datetime(s.index).tz_localize(None)
                    _px[tkr] = s
                else:
                    _px[tkr] = pd.Series(dtype=float)
        except Exception as e:
            # Mark all in chunk as empty so we don't retry
            for tkr in chunk:
                if tkr not in _px:
                    _px[tkr] = pd.Series(dtype=float)
# ── Signal 1: 8-K Item 1.01 SHORT ──────────────────────────────────────────
def run_8k(start, end, period_end):
    """8-K Item 1.01 shorts. SHORT, 5-day hold.
    Uses backtest_results_v2.db which has pre-computed returns with all filters applied:
    price >= $50, SIC exclusions, M&A filter. Pre-computed returns are more accurate
    than estimating from yfinance. DATA LIMITATION: DB ends Feb 13 2026."""
    db = os.path.join(BASE, 'eight_k_research', 'backtest_results_v2.db')
    conn = sqlite3.connect(db)
    try:
        df = pd.read_sql_query("""
            SELECT ticker, filing_date, ret_5d, abnret_5d, mkt_ret_5d, filing_price
            FROM filing_returns
            WHERE item_code = '1.01'
              AND filing_date >= '""" + start + """' AND filing_date <= '""" + end + """'
              AND ret_5d IS NOT NULL
              AND filing_price >= 50
            ORDER BY filing_date
        """, conn)
    except Exception as e:
        conn.close()
        return [], str(e)
    conn.close()
    if df.empty:
        return [], 'No signals in period (check backtest DB date range)'
    trades = []
    for _, row in df.iterrows():
        # SHORT: our return = -ret_5d, our alpha = -abnret_5d
        ret = -float(row['ret_5d'])
        alpha = -float(row['abnret_5d'])
        spy = float(row['mkt_ret_5d']) if row['mkt_ret_5d'] is not None else 0.0
        trades.append({
            'ret': ret, 'spy': spy, 'alpha': alpha,
            'win': ret > 0, 'direction': 'SHORT',
            'entry_dt': row['filing_date'], 'exit_dt': '', 'capped': False
        })
    return trades, None

# ── Signal 2: PEAD ──────────────────────────────────────────────────────────
def run_pead(start, end, period_end):
    """PEAD: EPS surprise >= +/-5%. BULL=LONG, BEAR=SHORT. 28-day hold.
    Q4 exclusion: skip Jan/Feb report dates. Full coverage through Mar 27 2026."""
    db = os.path.join(BASE, 'pead_backtest', 'pead_data.db')
    conn = sqlite3.connect(db)
    try:
        df = pd.read_sql_query("""
            SELECT e.ticker, e.report_date, e.surprise_pct
            FROM earnings e
            WHERE e.report_date >= '""" + start + """' AND e.report_date <= '""" + end + """'
              AND e.eps_actual IS NOT NULL AND e.eps_estimated IS NOT NULL
              AND ABS(e.eps_estimated) >= 0.01
              AND (e.surprise_pct >= 5 OR e.surprise_pct <= -5)
            ORDER BY e.report_date
        """, conn)
    except Exception as e:
        conn.close()
        return [], str(e)
    conn.close()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df[~df['report_date'].dt.month.isin([1, 2])]
    tickers = df['ticker'].dropna().unique().tolist()
    print('prefetching ' + str(len(tickers)) + ' PEAD tickers...', end=' ', flush=True)
    prefetch(tickers)
    trades = []
    for _, row in df.iterrows():
        direction = 'LONG' if row['surprise_pct'] >= 5 else 'SHORT'
        t = trade_return(row['ticker'], str(row['report_date'].date()), 28, direction, period_end)
        if t:
            trades.append(t)
    return trades, None

# ── Signal 3: Short Interest Squeeze ────────────────────────────────────────
def run_si(start, end, period_end):
    """SI Squeeze: 30%+ SI increase on SC exchange -> LONG. 28-day hold.
    DATA LIMITATION: Fintel DB caps at Jan 15 2026. Only 1 period in YTD 2026."""
    db = os.path.join(BASE, 'short_interest', 'short_interest.db')
    conn = sqlite3.connect(db)
    try:
        df = pd.read_sql_query("""
            SELECT ticker, settlement_date, change_percent, market_class
            FROM short_interest
            WHERE settlement_date >= '""" + start + """' AND settlement_date <= '""" + end + """'
              AND change_percent >= 30
              AND market_class = 'SC'
              AND short_position >= 100000
              AND avg_daily_volume >= 100000
            ORDER BY settlement_date
        """, conn)
    except Exception as e:
        conn.close()
        return [], str(e)
    conn.close()
    if df.empty:
        return [], 'No signals in period'
    tickers = df['ticker'].dropna().unique().tolist()
    print('prefetching ' + str(len(tickers)) + ' SI tickers...', end=' ', flush=True)
    prefetch(tickers)
    trades = []
    for _, row in df.iterrows():
        t = trade_return(row['ticker'], row['settlement_date'], 28, 'LONG', period_end)
        if t:
            trades.append(t)
    return trades, None

# ── Signal 4: COT ────────────────────────────────────────────────────────────
def run_cot(start, end, period_end):
    """COT: commercial net position percentile extremes. Uses cot_scanner.db.
    Vehicles: Crude->XOP(BULL 65d)/USO(BEAR 40d), Gold->GLD(40d), Wheat->WEAT(40d), Corn->CORN(65d).
    Full coverage through Mar 24 2026."""
    db = os.path.join(BASE, 'cot_scanner', 'cot_scanner.db')
    conn = sqlite3.connect(db)
    try:
        df = pd.read_sql_query("""
            SELECT report_date, commodity, net_commercial
            FROM cot_data
            WHERE report_date >= '2022-01-01'
            ORDER BY report_date
        """, conn)
    except Exception as e:
        conn.close()
        return [], str(e)
    conn.close()
    if df.empty:
        return [], 'No COT data'
    df['report_date'] = pd.to_datetime(df['report_date'])
    COMMODITY_MAP = {
        'WTI Crude Oil': ('XOP', 'USO', 65, 40),
        'Gold':          ('GLD', 'GLD', 40, 40),
        'Wheat (SRW)':   ('WEAT','WEAT',40, 40),
        'Corn':          ('CORN','CORN',65, 65),
    }
    LOOKBACK = 156
    BULL_PCT = 80
    BEAR_PCT = 20
    trades = []
    for commodity, grp in df.groupby('commodity'):
        if commodity not in COMMODITY_MAP:
            continue
        bull_v, bear_v, bull_hold, bear_hold = COMMODITY_MAP[commodity]
        grp = grp.sort_values('report_date').reset_index(drop=True)
        for i, row in grp.iterrows():
            if i < LOOKBACK:
                continue
            window = grp.iloc[i-LOOKBACK:i]['net_commercial']
            pct = (window < row['net_commercial']).mean() * 100
            sig_date = str(row['report_date'].date())
            if sig_date < start or sig_date > end:
                continue
            if pct >= BULL_PCT:
                t = trade_return(bull_v, sig_date, bull_hold, 'LONG', period_end)
                if t:
                    trades.append(t)
            elif pct <= BEAR_PCT:
                v = 'USO' if commodity == 'WTI Crude Oil' else bear_v
                t = trade_return(v, sig_date, bear_hold, 'SHORT', period_end)
                if t:
                    trades.append(t)
    return trades, None

# ── Signal 5: Commodity-Equity Lag ──────────────────────────────────────────
def run_cel(start, end, period_end):
    """CEL: USO daily drop >= 2% -> SHORT XOP/XLE/CVX/XOM/COP, 5-day hold.
    Pure yfinance, full coverage."""
    uso = prices('USO')
    if uso.empty:
        return [], 'No USO data'
    uso_rets = uso.pct_change()
    sd, ed = pd.Timestamp(start), pd.Timestamp(end)
    signals = uso_rets[(uso_rets.index >= sd) & (uso_rets.index <= ed) & (uso_rets <= -0.02)]
    targets = ['XOP','XLE','CVX','XOM','COP']
    trades = []
    for dt in signals.index:
        for tkr in targets:
            t = trade_return(tkr, str(dt.date()), 5, 'SHORT', period_end)
            if t:
                trades.append(t)
    return trades, None

# ── Signal 6: 13F Initiations ───────────────────────────────────────────────
def run_13f(start, end, period_end):
    """13F: 3+ initiators same quarter -> LONG, 91-day hold.
    VIX kill switch at 30. In 2026 YTD: Q4 2025 filings only (filed Feb 2026)."""
    db = os.path.join(BASE, 'thirteenf_backtest', 'thirteenf_data.db')
    conn = sqlite3.connect(db)
    try:
        df = pd.read_sql_query("""
            SELECT s.ticker, s.quarter_end, s.filing_date, s.new_initiations
            FROM initiation_signals s
            WHERE s.filing_date >= '""" + start + """' AND s.filing_date <= '""" + end + """'
              AND s.new_initiations >= 3
            ORDER BY s.filing_date
        """, conn)
    except Exception as e:
        conn.close()
        return [], str(e)
    conn.close()
    if df.empty:
        return [], 'No 13F signals in period'
    vix = prices('^VIX')
    tickers = df['ticker'].dropna().unique().tolist()
    print('prefetching ' + str(len(tickers)) + ' 13F tickers...', end=' ', flush=True)
    prefetch(tickers)
    trades = []
    for _, row in df.iterrows():
        fd = pd.Timestamp(row['filing_date'])
        if not vix.empty:
            vix_avail = vix[vix.index <= fd]
            if not vix_avail.empty and vix_avail.iloc[-1] > 30:
                continue
        t = trade_return(row['ticker'], row['filing_date'], 91, 'LONG', period_end)
        if t:
            trades.append(t)
    return trades, None

# ── Signal 7: Div Cut Score 3+ ───────────────────────────────────────────────
def run_divcot(start, end, period_end):
    """Div Cut Score 3+: LONG, 60-day hold, -39.9% catastrophic breaker.
    DATA: scored_results.csv through Oct 2025 only. No 2026 systematic data."""
    csv_path = os.path.join(BASE, 'dividend_backtest', 'results', 'scored_results.csv')
    if not os.path.exists(csv_path):
        return [], 'scored_results.csv not found'
    rows = []
    with open(csv_path) as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            try:
                if float(r.get('net_score', 0)) >= 3:
                    ed_val = r.get('entry_date','')
                    if ed_val >= start and ed_val <= end:
                        rows.append(r)
            except:
                pass
    if not rows:
        return [], 'scored_results.csv ends Oct 2025 -- no data in window'
    tickers = list(set(r['ticker'] for r in rows))
    print('prefetching ' + str(len(tickers)) + ' DivCut tickers...', end=' ', flush=True)
    prefetch(tickers)
    trades = []
    for r in rows:
        t = trade_return(r['ticker'], r['entry_date'], 60, 'LONG', period_end)
        if t:
            if t['ret'] <= -0.399:
                t['ret'] = -0.399
            trades.append(t)
    return trades, None
def spy_benchmark(start, end):
    spy = prices('SPY')
    if spy.empty: return None
    import pandas as pd
    sd, ed = pd.Timestamp(start), pd.Timestamp(end)
    avail = spy[(spy.index >= sd) & (spy.index <= ed)]
    if avail.empty or len(avail) < 2: return None
    return (avail.iloc[-1] - avail.iloc[0]) / avail.iloc[0]

def fmt(val, pct=False, dollar=False, n=False):
    if val is None: return 'N/A'
    if n: return str(int(val))
    if dollar: return '${:+,.0f}'.format(val)
    if pct: return '{:+.2f}%'.format(val)
    return str(val)

def print_report(all_results, PERIODS):
    from datetime import datetime
    SEP = '=' * 112
    print()
    print(SEP)
    print('GRIST MILL CAPITAL -- SIGNAL PERFORMANCE REPORT')
    print('Generated: ' + datetime.now().strftime('%Y-%m-%d %H:%M'))
    print(SEP)
    period_labels = {
        'YTD_2026':  '2026 YTD  (Jan 1 - Mar 30, 2026)',
        '2025_PLUS': '2025-Now  (Jan 1, 2025 - Mar 30, 2026)  [15 months]',
        'FULL_2025': 'Full 2025 (Jan 1, 2025 - Dec 31, 2025)',
    }
    signal_labels = {
        '8K':     '8-K Item 1.01 (SHORT 5d)',
        'PEAD':   'PEAD Bull+Bear (28d)',
        'SI':     'SI Squeeze (LONG 28d)',
        'COT':    'COT Multi-Commodity',
        'CEL':    'Commodity-Equity Lag (SHORT 5d)',
        '13F':    '13F Initiations (LONG 91d)',
        'DIVCOT': 'Div Cut Score 3+ (LONG 60d)',
    }
    for period_key, (start, end) in PERIODS.items():
        spy_ret = spy_benchmark(start, end)
        spy_str = fmt(spy_ret*100 if spy_ret is not None else None, pct=True)
        results = all_results[period_key]
        print()
        print('  PERIOD: ' + period_labels[period_key])
        print('  SPY benchmark: ' + spy_str)
        print()
        print('  {:<32} {:>5} {:>7} {:>8} {:>9} {:>11}  {}'.format(
            'Signal','N','Win%','AvgRet','AvgAlpha','TotalPnL','Status/Notes'))
        print('  ' + '-' * 108)
        all_t = []
        for sig_key, label in signal_labels.items():
            entry = results.get(sig_key, (None, 'Not run', ''))
            trades, err, note = entry
            s = summarize(trades) if trades else None
            if s:
                status = 'OK -- ' + note if note else 'OK'
                print('  {:<32} {:>5} {:>7} {:>8} {:>9} {:>11}  {}'.format(
                    label, fmt(s['n'],n=True), fmt(s['win_pct'],pct=True),
                    fmt(s['avg_ret'],pct=True), fmt(s['avg_alpha'],pct=True),
                    fmt(s['total_pnl'],dollar=True), status))
                all_t.extend(trades)
            elif err and err not in ('None',''):
                print('  {:<32} {:>5} {:>7} {:>8} {:>9} {:>11}  DATA GAP: {}'.format(
                    label,'--','--','--','--','--', err))
            else:
                print('  {:<32} {:>5} {:>7} {:>8} {:>9} {:>11}  No signals in period'.format(
                    label,'0','--','--','--','$0'))
        print('  ' + '-' * 108)
        total = summarize(all_t)
        if total:
            print('  {:<32} {:>5} {:>7} {:>8} {:>9} {:>11}'.format(
                'PORTFOLIO TOTAL',
                fmt(total['n'],n=True), fmt(total['win_pct'],pct=True),
                fmt(total['avg_ret'],pct=True), fmt(total['avg_alpha'],pct=True),
                fmt(total['total_pnl'],dollar=True)))
        print()
    print(SEP)
    print('DATA NOTES:')
    print('  8-K:    Filing DB ends Feb 13 2026 -- gap Feb 14 to Mar 30 (45 days missing in YTD/2025+)')
    print('  SI:     Fintel DB caps Jan 15 2026 -- only 1 of ~6 YTD reporting periods available')
    print('  13F:    Quarterly -- Q4 2025 filings in window; no Q1 2026 until May 2026')
    print('  DivCut: scored_results.csv ends Oct 2025; no 2026 systematic data')
    print('  COT:    Full coverage through Mar 24 2026')
    print('  PEAD:   Full coverage through Mar 27 2026')
    print('  CEL:    Full coverage (pure yfinance)')
    print(SEP)

SIGNAL_RUNNERS = {
    '8K':     run_8k,
    'PEAD':   run_pead,
    'SI':     run_si,
    'COT':    run_cot,
    'CEL':    run_cel,
    '13F':    run_13f,
    'DIVCOT': run_divcot,
}

print('Grist Mill Capital -- YTD Report', flush=True)
print('Pre-loading common prices...', flush=True)
for t in ['SPY','QQQ','^VIX','USO','XOP','XLE','CVX','XOM','COP','GLD','WEAT','CORN']:
    prices(t)
print('Done.', flush=True)

all_results = {}
for period_key, (start, end) in PERIODS.items():
    print('Running period: ' + period_key + ' (' + start + ' to ' + end + ')...', flush=True)
    all_results[period_key] = {}
    for sig_key, runner in SIGNAL_RUNNERS.items():
        print('  ' + sig_key + '...', end=' ', flush=True)
        try:
            result = runner(start, end, end)
            if isinstance(result, tuple) and len(result) == 2:
                trades, err = result
                note = ''
            else:
                trades, err, note = result
        except Exception as ex:
            import traceback
            trades, err, note = None, str(ex), ''
            traceback.print_exc()
        all_results[period_key][sig_key] = (trades, err, note)
        n = len(trades) if trades else 0
        print(str(n) + ' trades', flush=True)

print_report(all_results, PERIODS)
