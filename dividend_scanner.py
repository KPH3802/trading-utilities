#!/usr/bin/env python3
"""
Dividend Cut Scanner — Live Trading Signal Generator

Runs daily on PythonAnywhere. Detects dividend cuts from FMP calendar,
scores each cut using a 5-factor composite system validated by backtesting
(324 events, 1995-2025), and sends color-coded HTML email alerts.

Backtest results:
  - All cuts: +6.68% alpha at 60d (p<0.01)
  - Score 3+: +19.65% alpha at 60d, 74.3% win rate
  - Score 0:  -5.14% alpha at 60d, 34.7% win rate

Composite scoring:
  POSITIVE (+1 each): severe cut 75%+, good sector, bear market, Q1, cheap stock
  NEGATIVE (+1 each): moderate cut <50%, bad sector, bull market, expensive stock

Usage:
  python3 dividend_scanner.py              # Normal daily run
  python3 dividend_scanner.py --test-email # Send test email
  python3 dividend_scanner.py --backfill 30 # Check last 30 days
  python3 dividend_scanner.py --status     # Show database stats
"""
import os
import sys
import json
import sqlite3
import smtplib
import traceback
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import config

# ============================================================
# GLOBALS
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, config.DB_NAME)
FMP_BASE = "https://financialmodelingprep.com"

# ============================================================
# DATABASE
# ============================================================
def init_db():
    """Create tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS dividend_cuts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            declaration_date TEXT NOT NULL,
            record_date TEXT,
            payment_date TEXT,
            new_dividend REAL,
            old_dividend REAL,
            cut_pct REAL,
            entry_price REAL,
            sector TEXT,
            spy_trailing_60d REAL,
            pos_score INTEGER,
            neg_score INTEGER,
            net_score INTEGER,
            pos_flags TEXT,
            neg_flags TEXT,
            signal TEXT,
            detected_date TEXT,
            UNIQUE(ticker, declaration_date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sector_cache (
            ticker TEXT PRIMARY KEY,
            sector TEXT,
            updated TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS scan_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT,
            declarations_checked INTEGER,
            cuts_found INTEGER,
            alerts_sent INTEGER,
            errors TEXT
        )
    """)

    conn.commit()
    return conn


# ============================================================
# FMP API
# ============================================================
def fmp_fetch(endpoint, params=None):
    """Fetch JSON from FMP API."""
    if not config.FMP_API_KEY:
        print("ERROR: FMP_API_KEY not set")
        return None

    url = f"{FMP_BASE}/{endpoint}?apikey={config.FMP_API_KEY}"
    if params:
        for k, v in params.items():
            url += f"&{k}={v}"

    try:
        req = Request(url)
        req.add_header('User-Agent', 'DividendScanner/1.0')
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        print(f"  FMP HTTP Error {e.code}: {endpoint}")
        return None
    except URLError as e:
        print(f"  FMP URL Error: {e.reason}")
        return None
    except Exception as e:
        print(f"  FMP Error: {e}")
        return None


def get_dividend_calendar(from_date, to_date):
    """Fetch dividend calendar from FMP for a date range."""
    data = fmp_fetch("api/v3/stock_dividend_calendar", {
        'from': from_date,
        'to': to_date
    })
    if data and isinstance(data, list):
        return data
    return []


def get_historical_dividends(ticker):
    """Fetch historical dividend data for a ticker."""
    data = fmp_fetch(f"api/v3/historical-price-full/stock_dividend/{ticker}")
    if data and isinstance(data, dict) and 'historical' in data:
        return data['historical']
    return []


def get_quote(ticker):
    """Fetch current quote for a ticker."""
    data = fmp_fetch(f"api/v3/quote/{ticker}")
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def get_spy_history():
    """Fetch SPY price history for trailing return calculation."""
    data = fmp_fetch("api/v3/historical-price-full/SPY", {
        'serietype': 'line'
    })
    if data and isinstance(data, dict) and 'historical' in data:
        return {d['date']: d['close'] for d in data['historical']}
    return {}


# ============================================================
# SECTOR LOOKUP (yfinance with SQLite cache)
# ============================================================
def get_sector(ticker, conn):
    """Get sector for ticker, using cache first."""
    c = conn.cursor()
    c.execute("SELECT sector FROM sector_cache WHERE ticker = ?", (ticker,))
    row = c.fetchone()
    if row:
        return row[0]

    # Fetch from yfinance
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        sector = info.get('sector', 'Unknown')
    except Exception:
        sector = 'Unknown'

    # Cache it
    c.execute("INSERT OR REPLACE INTO sector_cache (ticker, sector, updated) VALUES (?, ?, ?)",
              (ticker, sector, datetime.now().strftime('%Y-%m-%d')))
    conn.commit()
    return sector


# ============================================================
# CUT DETECTION
# ============================================================
def detect_cuts(calendar_entries):
    """
    For each calendar entry, fetch historical dividends and detect cuts.
    Returns list of cut events with details.
    """
    cuts = []
    tickers_checked = set()
    errors = []

    for entry in calendar_entries:
        ticker = entry.get('symbol', '')
        decl_date = entry.get('date', '') or entry.get('declarationDate', '')
        new_div = entry.get('dividend', 0) or entry.get('adjDividend', 0)

        if not ticker or not decl_date or not new_div:
            continue

        # Skip duplicates within same run
        key = f"{ticker}_{decl_date}"
        if key in tickers_checked:
            continue
        tickers_checked.add(key)

        # Skip ETFs, funds, preferred shares
        if len(ticker) > 5 or '.' in ticker or '-' in ticker:
            continue

        try:
            new_div = float(new_div)
        except (ValueError, TypeError):
            continue

        if new_div <= 0:
            continue

        # Fetch historical dividends
        hist = get_historical_dividends(ticker)
        if not hist or len(hist) < 2:
            continue

        # Sort by date descending (FMP default)
        hist.sort(key=lambda x: x.get('date', ''), reverse=True)

        # Find the previous dividend BEFORE this declaration
        old_div = None
        for h in hist:
            h_date = h.get('date', '')
            h_div = h.get('dividend', 0) or h.get('adjDividend', 0)
            try:
                h_div = float(h_div)
            except (ValueError, TypeError):
                continue

            if h_date < decl_date and h_div > 0:
                old_div = h_div
                break

        if old_div is None or old_div <= 0:
            continue

        # Calculate cut percentage
        cut_pct = (1 - new_div / old_div) * 100

        # Only track cuts >= minimum threshold
        if cut_pct < config.MIN_CUT_PCT:
            continue

        # Special dividend filter: skip if "special" in label
        label = entry.get('label', '').lower()
        if 'special' in label:
            continue

        cuts.append({
            'ticker': ticker,
            'declaration_date': decl_date,
            'record_date': entry.get('recordDate', ''),
            'payment_date': entry.get('paymentDate', ''),
            'new_dividend': new_div,
            'old_dividend': old_div,
            'cut_pct': round(cut_pct, 1),
            'label': entry.get('label', ''),
        })

    return cuts, errors


# ============================================================
# COMPOSITE SCORING
# ============================================================
def score_cut(cut, conn, spy_prices):
    """Score a dividend cut using the 5-factor composite system."""
    pos_score = 0
    neg_score = 0
    pos_flags = []
    neg_flags = []

    ticker = cut['ticker']
    cut_pct = cut['cut_pct']
    decl_date = cut['declaration_date']

    # --- Get current price ---
    quote = get_quote(ticker)
    entry_price = quote.get('price', 0) if quote else 0
    cut['entry_price'] = entry_price

    # --- Get sector ---
    sector = get_sector(ticker, conn)
    cut['sector'] = sector

    # --- Get SPY trailing 60d return ---
    spy_trail = 0.0
    if spy_prices:
        sorted_dates = sorted(spy_prices.keys(), reverse=True)
        # Find closest date on or before declaration
        current_price = None
        past_price = None
        count = 0
        for d in sorted_dates:
            if d <= decl_date:
                if current_price is None:
                    current_price = spy_prices[d]
                count += 1
                if count >= 60:
                    past_price = spy_prices[d]
                    break
        if current_price and past_price:
            spy_trail = (current_price / past_price - 1) * 100
    cut['spy_trailing_60d'] = round(spy_trail, 2)

    # ========================================
    # POSITIVE FACTORS (+1 each, max 5)
    # ========================================

    # 1. Cut size: 75%+ (sweet spot)
    if cut_pct >= 75:
        pos_score += 1
        if cut_pct >= 90:
            pos_flags.append('CUT_90+')
        else:
            pos_flags.append('CUT_75_90')

    # 2. Sector
    if sector in config.POSITIVE_SECTORS:
        pos_score += 1
        pos_flags.append(f'SECTOR_{sector[:4].upper()}')

    # 3. Market regime: Bear
    if spy_trail < config.BEAR_MARKET_THRESHOLD:
        pos_score += 1
        pos_flags.append('BEAR_MKT')

    # 4. Seasonality: Q1
    try:
        month = datetime.strptime(decl_date, '%Y-%m-%d').month
    except ValueError:
        month = 0
    if month in config.Q1_MONTHS:
        pos_score += 1
        pos_flags.append('Q1')

    # 5. Price level: Cheap
    if 0 < entry_price <= config.CHEAP_PRICE_MAX:
        pos_score += 1
        pos_flags.append('CHEAP')

    # ========================================
    # NEGATIVE FACTORS (+1 each, max 4)
    # ========================================

    # 1. Moderate cut
    if cut_pct < config.MODERATE_CUT_MAX:
        neg_score += 1
        neg_flags.append('MODERATE_CUT')

    # 2. Bad sector
    if sector in config.NEGATIVE_SECTORS:
        neg_score += 1
        neg_flags.append('BAD_SECTOR')

    # 3. Bull market
    if spy_trail > config.BULL_MARKET_THRESHOLD:
        neg_score += 1
        neg_flags.append('BULL_MKT')

    # 4. Expensive
    if entry_price > config.EXPENSIVE_PRICE_MIN:
        neg_score += 1
        neg_flags.append('EXPENSIVE')

    # ========================================
    # SIGNAL CLASSIFICATION
    # ========================================
    net = pos_score - neg_score
    if net >= 2 and cut_pct >= config.CUT_SEVERE_MIN:
        signal = 'STRONG_BUY'
    elif net >= 1 and cut_pct >= config.CUT_SEVERE_MIN:
        signal = 'BUY'
    elif net <= -1:
        signal = 'AVOID'
    elif cut_pct >= config.CUT_SEVERE_MIN:
        signal = 'WATCH'
    else:
        signal = 'MONITOR'

    cut['pos_score'] = pos_score
    cut['neg_score'] = neg_score
    cut['net_score'] = net
    cut['pos_flags'] = ','.join(pos_flags)
    cut['neg_flags'] = ','.join(neg_flags)
    cut['signal'] = signal

    return cut


# ============================================================
# DATABASE STORAGE
# ============================================================
def store_cut(conn, cut):
    """Store a scored cut in the database. Returns True if new, False if duplicate."""
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO dividend_cuts
            (ticker, declaration_date, record_date, payment_date,
             new_dividend, old_dividend, cut_pct, entry_price, sector,
             spy_trailing_60d, pos_score, neg_score, net_score,
             pos_flags, neg_flags, signal, detected_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cut['ticker'], cut['declaration_date'], cut.get('record_date', ''),
            cut.get('payment_date', ''), cut['new_dividend'], cut['old_dividend'],
            cut['cut_pct'], cut['entry_price'], cut['sector'],
            cut['spy_trailing_60d'], cut['pos_score'], cut['neg_score'],
            cut['net_score'], cut['pos_flags'], cut['neg_flags'],
            cut['signal'], datetime.now().strftime('%Y-%m-%d')
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # Duplicate — already seen this ticker+date
        return False


# ============================================================
# EMAIL
# ============================================================
def build_email_html(new_cuts, all_recent):
    """Build HTML email with scored dividend cuts."""
    today = datetime.now().strftime('%Y-%m-%d')

    # Signal colors
    signal_colors = {
        'STRONG_BUY': '#00c853',
        'BUY': '#4caf50',
        'WATCH': '#ff9800',
        'MONITOR': '#9e9e9e',
        'AVOID': '#f44336',
    }

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Arial, sans-serif; background: #1a1a2e; color: #e0e0e0; }}
            .container {{ max-width: 700px; margin: 0 auto; padding: 20px; }}
            h1 {{ color: #00e676; font-size: 22px; border-bottom: 2px solid #333; padding-bottom: 10px; }}
            h2 {{ color: #64b5f6; font-size: 18px; margin-top: 30px; }}
            .summary {{ background: #16213e; border-radius: 8px; padding: 15px; margin: 15px 0; }}
            .cut-card {{
                background: #16213e;
                border-radius: 8px;
                padding: 15px;
                margin: 10px 0;
                border-left: 4px solid #444;
            }}
            .cut-card.strong-buy {{ border-left-color: #00c853; }}
            .cut-card.buy {{ border-left-color: #4caf50; }}
            .cut-card.watch {{ border-left-color: #ff9800; }}
            .cut-card.avoid {{ border-left-color: #f44336; }}
            .cut-card.monitor {{ border-left-color: #9e9e9e; }}
            .ticker {{ font-size: 20px; font-weight: bold; color: #fff; }}
            .signal-badge {{
                display: inline-block;
                padding: 3px 10px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: bold;
                color: #fff;
            }}
            .score {{ font-size: 14px; color: #aaa; }}
            .flags {{ font-size: 12px; color: #81c784; margin-top: 5px; }}
            .neg-flags {{ font-size: 12px; color: #ef9a9a; }}
            .metrics {{ display: flex; gap: 20px; margin-top: 10px; }}
            .metric {{ text-align: center; }}
            .metric-value {{ font-size: 16px; font-weight: bold; color: #fff; }}
            .metric-label {{ font-size: 11px; color: #888; }}
            table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
            th {{ background: #0f3460; color: #e0e0e0; padding: 8px; text-align: left; font-size: 12px; }}
            td {{ padding: 8px; border-bottom: 1px solid #333; font-size: 13px; }}
            .backtest-note {{
                background: #0d2137;
                border: 1px solid #1a5276;
                border-radius: 8px;
                padding: 12px;
                margin: 20px 0;
                font-size: 12px;
                color: #7fb3d8;
            }}
            .footer {{ color: #666; font-size: 11px; margin-top: 30px; border-top: 1px solid #333; padding-top: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>DIVIDEND CUT SCANNER — {today}</h1>
    """

    if new_cuts:
        # Sort: STRONG_BUY first, then BUY, WATCH, MONITOR, AVOID
        signal_order = {'STRONG_BUY': 0, 'BUY': 1, 'WATCH': 2, 'MONITOR': 3, 'AVOID': 4}
        new_cuts.sort(key=lambda x: (signal_order.get(x['signal'], 5), -x['net_score']))

        strong_buys = [c for c in new_cuts if c['signal'] == 'STRONG_BUY']
        buys = [c for c in new_cuts if c['signal'] == 'BUY']

        html += f"""
            <div class="summary">
                <strong>{len(new_cuts)} new dividend cut(s) detected</strong><br>
                STRONG BUY: {len(strong_buys)} &nbsp;|&nbsp;
                BUY: {len(buys)} &nbsp;|&nbsp;
                Other: {len(new_cuts) - len(strong_buys) - len(buys)}
            </div>
        """

        for cut in new_cuts:
            signal = cut['signal']
            color = signal_colors.get(signal, '#9e9e9e')
            css_class = signal.lower().replace('_', '-')

            html += f"""
            <div class="cut-card {css_class}">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span class="ticker">{cut['ticker']}</span>
                    <span class="signal-badge" style="background: {color};">{signal}</span>
                </div>

                <div class="metrics">
                    <div class="metric">
                        <div class="metric-value" style="color: #ef5350;">-{cut['cut_pct']:.0f}%</div>
                        <div class="metric-label">CUT SIZE</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">${cut['entry_price']:.2f}</div>
                        <div class="metric-label">PRICE</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{cut['pos_score']}/5</div>
                        <div class="metric-label">POS SCORE</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{cut['neg_score']}/4</div>
                        <div class="metric-label">RED FLAGS</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value" style="color: {'#00c853' if cut['net_score'] >= 2 else '#ff9800' if cut['net_score'] >= 0 else '#f44336'};">{cut['net_score']:+d}</div>
                        <div class="metric-label">NET SCORE</div>
                    </div>
                </div>

                <div class="score">
                    Sector: {cut['sector']} &nbsp;|&nbsp;
                    SPY 60d: {cut['spy_trailing_60d']:+.1f}% &nbsp;|&nbsp;
                    ${cut['old_dividend']:.4f} → ${cut['new_dividend']:.4f}
                </div>
            """

            if cut['pos_flags']:
                html += f'<div class="flags">+ {cut["pos_flags"]}</div>'
            if cut['neg_flags']:
                html += f'<div class="neg-flags">- {cut["neg_flags"]}</div>'

            html += "</div>"

        # Backtest reference
        html += """
            <div class="backtest-note">
                <strong>Backtest Reference (324 events, 1995-2025):</strong><br>
                Score 3+: +19.65% alpha at 60d, 74.3% win rate<br>
                Score 2: +6.90% alpha at 60d, 66.3% win rate<br>
                Score 0: -5.14% alpha at 60d, 34.7% win rate<br>
                <em>Insider buying around cuts = caution flag (weakens signal)</em>
            </div>
        """
    else:
        html += """
            <div class="summary">
                No new dividend cuts detected today.
            </div>
        """

    # Recent history table
    if all_recent:
        html += """
            <h2>Recent Cut History (last 30 days)</h2>
            <table>
                <tr>
                    <th>Date</th><th>Ticker</th><th>Cut%</th>
                    <th>Price</th><th>Score</th><th>Signal</th>
                </tr>
        """
        for r in all_recent[:20]:
            color = signal_colors.get(r[6], '#9e9e9e')
            html += f"""
                <tr>
                    <td>{r[0]}</td>
                    <td><strong>{r[1]}</strong></td>
                    <td style="color: #ef5350;">-{r[2]:.0f}%</td>
                    <td>${r[3]:.2f}</td>
                    <td>{r[4]:+d}</td>
                    <td style="color: {color};">{r[6]}</td>
                </tr>
            """
        html += "</table>"

    html += f"""
            <div class="footer">
                Dividend Cut Scanner v1.0 | Composite scoring based on backtested sub-signals<br>
                Factors: cut size, sector, market regime (SPY 60d), seasonality, price level<br>
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
            </div>
        </div>
    </body>
    </html>
    """

    return html


def send_email(subject, html_body):
    """Send HTML email."""
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = config.EMAIL_SENDER
    msg['To'] = config.EMAIL_RECIPIENT
    msg.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.EMAIL_SENDER, config.EMAIL_PASSWORD)
            server.sendmail(config.EMAIL_SENDER, config.EMAIL_RECIPIENT, msg.as_string())
        print("  Email sent successfully")
        return True
    except Exception as e:
        print(f"  ERROR sending email: {e}")
        return False


# ============================================================
# MAIN SCANNER LOGIC
# ============================================================
def run_scan(lookback_days=None):
    """Main scan: fetch calendar, detect cuts, score, store, email."""
    print(f"\n{'='*60}")
    print(f"DIVIDEND CUT SCANNER — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    if lookback_days is None:
        lookback_days = config.LOOKBACK_DAYS

    conn = init_db()
    errors = []

    # Step 1: Fetch dividend calendar
    to_date = datetime.now().strftime('%Y-%m-%d')
    from_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    print(f"\n1. Fetching dividend calendar: {from_date} to {to_date}")

    calendar = get_dividend_calendar(from_date, to_date)
    print(f"   Found {len(calendar)} calendar entries")

    if not calendar:
        print("   No calendar entries. Sending status email.")
        recent = get_recent_cuts(conn)
        subject = f"Dividend Scanner — No new declarations ({to_date})"
        html = build_email_html([], recent)
        send_email(subject, html)
        log_scan(conn, 0, 0, 0, "No calendar entries")
        conn.close()
        return

    # Step 2: Detect cuts
    print(f"\n2. Detecting cuts (threshold: >={config.MIN_CUT_PCT}%)...")
    cuts, detect_errors = detect_cuts(calendar)
    errors.extend(detect_errors)
    print(f"   Found {len(cuts)} cuts >= {config.MIN_CUT_PCT}%")

    if not cuts:
        print("   No cuts detected. Sending status email.")
        recent = get_recent_cuts(conn)
        subject = f"Dividend Scanner — {len(calendar)} declarations, no cuts ({to_date})"
        html = build_email_html([], recent)
        send_email(subject, html)
        log_scan(conn, len(calendar), 0, 0, "No cuts found")
        conn.close()
        return

    # Step 3: Get SPY prices for market regime scoring
    print(f"\n3. Fetching SPY price history for market regime scoring...")
    spy_prices = get_spy_history()
    print(f"   SPY prices: {len(spy_prices)} dates")

    # Step 4: Score each cut
    print(f"\n4. Scoring {len(cuts)} cuts...")
    new_cuts = []
    for cut in cuts:
        print(f"   Scoring {cut['ticker']} ({cut['cut_pct']:.0f}% cut)...")
        scored = score_cut(cut, conn, spy_prices)

        # Store in database
        is_new = store_cut(conn, scored)
        if is_new:
            new_cuts.append(scored)
            print(f"     → {scored['signal']} | Net: {scored['net_score']:+d} | "
                  f"+flags: {scored['pos_flags']} | -flags: {scored['neg_flags']}")
        else:
            print(f"     → Already in database (duplicate)")

    # Step 5: Build and send email
    print(f"\n5. Sending email ({len(new_cuts)} new cuts)...")
    recent = get_recent_cuts(conn)

    if new_cuts:
        strong = [c for c in new_cuts if c['signal'] in ('STRONG_BUY', 'BUY')]
        if strong:
            tickers = ', '.join(c['ticker'] for c in strong)
            subject = f"🟢 Dividend Cut ALERT: {tickers} — BUY Signal"
        else:
            tickers = ', '.join(c['ticker'] for c in new_cuts[:3])
            subject = f"📊 Dividend Cut Scanner: {len(new_cuts)} new cuts — {tickers}"
    else:
        subject = f"Dividend Scanner — {len(cuts)} cuts found (all previously detected)"

    html = build_email_html(new_cuts, recent)
    email_sent = send_email(subject, html)

    # Step 6: Log scan
    log_scan(conn, len(calendar), len(new_cuts), 1 if email_sent else 0,
             '; '.join(errors) if errors else '')

    print(f"\n{'='*60}")
    print(f"SCAN COMPLETE: {len(new_cuts)} new cuts scored and stored")
    print(f"{'='*60}")

    conn.close()


def get_recent_cuts(conn):
    """Get recent cuts from database for email history section."""
    c = conn.cursor()
    c.execute("""
        SELECT declaration_date, ticker, cut_pct, entry_price,
               net_score, pos_flags, signal
        FROM dividend_cuts
        WHERE declaration_date >= date('now', '-30 days')
        ORDER BY declaration_date DESC
    """)
    return c.fetchall()


def log_scan(conn, checked, found, sent, errors):
    """Log scan results."""
    c = conn.cursor()
    c.execute("""
        INSERT INTO scan_log (scan_date, declarations_checked, cuts_found, alerts_sent, errors)
        VALUES (?, ?, ?, ?, ?)
    """, (datetime.now().strftime('%Y-%m-%d %H:%M'), checked, found, sent, errors))
    conn.commit()


# ============================================================
# CLI
# ============================================================
def show_status(conn):
    """Show database stats."""
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM dividend_cuts")
    total = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM dividend_cuts WHERE signal IN ('STRONG_BUY', 'BUY')")
    buys = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM sector_cache")
    sectors = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM scan_log")
    scans = c.fetchone()[0]

    c.execute("SELECT * FROM scan_log ORDER BY id DESC LIMIT 5")
    recent_scans = c.fetchall()

    c.execute("""
        SELECT ticker, declaration_date, cut_pct, signal, net_score
        FROM dividend_cuts ORDER BY declaration_date DESC LIMIT 10
    """)
    recent_cuts = c.fetchall()

    print(f"\n{'='*50}")
    print(f"DIVIDEND SCANNER STATUS")
    print(f"{'='*50}")
    print(f"Total cuts tracked:  {total}")
    print(f"Buy signals:         {buys}")
    print(f"Sectors cached:      {sectors}")
    print(f"Total scans:         {scans}")

    if recent_scans:
        print(f"\nLast 5 scans:")
        for s in recent_scans:
            print(f"  {s[1]} | Checked: {s[2]} | Found: {s[3]} | Emailed: {s[4]}")

    if recent_cuts:
        print(f"\nRecent cuts:")
        for r in recent_cuts:
            print(f"  {r[1]} {r[0]:<6} | {r[2]:>5.0f}% cut | {r[3]:<12} | Net: {r[4]:+d}")


def send_test_email():
    """Send a test email to verify configuration."""
    html = f"""
    <html><body style="font-family: Arial; background: #1a1a2e; color: #e0e0e0; padding: 20px;">
        <h1 style="color: #00e676;">Dividend Cut Scanner — Test Email</h1>
        <p>Your email configuration is working correctly.</p>
        <p><strong>Scanner Configuration:</strong></p>
        <ul>
            <li>FMP API Key: {'✓ Set' if config.FMP_API_KEY else '✗ NOT SET'}</li>
            <li>Min Cut Threshold: {config.MIN_CUT_PCT}%</li>
            <li>Lookback Days: {config.LOOKBACK_DAYS}</li>
            <li>Positive Sectors: {', '.join(config.POSITIVE_SECTORS)}</li>
        </ul>
        <p><strong>Composite Scoring System:</strong></p>
        <p>+1 each: Severe cut (75%+), Good sector, Bear market, Q1, Cheap (&lt;=$15)</p>
        <p>-1 each: Moderate cut (&lt;50%), Bad sector, Bull market, Expensive (&gt;$30)</p>
        <p style="color: #666; margin-top: 30px;">
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
        </p>
    </body></html>
    """
    subject = "🧪 Dividend Cut Scanner — Test Email"
    send_email(subject, html)


if __name__ == '__main__':
    if '--test-email' in sys.argv:
        print("Sending test email...")
        send_test_email()

    elif '--status' in sys.argv:
        conn = init_db()
        show_status(conn)
        conn.close()

    elif '--backfill' in sys.argv:
        idx = sys.argv.index('--backfill')
        days = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 30
        print(f"Backfilling last {days} days...")
        run_scan(lookback_days=days)

    else:
        run_scan()
