#!/usr/bin/env python3
"""
13F Institutional Holdings Detector
Phase 6: Fixed top filers query + improved ticker lookup

Monitors SEC 13F filings to detect significant institutional position changes.
"""

import requests
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
import time
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

# Configuration
DB_PATH = "13f_holdings.db"
EDGAR_BASE = "https://www.sec.gov"
HEADERS = {
    "User-Agent": "Kevin Heaney KPH3802@gmail.com",  # SEC requires identification
    "Accept-Encoding": "gzip, deflate"
}

# Email configuration
EMAIL_SENDER = "KPH3802@gmail.com"
EMAIL_PASSWORD = "nitn znvs ifwy ekmk"
EMAIL_RECIPIENT = "KPH3802@gmail.com"

REQUEST_DELAY = 0.15

# Change detection thresholds
SIGNIFICANT_CHANGE_PCT = 25
MIN_VALUE_FOR_SIGNAL = 1000  # $1M minimum

# OpenFIGI API for CUSIP -> Ticker mapping
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"


def init_database():
    """Initialize SQLite database with required tables."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS filings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT NOT NULL,
            filer_name TEXT,
            accession_number TEXT UNIQUE NOT NULL,
            filing_date TEXT,
            period_of_report TEXT,
            processed_at TEXT,
            total_value_millions REAL,
            holdings_count INTEGER
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            cusip TEXT,
            issuer_name TEXT,
            ticker TEXT,
            class_title TEXT,
            value_thousands REAL,
            shares INTEGER,
            share_type TEXT,
            investment_discretion TEXT,
            voting_authority_sole INTEGER,
            voting_authority_shared INTEGER,
            voting_authority_none INTEGER,
            FOREIGN KEY (filing_id) REFERENCES filings(id)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            detected_at TEXT,
            signal_type TEXT,
            filer_name TEXT,
            cik TEXT,
            ticker TEXT,
            cusip TEXT,
            issuer_name TEXT,
            details TEXT,
            previous_shares INTEGER,
            current_shares INTEGER,
            change_percent REAL,
            value_thousands REAL
        )
    """)
    
    # Ticker cache table
    c.execute("""
        CREATE TABLE IF NOT EXISTS ticker_cache (
            cusip TEXT PRIMARY KEY,
            ticker TEXT,
            name TEXT,
            looked_up_at TEXT
        )
    """)
    
    c.execute("CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings(cik)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_filings_period ON filings(period_of_report)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_cusip ON holdings(cusip)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_holdings_filing ON holdings(filing_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type)")
    
    conn.commit()
    conn.close()


def lookup_tickers_batch(cusips):
    """
    Look up tickers for a batch of CUSIPs using OpenFIGI API.
    Returns dict of cusip -> ticker.
    """
    if not cusips:
        return {}
    
    # Check cache first
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    results = {}
    uncached = []
    
    for cusip in cusips:
        c.execute("SELECT ticker FROM ticker_cache WHERE cusip = ?", (cusip,))
        row = c.fetchone()
        if row:
            results[cusip] = row[0]
        else:
            uncached.append(cusip)
    
    conn.close()
    
    if not uncached:
        return results
    
    # Query OpenFIGI for uncached CUSIPs (max 100 per request)
    print(f"  Looking up {len(uncached)} tickers via OpenFIGI...")
    
    for i in range(0, len(uncached), 50):  # Reduced batch size for reliability
        batch = uncached[i:i+50]
        
        # Build request - try US equity first
        jobs = [{"idType": "ID_CUSIP", "idValue": cusip, "exchCode": "US"} for cusip in batch]
        
        try:
            time.sleep(0.6)  # Slightly slower rate limit
            response = requests.post(
                OPENFIGI_URL,
                json=jobs,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                now = datetime.now(timezone.utc).isoformat()
                
                for j, item in enumerate(data):
                    cusip = batch[j]
                    ticker = None
                    name = None
                    
                    if "data" in item and item["data"]:
                        # Prefer common stock over other types
                        for d in item["data"]:
                            if d.get("securityType") == "Common Stock":
                                ticker = d.get("ticker")
                                name = d.get("name")
                                break
                        # Fallback to first result
                        if not ticker:
                            ticker = item["data"][0].get("ticker")
                            name = item["data"][0].get("name")
                    
                    results[cusip] = ticker
                    
                    # Cache the result (even if None, to avoid re-lookups)
                    c.execute("""
                        INSERT OR REPLACE INTO ticker_cache (cusip, ticker, name, looked_up_at)
                        VALUES (?, ?, ?, ?)
                    """, (cusip, ticker, name, now))
                
                conn.commit()
                conn.close()
            elif response.status_code == 429:
                print(f"  OpenFIGI rate limited, waiting...")
                time.sleep(5)
                
        except Exception as e:
            print(f"  OpenFIGI error: {e}")
    
    return results


def update_holdings_with_tickers(filing_id):
    """Update holdings records with ticker symbols."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get CUSIPs for this filing that don't have tickers
    c.execute("""
        SELECT DISTINCT cusip FROM holdings 
        WHERE filing_id = ? AND (ticker IS NULL OR ticker = '')
    """, (filing_id,))
    
    cusips = [row[0] for row in c.fetchall() if row[0]]
    conn.close()
    
    if not cusips:
        return
    
    # Lookup tickers
    ticker_map = lookup_tickers_batch(cusips)
    
    # Update holdings
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for cusip, ticker in ticker_map.items():
        if ticker:
            c.execute("""
                UPDATE holdings SET ticker = ? 
                WHERE filing_id = ? AND cusip = ?
            """, (ticker, filing_id, cusip))
    
    conn.commit()
    conn.close()


def fetch_recent_13f_index():
    """Fetch recent 13F-HR filings from SEC EDGAR."""
    url = f"{EDGAR_BASE}/cgi-bin/browse-edgar"
    params = {
        "action": "getcurrent",
        "type": "13F-HR",
        "company": "",
        "dateb": "",
        "owner": "include",
        "count": 100,
        "output": "atom"
    }
    
    print("Fetching recent 13F filings index...")
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        filings = []
        root = ET.fromstring(response.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        
        for entry in root.findall("atom:entry", ns):
            title_elem = entry.find("atom:title", ns)
            link_elem = entry.find("atom:link", ns)
            updated_elem = entry.find("atom:updated", ns)
            
            if title_elem is None or link_elem is None:
                continue
            
            title = title_elem.text or ""
            href = link_elem.get("href", "")
            updated = updated_elem.text if updated_elem is not None else ""
            
            match = re.match(r"13F-HR[A-Z/]*\s*-\s*(.+?)\s*\((\d+)\)", title)
            if match:
                filer_name = match.group(1).strip()
                cik = match.group(2).lstrip("0")
                
                acc_match = re.search(r"/(\d{10}-\d{2}-\d+)", href)
                if acc_match:
                    accession = acc_match.group(1)
                    filings.append({
                        "cik": cik,
                        "accession": accession,
                        "filer_name": filer_name,
                        "filing_date": updated[:10] if updated else ""
                    })
        
        print(f"Found {len(filings)} recent 13F filings")
        return filings
        
    except requests.RequestException as e:
        print(f"Error fetching filings: {e}")
        return []


def fetch_filer_history(cik, limit=4):
    """Fetch historical 13F filings for a specific filer."""
    url = f"{EDGAR_BASE}/cgi-bin/browse-edgar"
    params = {
        "action": "getcompany",
        "CIK": cik,
        "type": "13F-HR",
        "dateb": "",
        "owner": "include",
        "count": limit,
        "output": "atom"
    }
    
    time.sleep(REQUEST_DELAY)
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        filings = []
        root = ET.fromstring(response.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        
        # Get filer name from feed title
        filer_name = ""
        title_elem = root.find("atom:title", ns)
        if title_elem is not None and title_elem.text:
            match = re.search(r"for (.+)", title_elem.text)
            if match:
                filer_name = match.group(1).strip()
        
        for entry in root.findall("atom:entry", ns):
            link_elem = entry.find("atom:link", ns)
            updated_elem = entry.find("atom:updated", ns)
            
            if link_elem is None:
                continue
            
            href = link_elem.get("href", "")
            updated = updated_elem.text if updated_elem is not None else ""
            
            acc_match = re.search(r"/(\d{10}-\d{2}-\d+)", href)
            if acc_match:
                accession = acc_match.group(1)
                filings.append({
                    "cik": cik,
                    "accession": accession,
                    "filer_name": filer_name,
                    "filing_date": updated[:10] if updated else ""
                })
        
        return filings
        
    except requests.RequestException as e:
        print(f"Error fetching history for CIK {cik}: {e}")
        return []


def find_infotable_url(cik, accession):
    """Find the infotable XML URL."""
    acc_nodash = accession.replace("-", "")
    base_dir = f"/Archives/edgar/data/{cik}/{acc_nodash}/"
    index_url = f"{EDGAR_BASE}{base_dir}"
    
    time.sleep(REQUEST_DELAY)
    
    try:
        response = requests.get(index_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html = response.text
        
        patterns = [
            r'href="([^"]*infotable[^"]*\.xml)"',
            r'href="([^"]*13f[^"]*table[^"]*\.xml)"',
            r'href="([^"]*information[^"]*table[^"]*\.xml)"',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                href = matches[0]
                if href.startswith("/"):
                    return f"{EDGAR_BASE}{href}"
                else:
                    return f"{EDGAR_BASE}{base_dir}{href}"
        
        xml_files = re.findall(r'href="([^"]+\.xml)"', html, re.IGNORECASE)
        for xml_file in xml_files:
            if "primary" not in xml_file.lower():
                if xml_file.startswith("/"):
                    return f"{EDGAR_BASE}{xml_file}"
                else:
                    return f"{EDGAR_BASE}{base_dir}{xml_file}"
        
        return None
        
    except requests.RequestException:
        return None


def fetch_and_parse_holdings(cik, accession):
    """Fetch and parse 13F holdings."""
    xml_url = find_infotable_url(cik, accession)
    
    if not xml_url:
        return [], None
    
    time.sleep(REQUEST_DELAY)
    
    try:
        response = requests.get(xml_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        xml_content = response.text
        
        holdings = parse_13f_xml(xml_content)
        period = extract_period_from_xml(xml_content)
        
        return holdings, period
        
    except requests.RequestException:
        return [], None


def parse_13f_xml(xml_content):
    """Parse 13F information table XML."""
    holdings = []
    
    try:
        xml_content = xml_content.strip()
        if xml_content.startswith('\ufeff'):
            xml_content = xml_content[1:]
        
        root = ET.fromstring(xml_content)
        
        for elem in root.iter():
            tag_local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            
            if tag_local.lower() == "infotable":
                holding = parse_info_table_entry(elem)
                if holding and holding.get("cusip"):
                    holdings.append(holding)
                    
    except ET.ParseError:
        pass
    
    return holdings


def parse_info_table_entry(elem):
    """Parse a single infoTable entry."""
    holding = {}
    
    for child in elem.iter():
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        tag_lower = tag.lower()
        text = (child.text or "").strip()
        
        if tag_lower == "nameofissuer":
            holding["issuer_name"] = text
        elif tag_lower == "titleofclass":
            holding["class_title"] = text
        elif tag_lower == "cusip":
            holding["cusip"] = text.upper()
        elif tag_lower == "value":
            try:
                holding["value_thousands"] = float(text)
            except:
                holding["value_thousands"] = 0
        elif tag_lower == "sshprnamt":
            try:
                holding["shares"] = int(text)
            except:
                holding["shares"] = 0
        elif tag_lower == "sshprnamttype":
            holding["share_type"] = text
        elif tag_lower == "investmentdiscretion":
            holding["investment_discretion"] = text
        elif tag_lower == "sole":
            try:
                holding["voting_sole"] = int(text)
            except:
                pass
        elif tag_lower == "shared":
            try:
                holding["voting_shared"] = int(text)
            except:
                pass
    
    return holding


def extract_period_from_xml(xml_content):
    """Extract period of report from XML."""
    patterns = [
        r"<(?:\w+:)?reportCalendarOrQuarter>(\d{2}-\d{2}-\d{4})</",
        r"<(?:\w+:)?periodOfReport>(\d{4}-\d{2}-\d{2})</",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, xml_content)
        if match:
            date_str = match.group(1)
            if "-" in date_str:
                parts = date_str.split("-")
                if len(parts[0]) == 2:
                    return f"{parts[2]}-{parts[0]}-{parts[1]}"
                return date_str
    return None


def store_filing(cik, filer_name, accession, filing_date, period, holdings):
    """Store filing and holdings."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT id FROM filings WHERE accession_number = ?", (accession,))
    if c.fetchone():
        conn.close()
        return None, False
    
    total_value = sum(h.get("value_thousands", 0) for h in holdings) / 1000
    
    c.execute("""
        INSERT INTO filings 
        (cik, filer_name, accession_number, filing_date, period_of_report, 
         processed_at, total_value_millions, holdings_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (cik, filer_name, accession, filing_date, period,
          datetime.now(timezone.utc).isoformat(), total_value, len(holdings)))
    
    filing_id = c.lastrowid
    
    for h in holdings:
        c.execute("""
            INSERT INTO holdings 
            (filing_id, cusip, issuer_name, class_title, value_thousands, 
             shares, share_type, investment_discretion,
             voting_authority_sole, voting_authority_shared, voting_authority_none)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (filing_id, h.get("cusip"), h.get("issuer_name"), h.get("class_title"),
              h.get("value_thousands"), h.get("shares"), h.get("share_type"),
              h.get("investment_discretion"), h.get("voting_sole"),
              h.get("voting_shared"), h.get("voting_none")))
    
    conn.commit()
    conn.close()
    
    return filing_id, True


def get_previous_filing(cik, current_accession):
    """Get the previous filing for comparison."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT filing_date FROM filings WHERE accession_number = ?", (current_accession,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    
    current_date = row[0]
    
    c.execute("""
        SELECT id, accession_number, filing_date, period_of_report
        FROM filings 
        WHERE cik = ? AND filing_date < ?
        ORDER BY filing_date DESC LIMIT 1
    """, (cik, current_date))
    
    result = c.fetchone()
    conn.close()
    
    if result:
        return {"id": result[0], "accession": result[1], "filing_date": result[2], "period": result[3]}
    return None


def get_holdings_for_filing(filing_id):
    """Get holdings dict keyed by CUSIP."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        SELECT cusip, issuer_name, shares, value_thousands, ticker
        FROM holdings WHERE filing_id = ?
    """, (filing_id,))
    
    holdings = {}
    for row in c.fetchall():
        if row[0]:
            holdings[row[0]] = {
                "issuer_name": row[1], 
                "shares": row[2] or 0, 
                "value": row[3] or 0,
                "ticker": row[4]
            }
    
    conn.close()
    return holdings


def detect_changes(cik, filer_name, current_filing_id, previous_filing_id):
    """Detect significant position changes."""
    current = get_holdings_for_filing(current_filing_id)
    previous = get_holdings_for_filing(previous_filing_id)
    
    signals = []
    
    for cusip, data in current.items():
        if cusip not in previous:
            if data["value"] >= MIN_VALUE_FOR_SIGNAL:
                signals.append({
                    "type": "NEW_POSITION", "cusip": cusip, 
                    "issuer_name": data["issuer_name"],
                    "ticker": data.get("ticker"),
                    "current_shares": data["shares"], "previous_shares": 0,
                    "change_pct": None, "value": data["value"]
                })
    
    for cusip, prev_data in previous.items():
        if cusip not in current:
            if prev_data["value"] >= MIN_VALUE_FOR_SIGNAL:
                signals.append({
                    "type": "EXIT", "cusip": cusip, 
                    "issuer_name": prev_data["issuer_name"],
                    "ticker": prev_data.get("ticker"),
                    "current_shares": 0, "previous_shares": prev_data["shares"],
                    "change_pct": -100, "value": prev_data["value"]
                })
        else:
            curr_data = current[cusip]
            prev_shares = prev_data["shares"]
            curr_shares = curr_data["shares"]
            
            if prev_shares > 0:
                change_pct = ((curr_shares - prev_shares) / prev_shares) * 100
                
                if abs(change_pct) >= SIGNIFICANT_CHANGE_PCT:
                    if curr_data["value"] >= MIN_VALUE_FOR_SIGNAL or prev_data["value"] >= MIN_VALUE_FOR_SIGNAL:
                        signal_type = "INCREASED" if change_pct > 0 else "DECREASED"
                        signals.append({
                            "type": signal_type, "cusip": cusip, 
                            "issuer_name": curr_data["issuer_name"],
                            "ticker": curr_data.get("ticker"),
                            "current_shares": curr_shares, "previous_shares": prev_shares,
                            "change_pct": change_pct, "value": curr_data["value"]
                        })
    
    return signals


def store_signals(signals, cik, filer_name):
    """Store signals in database."""
    if not signals:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    
    for s in signals:
        c.execute("""
            INSERT INTO signals 
            (detected_at, signal_type, filer_name, cik, ticker, cusip, issuer_name,
             previous_shares, current_shares, change_percent, value_thousands)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (now, s["type"], filer_name, cik, s.get("ticker"), s["cusip"], s["issuer_name"],
              s["previous_shares"], s["current_shares"], s["change_pct"], s["value"]))
    
    conn.commit()
    conn.close()
    return len(signals)


def backfill_filer(cik, filer_name):
    """Backfill historical filings for a filer."""
    print(f"\n  Backfilling history for {filer_name[:30]}...")
    
    historical = fetch_filer_history(cik, limit=4)
    
    if not historical:
        print(f"    No historical filings found")
        return 0
    
    backfilled = 0
    for f in historical[1:]:  # Skip first (current), get previous
        holdings, period = fetch_and_parse_holdings(f["cik"], f["accession"])
        
        if holdings:
            filing_id, is_new = store_filing(
                f["cik"], filer_name, f["accession"], 
                f["filing_date"], period, holdings
            )
            if is_new:
                print(f"    ✓ Backfilled {f['filing_date']}: {len(holdings)} holdings")
                backfilled += 1
    
    return backfilled


def process_recent_filings(limit=40, do_backfill=False, do_ticker_lookup=True):
    """Process recent 13F filings."""
    print(f"\nProcessing up to {limit} recent filings...")
    print("-" * 50)
    
    filings = fetch_recent_13f_index()
    
    processed = 0
    skipped = 0
    total_signals = 0
    new_filings = []
    all_signals = []
    
    for f in filings[:limit]:
        cik = f["cik"]
        accession = f["accession"]
        filer_name = f["filer_name"]
        filing_date = f["filing_date"]
        
        print(f"\n{filer_name[:40]}...")
        
        holdings, period = fetch_and_parse_holdings(cik, accession)
        
        if not holdings:
            print(f"  No holdings found, skipping")
            skipped += 1
            continue
        
        filing_id, is_new = store_filing(cik, filer_name, accession, filing_date, period, holdings)
        
        if is_new:
            total_val = sum(h.get("value_thousands", 0) for h in holdings) / 1000
            print(f"  ✓ Stored {len(holdings)} holdings, ${total_val:,.1f}M")
            processed += 1
            
            # Ticker lookup
            if do_ticker_lookup:
                update_holdings_with_tickers(filing_id)
            
            new_filings.append({
                "filer_name": filer_name,
                "cik": cik,
                "holdings_count": len(holdings),
                "total_value": total_val
            })
            
            # Backfill if requested and no previous filing
            prev = get_previous_filing(cik, accession)
            if not prev and do_backfill:
                backfill_filer(cik, filer_name)
                prev = get_previous_filing(cik, accession)
            
            if prev:
                signals = detect_changes(cik, filer_name, filing_id, prev["id"])
                if signals:
                    stored = store_signals(signals, cik, filer_name)
                    total_signals += stored
                    print(f"  🔔 {stored} signals detected!")
                    for s in signals:
                        s["filer_name"] = filer_name
                        all_signals.append(s)
        else:
            print(f"  Already in database")
            skipped += 1
    
    print("\n" + "-" * 50)
    print(f"Processed: {processed}, Skipped: {skipped}, Signals: {total_signals}")
    
    return new_filings, all_signals


def get_database_stats():
    """Get database statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM filings")
    filing_count = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM holdings")
    holding_count = c.fetchone()[0]
    
    c.execute("SELECT SUM(total_value_millions) FROM filings")
    total_aum = c.fetchone()[0] or 0
    
    c.execute("SELECT COUNT(*) FROM signals")
    signal_count = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM ticker_cache WHERE ticker IS NOT NULL")
    ticker_count = c.fetchone()[0]
    
    # FIXED: Get most recent filing per filer, grouped by CIK
    c.execute("""
        SELECT f.filer_name, f.total_value_millions, f.holdings_count
        FROM filings f
        INNER JOIN (
            SELECT cik, MAX(filing_date) as max_date
            FROM filings
            GROUP BY cik
        ) latest ON f.cik = latest.cik AND f.filing_date = latest.max_date
        ORDER BY f.total_value_millions DESC
        LIMIT 5
    """)
    top_filers = c.fetchall()
    
    conn.close()
    
    return {
        "filing_count": filing_count,
        "holding_count": holding_count,
        "total_aum": total_aum,
        "signal_count": signal_count,
        "ticker_count": ticker_count,
        "top_filers": top_filers
    }


def generate_email_html(new_filings, signals, stats):
    """Generate HTML email report."""
    now = datetime.now(timezone.utc)
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
            h1 {{ color: #1a5f7a; border-bottom: 2px solid #1a5f7a; padding-bottom: 10px; }}
            h2 {{ color: #2c3e50; margin-top: 30px; }}
            .stats {{ background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0; }}
            .stat-item {{ display: inline-block; margin-right: 30px; }}
            .stat-value {{ font-size: 24px; font-weight: bold; color: #1a5f7a; }}
            .stat-label {{ font-size: 12px; color: #666; }}
            table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
            th {{ background: #1a5f7a; color: white; }}
            tr:nth-child(even) {{ background: #f8f9fa; }}
            .signal-new {{ color: #27ae60; font-weight: bold; }}
            .signal-exit {{ color: #e74c3c; font-weight: bold; }}
            .signal-increased {{ color: #2980b9; }}
            .signal-decreased {{ color: #e67e22; }}
            .ticker {{ font-weight: bold; color: #1a5f7a; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 12px; }}
        </style>
    </head>
    <body>
        <h1>📊 13F Institutional Holdings Report</h1>
        <p>Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}</p>
        
        <div class="stats">
            <div class="stat-item">
                <div class="stat-value">{stats['filing_count']}</div>
                <div class="stat-label">Total Filings</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{stats['holding_count']:,}</div>
                <div class="stat-label">Holdings Tracked</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">${stats['total_aum']/1000000:,.1f}T</div>
                <div class="stat-label">Total AUM</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{stats['signal_count']}</div>
                <div class="stat-label">Total Signals</div>
            </div>
        </div>
    """
    
    # Signals section
    if signals:
        html += f"""
        <h2>🔔 Signals Detected ({len(signals)})</h2>
        <table>
            <tr><th>Type</th><th>Filer</th><th>Ticker</th><th>Security</th><th>Change</th><th>Value</th></tr>
        """
        for s in sorted(signals, key=lambda x: x["value"], reverse=True)[:20]:
            type_class = f"signal-{s['type'].lower().replace('_', '-')}"
            change_str = f"{s['change_pct']:+.1f}%" if s['change_pct'] else "NEW"
            ticker_str = f"<span class='ticker'>{s.get('ticker') or '—'}</span>"
            html += f"""
            <tr>
                <td class="{type_class}">{s['type']}</td>
                <td>{s['filer_name'][:25]}</td>
                <td>{ticker_str}</td>
                <td>{s['issuer_name'][:25]}</td>
                <td>{change_str}</td>
                <td>${s['value']/1000:.1f}M</td>
            </tr>
            """
        html += "</table>"
    else:
        html += "<h2>🔔 Signals</h2><p>No significant position changes detected today.</p>"
    
    # New filings section
    if new_filings:
        html += f"""
        <h2>📁 New Filings Processed ({len(new_filings)})</h2>
        <table>
            <tr><th>Filer</th><th>Holdings</th><th>Total Value</th></tr>
        """
        for f in sorted(new_filings, key=lambda x: x["total_value"], reverse=True)[:15]:
            html += f"""
            <tr>
                <td>{f['filer_name'][:40]}</td>
                <td>{f['holdings_count']}</td>
                <td>${f['total_value']:,.1f}M</td>
            </tr>
            """
        html += "</table>"
    else:
        html += "<h2>📁 New Filings</h2><p>No new filings processed today.</p>"
    
    # Top filers
    if stats["top_filers"]:
        html += """
        <h2>🏆 Top Filers by AUM (Latest Filing)</h2>
        <table>
            <tr><th>Filer</th><th>AUM</th><th>Holdings</th></tr>
        """
        for filer in stats["top_filers"]:
            html += f"""
            <tr>
                <td>{filer[0][:40]}</td>
                <td>${filer[1]:,.1f}M</td>
                <td>{filer[2]}</td>
            </tr>
            """
        html += "</table>"
    
    html += """
        <div class="footer">
            <p>13F Institutional Holdings Detector | Data from SEC EDGAR</p>
        </div>
    </body>
    </html>
    """
    
    return html


def send_email(new_filings, signals, stats):
    """Send email report."""
    print("\nSending email report...")
    
    html_content = generate_email_html(new_filings, signals, stats)
    
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"13F Report: {len(new_filings)} filings, {len(signals)} signals"
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECIPIENT
    
    msg.attach(MIMEText(html_content, "html"))
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
        print("✓ Email sent successfully!")
        return True
    except Exception as e:
        print(f"✗ Email failed: {e}")
        return False


def show_stats():
    """Display database statistics."""
    stats = get_database_stats()
    
    print("\n📊 Database Statistics:")
    print(f"  Filings: {stats['filing_count']}")
    print(f"  Holdings: {stats['holding_count']:,}")
    print(f"  Total AUM: ${stats['total_aum']:,.1f}M")
    print(f"  Signals: {stats['signal_count']}")
    print(f"  Tickers cached: {stats['ticker_count']}")
    
    if stats["top_filers"]:
        print("\n  Top 5 by AUM (Latest Filing):")
        for f in stats["top_filers"]:
            print(f"    {f[0][:35]}: ${f[1]:,.1f}M ({f[2]} holdings)")


def main():
    """Main function."""
    # Skip weekends
    if datetime.now(timezone.utc).weekday() >= 5:
        print("Weekend - skipping run")
        return
    
    print("=" * 60)
    print("13F Institutional Holdings Detector - Phase 6")
    print("=" * 60)
    
    init_database()
    
    # Process with backfill and ticker lookup enabled
    new_filings, signals = process_recent_filings(
        limit=40, 
        do_backfill=True,
        do_ticker_lookup=True
    )
    
    stats = get_database_stats()
    show_stats()
    
    if new_filings or signals:
        send_email(new_filings, signals, stats)
    else:
        print("\nNo new data - skipping email")
    
    print("\n" + "=" * 60)
    print("Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
