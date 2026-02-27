"""
Earnings Calendar Collector - Main Orchestrator
Scheduled to run daily via PythonAnywhere
"""

import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone

from config import (
    SMTP_SERVER, SMTP_PORT, EMAIL_ADDRESS, 
    EMAIL_PASSWORD, RECIPIENT_EMAIL, DAYS_AHEAD
)
from database import init_database, upsert_earnings, log_collection, get_database_stats
from collector import fetch_earnings_calendar


def is_weekend():
    """Check if today is Saturday (5) or Sunday (6)."""
    return datetime.now(timezone.utc).weekday() >= 5


def build_email_report(earnings_fetched, new_count, updated_count, stats):
    """Build HTML email report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h2 {{ color: #2c3e50; }}
            .summary {{ background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0; }}
            .stat {{ margin: 5px 0; }}
            .label {{ font-weight: bold; color: #555; }}
        </style>
    </head>
    <body>
        <h2>📅 Earnings Calendar Collection Report</h2>
        <p>Run completed: {now}</p>
        
        <div class="summary">
            <h3>Today's Collection</h3>
            <div class="stat"><span class="label">Days Ahead:</span> {DAYS_AHEAD}</div>
            <div class="stat"><span class="label">Earnings Fetched:</span> {earnings_fetched}</div>
            <div class="stat"><span class="label">New Records:</span> {new_count}</div>
            <div class="stat"><span class="label">Updated Records:</span> {updated_count}</div>
        </div>
        
        <div class="summary">
            <h3>Database Totals</h3>
            <div class="stat"><span class="label">Total Records:</span> {stats['total_records']}</div>
            <div class="stat"><span class="label">Unique Tickers:</span> {stats['unique_tickers']}</div>
            <div class="stat"><span class="label">Date Range:</span> {stats['earliest_date']} to {stats['latest_date']}</div>
        </div>
    </body>
    </html>
    """
    return html


def send_email(subject, html_body):
    """Send HTML email report."""
    if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, RECIPIENT_EMAIL]):
        print("Email credentials not configured. Skipping email.")
        return False
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = RECIPIENT_EMAIL
    
    msg.attach(MIMEText(html_body, 'html'))
    
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, RECIPIENT_EMAIL, msg.as_string())
        print("Email sent successfully.")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False


def main():
    """Main execution function."""
    print("=" * 50)
    print("Earnings Calendar Collector")
    print(f"Run Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)
    
    # Skip weekends
    if is_weekend():
        print("Weekend detected. Skipping collection.")
        return
    
    # Initialize database
    init_database()
    
    try:
        # Fetch earnings
        earnings_list = fetch_earnings_calendar()
        earnings_fetched = len(earnings_list)
        
        if earnings_list:
            # Store in database
            new_count, updated_count = upsert_earnings(earnings_list)
            print(f"Database updated: {new_count} new, {updated_count} updated")
        else:
            new_count, updated_count = 0, 0
            print("No earnings to store.")
        
        # Log successful run
        log_collection(earnings_fetched, new_count, updated_count, "success")
        
        # Get stats for report
        stats = get_database_stats()
        
        # Send email report
        subject = f"📅 Earnings Calendar: {new_count} new, {updated_count} updated"
        html_body = build_email_report(earnings_fetched, new_count, updated_count, stats)
        send_email(subject, html_body)
        
        print("Collection completed successfully.")
        
    except Exception as e:
        error_msg = str(e)
        print(f"ERROR: {error_msg}")
        log_collection(0, 0, 0, "error", error_msg)
        
        # Send error notification
        subject = "❌ Earnings Calendar Collection FAILED"
        html_body = f"<html><body><h2>Collection Error</h2><p>{error_msg}</p></body></html>"
        send_email(subject, html_body)
        
        sys.exit(1)


if __name__ == "__main__":
    main()
