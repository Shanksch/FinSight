"""
fetch_stocks.py
---------------
Fetches daily OHLCV price data for all Nifty 50 stocks from yfinance
and stores it in the Supabase `prices` table.

MODES:
  Run once now:
      python -m scripts.fetch_stocks

  Run as scheduler (fires daily at 6:00 PM IST):
      python -m scripts.fetch_stocks --schedule

  Test scheduler fires in 1 minute:
      python -m scripts.fetch_stocks --test
"""

import sys
import time
import argparse
import pandas as pd
import yfinance as yf

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from backend.core.logger import get_logger
from backend.db.supabase import supabase
from backend.db.utils import get_last_date_for_ticker, get_stock_uuid

logger = get_logger("fetch_stocks")

# ── Nifty 50 tickers ──────────────────────────────────────────────────────────
NIFTY50_TICKERS = [
    "RELIANCE.NS",   "TCS.NS",        "HDFCBANK.NS",  "INFY.NS",
    "ICICIBANK.NS",  "HINDUNILVR.NS", "ITC.NS",        "SBIN.NS",
    "BHARTIARTL.NS", "KOTAKBANK.NS",  "LT.NS",         "AXISBANK.NS",
    "ASIANPAINT.NS", "MARUTI.NS",     "TITAN.NS",      "SUNPHARMA.NS",
    "ULTRACEMCO.NS", "BAJFINANCE.NS", "WIPRO.NS",      "NESTLEIND.NS",
    "POWERGRID.NS",  "NTPC.NS",       "TECHM.NS",      "HCLTECH.NS",
    "ONGC.NS",       "TATAMOTORS.NS", "TATASTEEL.NS",  "BAJAJFINSV.NS",
    "ADANIENT.NS",   "ADANIPORTS.NS", "COALINDIA.NS",  "DIVISLAB.NS",
    "DRREDDY.NS",    "EICHERMOT.NS",  "GRASIM.NS",     "HEROMOTOCO.NS",
    "HINDALCO.NS",   "JSWSTEEL.NS",   "M&M.NS",        "BRITANNIA.NS",
    "CIPLA.NS",      "APOLLOHOSP.NS", "BAJAJ-AUTO.NS", "BPCL.NS",
    "SBILIFE.NS",    "HDFCLIFE.NS",   "INDUSINDBK.NS", "TATACONSUM.NS",
    "UPL.NS",        "VEDL.NS",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def transform_to_records(df: pd.DataFrame, stock_uuid: str) -> list[dict]:
    records = []
    for date, row in df.iterrows():
        records.append({
            "stock_id": stock_uuid,
            "date":     date.strftime("%Y-%m-%d"),
            "open":     round(float(row["Open"]),  2),
            "high":     round(float(row["High"]),  2),
            "low":      round(float(row["Low"]),   2),
            "close":    round(float(row["Close"]), 2),
            "volume":   int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
        })
    return records


def chunked_upsert(records: list[dict], chunk_size: int = 100) -> None:
    total_chunks = (len(records) + chunk_size - 1) // chunk_size
    for i in range(0, len(records), chunk_size):
        chunk_num = (i // chunk_size) + 1
        chunk = records[i : i + chunk_size]
        try:
            supabase.table("prices").upsert(
                chunk, on_conflict="stock_id,date"
            ).execute()
            logger.info(f"Chunk {chunk_num}/{total_chunks} upserted | rows={len(chunk)}")
        except Exception:
            logger.exception(f"Chunk {chunk_num}/{total_chunks} FAILED")
            raise


def fetch_ticker_data(ticker: str) -> int:
    try:
        stock_uuid = get_stock_uuid(ticker)
        if not stock_uuid:
            logger.error(f"UUID not found | ticker={ticker} — run seed_stocks.py first")
            return 0

        last_date = get_last_date_for_ticker(ticker)

        if not last_date:
            logger.info(f"Initial fetch | ticker={ticker}")
            df = yf.download(ticker, period="1y", interval="1d",
                             progress=False, auto_adjust=True)
        else:
            logger.info(f"Incremental fetch | ticker={ticker} | from={last_date}")
            df = yf.download(ticker, start=last_date, interval="1d",
                             progress=False, auto_adjust=True)
            last_ts = pd.Timestamp(last_date)
            df = df[df.index > last_ts]

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if df.empty:
            logger.info(f"No new data | ticker={ticker}")
            return 0

        df = df.dropna(subset=["Close"])
        if df.empty:
            logger.warning(f"All rows NaN after cleaning | ticker={ticker}")
            return 0

        records = transform_to_records(df, stock_uuid)
        chunked_upsert(records)
        logger.info(f"Stored {len(records)} rows | ticker={ticker}")
        return len(records)

    except Exception:
        logger.exception(f"Failed | ticker={ticker}")
        return 0


# ── Main job ──────────────────────────────────────────────────────────────────

def run() -> None:
    """Fetch prices for all 50 tickers. Called by scheduler or directly."""
    logger.info("=" * 60)
    logger.info(f"Fetch job started | tickers={len(NIFTY50_TICKERS)}")
    logger.info("=" * 60)

    total_rows = 0
    failed     = []

    for i, ticker in enumerate(NIFTY50_TICKERS, 1):
        logger.info(f"[{i}/{len(NIFTY50_TICKERS)}] Processing | ticker={ticker}")
        rows = fetch_ticker_data(ticker)
        if rows == 0:
            failed.append(ticker)
        else:
            total_rows += rows
        time.sleep(0.5)

    logger.info("=" * 60)
    logger.info(f"Fetch job complete | total_rows={total_rows}")
    if failed:
        logger.warning(f"No data or errors: {failed}")
    logger.info("=" * 60)


# ── Scheduler listeners ───────────────────────────────────────────────────────

def on_job_executed(event):
    logger.info(f"Scheduler | job finished | job_id={event.job_id}")

def on_job_error(event):
    logger.error(f"Scheduler | job CRASHED | job_id={event.job_id} | error={event.exception}")


# ── Scheduler setup ───────────────────────────────────────────────────────────

def start_scheduler(test_mode: bool = False) -> None:
    """
    test_mode=False  → fires daily at 6:00 PM IST (production)
    test_mode=True   → fires once 1 minute from now (for testing)
    """
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_listener(on_job_executed, EVENT_JOB_EXECUTED)
    scheduler.add_listener(on_job_error,    EVENT_JOB_ERROR)

    if test_mode:
        from datetime import datetime, timedelta
        run_at = datetime.now() + timedelta(minutes=1)
        scheduler.add_job(
            run,
            trigger="date",
            run_date=run_at,
            id="fetch_stocks_test",
        )
        logger.info(f"TEST MODE — fires once at {run_at.strftime('%H:%M:%S')}")
        logger.info("Waiting... (Ctrl+C to cancel)")
    else:
        scheduler.add_job(
            run,
            trigger="cron",
            hour=18,
            minute=0,
            timezone="Asia/Kolkata",
            id="fetch_stocks_daily",
            misfire_grace_time=3600,  # run within 1hr if server was down at 6pm
            coalesce=True,            # if multiple misfires stacked, run only once
        )
        logger.info("Scheduler started — fetch runs daily at 18:00 IST")
        logger.info("Press Ctrl+C to stop")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")
        scheduler.shutdown(wait=False)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nifty 50 price fetcher")
    parser.add_argument("--schedule", action="store_true",
                        help="Run as daily scheduler at 18:00 IST")
    parser.add_argument("--test",     action="store_true",
                        help="Test mode — fires once in 1 minute")
    args = parser.parse_args()

    if args.schedule:
        start_scheduler(test_mode=False)
    elif args.test:
        start_scheduler(test_mode=True)
    else:
        run()