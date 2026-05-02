"""
fetch_stocks.py
---------------
Fetches daily OHLCV price data for all Nifty 50 stocks from yfinance
and stores it in the Supabase `prices` table.

- First run  : fetches 1 full year of historical data
- Daily runs : fetches only new data since the last stored date (incremental)
- Safe       : uses upsert — will never create duplicate rows

Usage:
    python -m scripts.fetch_stocks

Schedule:
    Run daily at 6:30 PM IST after NSE market close (3:30 PM IST)
"""

import time
import pandas as pd
import yfinance as yf

from backend.core.logger import get_logger
from backend.db.supabase import supabase
from backend.db.utils import get_last_date_for_ticker, get_stock_uuid

logger = get_logger("fetch_stocks")

# ── All 50 Nifty 50 tickers ───────────────────────────────────────────────────
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


# ── Transform ─────────────────────────────────────────────────────────────────

def transform_to_records(df: pd.DataFrame, stock_uuid: str) -> list[dict]:
    """
    Convert a yfinance OHLCV DataFrame into a list of dicts
    ready for Supabase upsert.

    Fixes applied:
    - Uses stock_uuid (not ticker string) for stock_id — matches FK in DB
    - Handles NaN volume (trading halts) → stored as 0
    - Rounds float prices to 2 decimal places
    """
    records = []
    for date, row in df.iterrows():
        records.append({
            "stock_id": stock_uuid,                                          # FIX 1: UUID not ticker string
            "date":     date.strftime("%Y-%m-%d"),
            "open":     round(float(row["Open"]),  2),
            "high":     round(float(row["High"]),  2),
            "low":      round(float(row["Low"]),   2),
            "close":    round(float(row["Close"]), 2),
            "volume":   int(row["Volume"]) if pd.notna(row["Volume"]) else 0, # FIX 2: NaN volume crash
        })
    return records


# ── Supabase upsert in safe chunks ────────────────────────────────────────────

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
            logger.exception(f"Chunk {chunk_num}/{total_chunks} FAILED | rows={len(chunk)}")
            raise   # re-raise so fetch_ticker_data knows it failed


# ── Core fetch logic per ticker ───────────────────────────────────────────────

def fetch_ticker_data(ticker: str) -> int:
    """
    Fetch and store price data for a single ticker.
    Returns the number of rows stored (0 if nothing new or on error).
    """
    try:
        # Step 1: resolve stock UUID — needed for prices.stock_id FK
        stock_uuid = get_stock_uuid(ticker)
        if not stock_uuid:
            logger.error(
                f"Ticker not found in stocks table | ticker={ticker} "
                f"— run seed_stocks.py first"
            )
            return 0

        # Step 2: decide fetch range
        last_date = get_last_date_for_ticker(ticker)

        if not last_date:
            # First run — no data in DB yet, fetch full year
            logger.info(f"Initial fetch | ticker={ticker}")
            df = yf.download(
                ticker,
                period="1y",
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
        else:
            # Incremental — only fetch after last stored date
            logger.info(f"Incremental fetch | ticker={ticker} | from={last_date}")
            df = yf.download(
                ticker,
                start=last_date,
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
            # FIX 3: normalize last_date to Timestamp before comparison
            # last_date from DB is a string like "2024-11-15" — comparing
            # a string to DatetimeIndex causes silent wrong results or crashes
            last_ts = pd.Timestamp(last_date)
            df = df[df.index > last_ts]

        # Step 3: flatten MultiIndex columns
        # FIX 4: yfinance 0.2+ returns MultiIndex like ('Open', 'RELIANCE.NS')
        # row["Open"] would KeyError without this flattening step
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Step 4: skip if nothing new
        if df.empty:
            logger.info(f"No new data | ticker={ticker}")
            return 0

        # Step 5: drop rows where Close is NaN (bad data days)
        df = df.dropna(subset=["Close"])
        if df.empty:
            logger.warning(f"All rows NaN after cleaning | ticker={ticker}")
            return 0

        # Step 6: transform and store in chunks
        records = transform_to_records(df, stock_uuid)
        chunked_upsert(records)                                              # FIX 5: chunked not single upsert
        logger.info(f"Stored {len(records)} rows | ticker={ticker}")
        return len(records)

    except Exception:
        logger.exception(f"Failed to fetch data | ticker={ticker}")
        return 0


# ── Main runner ───────────────────────────────────────────────────────────────

def run() -> None:
    logger.info(f"=== Fetch started | total_tickers={len(NIFTY50_TICKERS)} ===")

    total_rows = 0
    failed     = []

    for i, ticker in enumerate(NIFTY50_TICKERS, 1):
        logger.info(f"[{i}/{len(NIFTY50_TICKERS)}] Processing | ticker={ticker}")
        rows = fetch_ticker_data(ticker)

        if rows == 0:
            failed.append(ticker)
        else:
            total_rows += rows

        time.sleep(0.5)  # avoid yfinance rate limiting

    logger.info(f"=== Fetch complete | total_rows={total_rows} ===")
    if failed:
        logger.warning(f"No new data or errors: {failed}")


if __name__ == "__main__":
    run()