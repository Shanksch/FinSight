from typing import Optional
from backend.db.supabase import supabase
from backend.core.logger import get_logger

logger = get_logger("db.utils")


def get_stock_uuid(ticker: str) -> Optional[str]:
    """Look up the UUID of a stock by its ticker symbol."""
    try:
        result = (
            supabase.table("stocks")
            .select("id")
            .eq("ticker", ticker)
            .single()
            .execute()
        )
        return result.data["id"]
    except Exception:
        logger.exception(f"Could not find UUID for ticker={ticker}")
        return None


def get_last_date_for_ticker(ticker: str) -> Optional[str]:
    """
    Returns the most recent date stored in the prices table
    for a given ticker. Returns None if no data exists yet.
    """
    try:
        stock_uuid = get_stock_uuid(ticker)
        if not stock_uuid:
            return None

        result = (
            supabase.table("prices")
            .select("date")
            .eq("stock_id", stock_uuid)
            .order("date", desc=True)
            .limit(1)
            .execute()
        )

        if result.data:
            return result.data[0]["date"]   # e.g. "2024-11-15"
        return None

    except Exception:
        logger.exception(f"Could not fetch last date for ticker={ticker}")
        return None