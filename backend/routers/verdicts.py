from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import date
from db.supabase import supabase, supabase_admin

router = APIRouter(prefix="/verdicts", tags=["Verdicts"])


class VerdictCreate(BaseModel):
    stock_id: str
    date: date
    signal: str
    timeframe: str
    health_score: int
    explanation_en: str
    explanation_hi: str


@router.get("/")
async def get_verdicts(
    timeframe: str | None = Query(None),
    signal: str | None = Query(None),
    limit: int = Query(20, le=100),
):
    try:
        query = (
            supabase.table("verdicts")
            .select("*, stocks(ticker, name, sector)")
            .order("date", desc=True)
            .limit(limit)
        )
        if timeframe:
            query = query.eq("timeframe", timeframe)
        if signal:
            query = query.eq("signal", signal)
        res = query.execute()
        return {"data": res.data, "count": len(res.data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{stock_id}")
async def get_stock_verdicts(
    stock_id: str,
    timeframe: str | None = Query(None),
    limit: int = Query(10, le=50),
):
    try:
        query = (
            supabase.table("verdicts").select("*")
            .eq("stock_id", stock_id)
            .order("date", desc=True)
            .limit(limit)
        )
        if timeframe:
            query = query.eq("timeframe", timeframe)
        res = query.execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="No verdicts found")
        return {"stock_id": stock_id, "verdicts": res.data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_verdict(body: VerdictCreate):
    if body.signal not in {"strong_buy", "buy", "hold", "sell", "strong_sell"}:
        raise HTTPException(status_code=400, detail="Invalid signal")
    if body.timeframe not in {"short", "medium", "long"}:
        raise HTTPException(status_code=400, detail="Invalid timeframe")
    if not (0 <= body.health_score <= 100):
        raise HTTPException(status_code=400, detail="health_score must be 0–100")
    try:
        payload = body.model_dump()
        payload["date"] = payload["date"].isoformat()
        res = supabase_admin.table("verdicts").upsert(payload).execute()
        return {"message": "Verdict saved", "data": res.data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))