from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from db.supabase import supabase, supabase_admin

router = APIRouter(prefix="/stocks", tags=["Stocks"])


class StockCreate(BaseModel):
    ticker: str
    name: str
    sector: str | None = None
    market_cap: float | None = None


@router.get("/")
async def get_stocks(
    sector: str | None = Query(None),
    limit: int = Query(50, le=200),
):
    try:
        query = supabase.table("stocks").select("*").limit(limit)
        if sector:
            query = query.eq("sector", sector)
        res = query.execute()
        return {"data": res.data, "count": len(res.data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{stock_id}")
async def get_stock(stock_id: str):
    try:
        res = supabase.table("stocks").select("*").eq("id", stock_id).single().execute()
        return res.data
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{stock_id}/prices")
async def get_prices(stock_id: str, limit: int = Query(30, le=365)):
    try:
        res = (
            supabase.table("prices")
            .select("*")
            .eq("stock_id", stock_id)
            .order("date", desc=True)
            .limit(limit)
            .execute()
        )
        return {"stock_id": stock_id, "data": res.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_stock(body: StockCreate):
    try:
        res = supabase_admin.table("stocks").insert(body.model_dump()).execute()
        return {"message": "Stock created", "data": res.data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))