from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from db.supabase import supabase

router = APIRouter(prefix="/portfolio", tags=["Portfolio"])


class TradeRequest(BaseModel):
    stock_id: str
    trade_type: str        # "buy" | "sell"
    quantity: float
    price_at_trade: float


async def get_user_id(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    try:
        return supabase.auth.get_user(authorization.split(" ")[1]).user.id
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@router.get("/")
async def get_portfolio(authorization: str | None = Header(None)):
    user_id = await get_user_id(authorization)
    try:
        res = (
            supabase.table("portfolio")
            .select("*, stocks(ticker, name, sector)")
            .eq("user_id", user_id)
            .execute()
        )
        return {"user_id": user_id, "holdings": res.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trades")
async def get_trades(authorization: str | None = Header(None)):
    user_id = await get_user_id(authorization)
    try:
        res = (
            supabase.table("trades")
            .select("*, stocks(ticker, name)")
            .eq("user_id", user_id)
            .order("trade_date", desc=True)
            .execute()
        )
        return {"user_id": user_id, "trades": res.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trade")
async def execute_trade(body: TradeRequest, authorization: str | None = Header(None)):
    user_id = await get_user_id(authorization)
    if body.trade_type not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="trade_type must be 'buy' or 'sell'")
    try:
        supabase.table("trades").insert({
            "user_id": user_id,
            "stock_id": body.stock_id,
            "trade_type": body.trade_type,
            "quantity": body.quantity,
            "price_at_trade": body.price_at_trade,
        }).execute()

        total_cost = body.quantity * body.price_at_trade
        existing = (
            supabase.table("portfolio")
            .select("*")
            .eq("user_id", user_id)
            .eq("stock_id", body.stock_id)
            .execute()
        )

        if existing.data:
            row = existing.data[0]
            if body.trade_type == "buy":
                new_qty = row["quantity_held"] + body.quantity
                new_avg = ((row["avg_buy_price"] * row["quantity_held"]) + total_cost) / new_qty
            else:
                new_qty = row["quantity_held"] - body.quantity
                new_avg = row["avg_buy_price"]
                if new_qty < 0:
                    raise HTTPException(status_code=400, detail="Insufficient holdings")
            supabase.table("portfolio").update({
                "quantity_held": new_qty,
                "avg_buy_price": new_avg,
                "current_value": new_qty * body.price_at_trade,
            }).eq("user_id", user_id).eq("stock_id", body.stock_id).execute()
        else:
            if body.trade_type == "sell":
                raise HTTPException(status_code=400, detail="No holdings to sell")
            supabase.table("portfolio").insert({
                "user_id": user_id, "stock_id": body.stock_id,
                "quantity_held": body.quantity, "avg_buy_price": body.price_at_trade,
                "current_value": total_cost, "pnl_percent": 0,
            }).execute()

        return {"message": f"{body.trade_type.capitalize()} executed successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))