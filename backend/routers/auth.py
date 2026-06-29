from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from db.supabase import supabase

router = APIRouter(prefix="/auth", tags=["Auth"])


class SignUpRequest(BaseModel):
    email: EmailStr
    password: str
    phone: str | None = None
    language: str = "en"


class SignInRequest(BaseModel):
    email: EmailStr
    password: str


@router.post("/signup")
async def signup(body: SignUpRequest):
    try:
        res = supabase.auth.sign_up({"email": body.email, "password": body.password})
        if res.user is None:
            raise HTTPException(status_code=400, detail="Signup failed")
        return {"user_id": res.user.id, "email": res.user.email, "message": "Check email to confirm."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/signin")
async def signin(body: SignInRequest):
    try:
        res = supabase.auth.sign_in_with_password({"email": body.email, "password": body.password})
        if res.user is None:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return {
            "access_token": res.session.access_token,
            "token_type": "bearer",
            "user_id": res.user.id,
            "email": res.user.email,
        }
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/signout")
async def signout():
    supabase.auth.sign_out()
    return {"message": "Signed out successfully"}