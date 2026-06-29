from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.config import settings
from routers import auth, stocks, portfolio, verdicts

app = FastAPI(
    title="Finsight",
    description="Virtual trading + AI stock verdicts in English & Hindi",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(stocks.router)
app.include_router(portfolio.router)
app.include_router(verdicts.router)


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok"}


@app.get("/", tags=["Health"])
async def root():
    return {"app": "FinSight", "docs": "/docs"}