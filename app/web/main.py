from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.web.routers import arb, cross_market, edges, fair_odds, history, markets, movement, props, scoring

app = FastAPI(title="bountygate read API", description="Read-only prediction-market analytics")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(markets.router)
app.include_router(edges.router)
app.include_router(cross_market.router)
app.include_router(history.router)
app.include_router(fair_odds.router)
app.include_router(scoring.router)
app.include_router(movement.router)
app.include_router(arb.router)
app.include_router(props.router)


@app.get("/")
def index():
    return {"service": "bountygate read API", "docs": "/docs"}
