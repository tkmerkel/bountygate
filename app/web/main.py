import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = ROOT / "dashboard"

app = FastAPI(title="BountyGate")

db_url = os.environ.get("DATABASE_URL", "")
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)

engine: Optional[Engine] = None
if db_url:
    engine = create_engine(db_url, pool_pre_ping=True)  # type: ignore[assignment]


@app.get("/health")
def health():
    db_ok = False
    if engine is not None:
        try:
            with engine.connect() as c:
                c.execute(text("select 1"))
            db_ok = True
        except Exception:
            db_ok = False
    return {"status": "ok", "db": db_ok}


@app.get("/")
def root():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
