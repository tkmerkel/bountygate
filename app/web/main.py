import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.web.watcher_status import compute_status
from app.web.wiki import render_markdown

ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = ROOT / "dashboard"
WIKI_DIR = ROOT / "wiki"

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


@app.get("/api/runs")
def api_runs(limit: int = 50):
    limit = max(1, min(limit, 500))
    if engine is None:
        return {"version": 1, "updated_at": None, "stale_after_minutes": 30, "runs": []}
    with engine.connect() as c:
        rows = c.execute(
            text(
                "SELECT run_id, occurred_at, player, market, outcome, duration_s, issues, "
                "top_finding, video_url, review_url FROM dashboard_runs "
                "ORDER BY occurred_at DESC LIMIT :lim"
            ),
            {"lim": limit},
        ).mappings().all()
    runs = []
    latest = None
    for r in rows:
        d = dict(r)
        d["occurred_at"] = d["occurred_at"].isoformat()
        d["duration_s"] = float(d["duration_s"]) if d["duration_s"] is not None else None
        runs.append(d)
        if latest is None or r["occurred_at"] > latest:
            latest = r["occurred_at"]
    return {
        "version": 1,
        "updated_at": latest.isoformat() if latest else None,
        "stale_after_minutes": 30,
        "runs": runs,
    }


@app.get("/api/account-stats")
def api_account_stats():
    if engine is None:
        return {"version": 1, "updated_at": None, "stale_after_minutes": 120, "books": {}}
    with engine.connect() as c:
        rows = c.execute(
            text(
                "SELECT book, balance, pending_wagers, available_liquidity, pnl_7d, "
                "scrape_status, last_error, scraped_at FROM account_stats"
            )
        ).mappings().all()
    books = {}
    latest = None
    for r in rows:
        d = dict(r)
        d["scraped_at"] = d["scraped_at"].isoformat()
        for k in ("balance", "pending_wagers", "available_liquidity", "pnl_7d"):
            d[k] = float(d[k]) if d[k] is not None else None
        books[r["book"]] = d
        if latest is None or r["scraped_at"] > latest:
            latest = r["scraped_at"]
    return {
        "version": 1,
        "updated_at": latest.isoformat() if latest else None,
        "stale_after_minutes": 120,
        "books": books,
    }


@app.get("/api/watchers")
def api_watchers():
    if engine is None:
        return {
            "version": 1,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "watchers": [],
        }
    with engine.connect() as c:
        rows = c.execute(
            text(
                "SELECT name, is_running, last_tick_at, pending_count, oldest_pending_age_s, "
                "completed_24h, errors_24h, last_error, expected_interval_s "
                "FROM watcher_heartbeats ORDER BY name"
            )
        ).mappings().all()
    watchers = []
    for r in rows:
        d = dict(r)
        d["last_tick_at"] = d["last_tick_at"].isoformat() if d["last_tick_at"] else None
        d["status"] = compute_status(r)
        watchers.append(d)
    return {
        "version": 1,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "watchers": watchers,
    }


# --- Wiki ----------------------------------------------------------------

_FRONT_MATTER_RE = re.compile(r"^---\n(.*?)\n---\n(.*)$", re.DOTALL)


def _parse_front_matter(text_content: str) -> tuple[dict, str]:
    m = _FRONT_MATTER_RE.match(text_content)
    if not m:
        return {}, text_content
    raw, body = m.group(1), m.group(2)
    meta: dict = {}
    for line in raw.splitlines():
        if ":" in line and not line.startswith(" "):
            k, _, v = line.partition(":")
            meta[k.strip()] = v.strip()
    return meta, body


def _list_wiki_pages() -> list[dict]:
    if not WIKI_DIR.exists():
        return []
    pages = []
    for p in sorted(WIKI_DIR.glob("*.md")):
        if p.name.startswith("_"):
            continue
        meta, _ = _parse_front_matter(p.read_text(encoding="utf-8"))
        pages.append({
            "slug": p.stem,
            "title": meta.get("title", p.stem),
            "updated_at": meta.get("updated_at", ""),
        })
    return pages


_WIKI_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>{title} · BountyGate Wiki</title>
<link rel="stylesheet" href="/static/wiki/wiki.css">
</head><body>
<aside class="wiki-sidebar">
  <a class="home-link" href="/">← Dashboard</a>
  <h3>Pages</h3>
  <ul>{sidebar_items}</ul>
</aside>
<main class="wiki-main">
  <header class="wiki-header">
    <h1>{title}</h1>
    <div class="wiki-updated">{updated}</div>
  </header>
  {body}
</main>
{scripts}
</body></html>
"""


def _sidebar_html(active_slug: Optional[str] = None) -> str:
    parts = []
    for p in _list_wiki_pages():
        marker = " style=\"color:#a78bfa;\"" if p["slug"] == active_slug else ""
        parts.append(f'<li><a href="/wiki/{p["slug"]}"{marker}>{p["title"]}</a></li>')
    return "".join(parts) or '<li style="color:#8b95a8;">No pages yet</li>'


@app.get("/wiki", response_class=HTMLResponse)
def wiki_index():
    pages = _list_wiki_pages()
    if not pages:
        body = '<p style="color:#8b95a8;">No wiki pages exist yet. Drop a markdown file into <code>wiki/</code> to get started.</p>'
    else:
        items = "".join(
            f'<li><a href="/wiki/{p["slug"]}">{p["title"]}</a> '
            f'<small style="color:#8b95a8;">{p["updated_at"]}</small></li>'
            for p in pages
        )
        body = f'<p>Internal docs. {len(pages)} page(s):</p><ul>{items}</ul>'
    return _WIKI_TEMPLATE.format(
        title="Wiki",
        sidebar_items=_sidebar_html(),
        updated="",
        body=body,
        scripts="",
    )


@app.get("/wiki/{slug}", response_class=HTMLResponse)
def wiki_page(slug: str):
    if not re.fullmatch(r"[a-z0-9_-]+", slug):
        raise HTTPException(404)
    path = WIKI_DIR / f"{slug}.md"
    if not path.exists():
        raise HTTPException(404)
    meta, body_md = _parse_front_matter(path.read_text(encoding="utf-8"))
    body_html, has_reactflow = render_markdown(body_md)

    scripts = (
        '<script type="module">\n'
        'import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";\n'
        'mermaid.initialize({startOnLoad: true, theme: "dark"});\n'
        '</script>\n'
    )
    if has_reactflow:
        scripts += '<script src="/static/wiki/reactflow-bootstrap.js" defer></script>\n'

    return _WIKI_TEMPLATE.format(
        title=meta.get("title", slug),
        sidebar_items=_sidebar_html(active_slug=slug),
        updated=f'updated {meta.get("updated_at", "—")}',
        body=body_html,
        scripts=scripts,
    )


@app.get("/api/wiki/{slug}.json")
def api_wiki_page(slug: str):
    """Per-page metrics for React Flow islands. Currently only bot-flow."""
    now_iso = datetime.now(timezone.utc).isoformat()
    if slug != "bot-flow" or engine is None:
        return {"slug": slug, "computed_at": now_iso, "node_metrics": {}, "edge_metrics": {}}

    # Aggregate per-outcome metrics over last 24h.
    with engine.connect() as c:
        rows = c.execute(text(
            """
            SELECT outcome, COUNT(*) AS n, AVG(duration_s) AS avg_dur
            FROM dashboard_runs
            WHERE occurred_at > now() - interval '24 hours'
            GROUP BY outcome
            """
        )).mappings().all()
    node_metrics: dict = {}
    for r in rows:
        node_metrics[f"outcome_{r['outcome']}"] = {
            "runs_24h": int(r["n"]),
            "avg_duration_s": float(r["avg_dur"]) if r["avg_dur"] is not None else None,
        }
    return {
        "slug": slug,
        "computed_at": now_iso,
        "node_metrics": node_metrics,
        "edge_metrics": {},
    }


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
