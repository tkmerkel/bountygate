"""Arb detection orchestrator: load fresh book + venue rows, run the four pure
pair builders, upsert into ``arb_opportunities``, and Discord-alert NEW edges.

This is the only I/O layer for the ``bountygate.arb`` package. All detection
logic lives in the pure functions (``pairs``/``venue``/``props_matching``); this
module reads the source tables, reduces to latest-per-key in Python, resolves
the Kalshi Yes/No -> team mapping for game markets, calls the builders, and
persists results idempotently (hash PK; ON CONFLICT bumps last_seen_at).

Design notes carried from the Task 11 spec:

  * **Sqlite-compatible SQL.** Freshness cutoffs are computed in PYTHON
    (``datetime.now(utc) - timedelta``) and bound as params; the queries avoid
    Postgres-only constructs so the integration test can run on sqlite.
  * **Latest-per-key in Python.** We pull every row newer than the cutoff and
    keep the last by ``captured_at`` per natural key, rather than relying on a
    DB-side window function (portability).
  * **Kalshi game Yes/No resolution.** The Kalshi parser stores game-market
    outcomes as literal "Yes"/"No", NOT team names. For a Kalshi GAME market we
    parse the ticker (``parse_kalshi_external_id``) to learn which team "Yes"
    refers to: outcome "Yes" -> ticker ``team``; "No" -> ``opponent``. We pass
    the RESOLVED canonical team code as ``outcome_name`` into
    ``pair_book_venue_game`` (venue.py canonicalizes it back and matches the
    event home/away). Polymarket game outcomes are already team-named -> passed
    through as-is.
  * **No double-feeding.** The game resolver only consumes venue rows whose
    Kalshi ticker parses as a GAME market (``parse_kalshi_external_id`` succeeds)
    or whose venue is Polymarket. The prop matcher receives ALL linked venue
    rows with their literal Yes/No outcomes; its own title gate
    (``parse_prop_title``) filters out non-prop titles. A game market's title
    ("Will the Brewers beat the Phillies?") has no stat pattern, so it never
    parses as a prop -> no overlap in practice.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

from bountygate.arb.pairs import pair_game_lines, pair_props
from bountygate.arb.venue import pair_book_venue_game
from bountygate.arb.props_matching import pair_book_venue_props
from bountygate.transforms.matching.event_key import parse_kalshi_external_id
from bountygate.utils.discord_notify import notify

# Freshness windows (minutes), env-tunable.
_GAME_FRESH_MIN = float(os.getenv("BG_ARB_GAME_FRESH_MIN", "20"))
_PROPS_FRESH_MIN = float(os.getenv("BG_ARB_PROPS_FRESH_MIN", "35"))
_VENUE_FRESH_MIN = float(os.getenv("BG_ARB_VENUE_FRESH_MIN", "30"))

_BASE_WAGER = 100.0

# Columns written to arb_opportunities (excludes first_detected_at/last_seen_at,
# which are managed by DEFAULT now() + the ON CONFLICT bump).
_OPP_COLUMNS = [
    "opportunity_hash", "kind", "pairing", "event_id", "sport_key",
    "home_team", "away_team", "commence_time", "market_segment", "player_name",
    "line", "pairing_type", "leg_a_kind", "leg_a_source", "leg_a_outcome",
    "leg_a_point", "leg_a_price", "leg_a_stake", "leg_b_kind", "leg_b_source",
    "leg_b_outcome", "leg_b_point", "leg_b_price", "leg_b_stake", "payout",
    "arb_ev", "roi", "fee_adjusted_roi", "hours_until_commence", "details",
]

# Chunk size for the "which hashes already exist" SELECT ... IN (...) probe.
_HASH_PROBE_CHUNK = 500


def _engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    return create_engine(url)


def _as_dt(value):
    """Coerce a DB value (datetime or ISO string) to an aware-UTC datetime, or None.

    Sqlite returns timestamps as strings; Postgres returns datetimes. The pure
    builders require tz-aware UTC datetimes, so naive values are assumed UTC.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Loaders (latest-per-key reduction in Python)
# ---------------------------------------------------------------------------
def _load_game_book_rows(conn, *, cutoff, now):
    sql = text(
        "SELECT h.event_id, h.market_type, h.bookmaker, h.outcome_name, h.point, "
        "       h.decimal_price, h.captured_at, "
        "       e.sport_key, e.commence_time, e.home_team, e.away_team "
        "FROM sportsbook_odds_history h "
        "JOIN sports_events e ON e.event_id = h.event_id "
        "WHERE h.captured_at >= :cutoff AND e.commence_time > :now"
    )
    rows = conn.execute(sql, {"cutoff": cutoff, "now": now}).mappings().all()
    latest: dict[tuple, dict] = {}
    for r in rows:
        captured = _as_dt(r["captured_at"])
        key = (r["event_id"], r["market_type"], r["bookmaker"],
               r["outcome_name"], r["point"])
        prev = latest.get(key)
        if prev is None or captured > prev["captured_at"]:
            latest[key] = {
                "event_id": r["event_id"],
                "sport_key": r["sport_key"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "commence_time": _as_dt(r["commence_time"]),
                "market_type": r["market_type"],
                "bookmaker": r["bookmaker"],
                "outcome_name": r["outcome_name"],
                "point": _to_float(r["point"]),
                "decimal_price": _to_float(r["decimal_price"]),
                "captured_at": captured,
            }
    return list(latest.values())


def _load_prop_book_rows(conn, *, cutoff, now):
    sql = text(
        "SELECT p.event_id, p.market_key, p.player_name, p.line, p.side, "
        "       p.bookmaker, p.decimal_price, p.captured_at, "
        "       e.sport_key, e.commence_time, e.home_team, e.away_team "
        "FROM player_props_odds_history p "
        "JOIN sports_events e ON e.event_id = p.event_id "
        "WHERE p.captured_at >= :cutoff AND e.commence_time > :now"
    )
    rows = conn.execute(sql, {"cutoff": cutoff, "now": now}).mappings().all()
    latest: dict[tuple, dict] = {}
    for r in rows:
        captured = _as_dt(r["captured_at"])
        key = (r["event_id"], r["market_key"], r["player_name"],
               r["line"], r["side"], r["bookmaker"])
        prev = latest.get(key)
        if prev is None or captured > prev["captured_at"]:
            latest[key] = {
                "event_id": r["event_id"],
                "sport_key": r["sport_key"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "commence_time": _as_dt(r["commence_time"]),
                "market_key": r["market_key"],
                "player_name": r["player_name"],
                "line": _to_float(r["line"]),
                "side": r["side"],
                "bookmaker": r["bookmaker"],
                "decimal_price": _to_float(r["decimal_price"]),
                "captured_at": captured,
            }
    return list(latest.values())


def _load_venue_rows(conn, *, cutoff, now):
    """Latest-ask-per-outcome venue rows linked to a future event.

    Filters out resolved markets (resolved_outcome IS NULL) so settled contracts
    don't produce phantom arbs. Returns the raw joined rows (Yes/No outcomes);
    the caller splits them into game vs prop feeds.
    """
    sql = text(
        "SELECT m.market_id, m.venue_key, m.external_id, m.category, m.title, "
        "       l.event_id, e.sport_key, e.commence_time, e.home_team, e.away_team, "
        "       o.outcome_id, o.outcome_name, ph.ask, ph.captured_at "
        "FROM markets m "
        "JOIN market_event_links l ON l.market_id = m.market_id "
        "JOIN sports_events e ON e.event_id = l.event_id "
        "JOIN market_outcomes o ON o.market_id = m.market_id "
        "JOIN price_history ph ON ph.outcome_id = o.outcome_id "
        "WHERE ph.captured_at >= :cutoff AND e.commence_time > :now "
        "  AND m.resolved_outcome IS NULL"
    )
    rows = conn.execute(sql, {"cutoff": cutoff, "now": now}).mappings().all()
    latest: dict[object, dict] = {}
    for r in rows:
        captured = _as_dt(r["captured_at"])
        oid = r["outcome_id"]
        prev = latest.get(oid)
        if prev is None or captured > prev["captured_at"]:
            latest[oid] = {
                "market_id": r["market_id"],
                "venue_key": r["venue_key"],
                "external_id": r["external_id"],
                "category": r["category"],
                "title": r["title"],
                "event_id": r["event_id"],
                "sport_key": r["sport_key"],
                "commence_time": _as_dt(r["commence_time"]),
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "outcome_id": oid,
                "outcome_name": r["outcome_name"],
                "ask": _to_float(r["ask"]),
                "captured_at": captured,
            }
    return list(latest.values())


# ---------------------------------------------------------------------------
# Venue row splitting (Yes/No -> team resolution for game markets)
# ---------------------------------------------------------------------------
def _build_game_venue_rows(venue_rows: list[dict]) -> list[dict]:
    """Produce the venue_rows feed for pair_book_venue_game.

    Kalshi: resolve outcome Yes->ticker team, No->opponent (canonical codes);
    rows whose ticker doesn't parse as a game market are skipped (the prop
    matcher will handle them via its title gate). Polymarket: outcome_name passes
    through as-is (already team-named).
    """
    out: list[dict] = []
    for v in venue_rows:
        venue_key = v.get("venue_key")
        if venue_key == "kalshi":
            parsed = parse_kalshi_external_id(v.get("external_id"), v.get("category"))
            if parsed is None:
                continue  # not a parseable GAME market -> leave to prop matcher
            outcome = str(v.get("outcome_name") or "").strip().lower()
            if outcome == "yes":
                resolved = parsed["team"]
            elif outcome == "no":
                resolved = parsed["opponent"]
            else:
                continue  # game markets are strictly Yes/No
            row = dict(v)
            row["outcome_name"] = resolved
            out.append(row)
        elif venue_key == "polymarket":
            # Team-named outcomes already; pass through untouched.
            out.append(dict(v))
        # other venues: ignored for game pairing
    return out


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def _existing_hashes(conn, hashes: list[str]) -> set[str]:
    """Return the subset of ``hashes`` already present in arb_opportunities.

    Chunked SELECT ... WHERE opportunity_hash IN (...) — portable to sqlite
    (avoids xmax / RETURNING tricks).
    """
    found: set[str] = set()
    for i in range(0, len(hashes), _HASH_PROBE_CHUNK):
        chunk = hashes[i:i + _HASH_PROBE_CHUNK]
        params = {f"h{j}": h for j, h in enumerate(chunk)}
        placeholders = ", ".join(f":{k}" for k in params)
        rows = conn.execute(
            text("SELECT opportunity_hash FROM arb_opportunities "
                 f"WHERE opportunity_hash IN ({placeholders})"),
            params,
        ).scalars().all()
        found.update(rows)
    return found


def _row_params(opp: dict, now) -> dict:
    """Flatten an opp dict into bind params; json-serialize details, keep None NULL."""
    params = {}
    for col in _OPP_COLUMNS:
        val = opp.get(col)
        if col == "details":
            val = json.dumps(val) if val is not None else None
        params[col] = val
    params["now"] = now
    return params


def _upsert(conn, opps: list[dict], now) -> None:
    cols = ", ".join(_OPP_COLUMNS)
    binds = ", ".join(f":{c}" for c in _OPP_COLUMNS)
    sql = text(
        f"INSERT INTO arb_opportunities ({cols}) VALUES ({binds}) "
        "ON CONFLICT (opportunity_hash) DO UPDATE SET last_seen_at = :now"
    )
    for opp in opps:
        conn.execute(sql, _row_params(opp, now))


# ---------------------------------------------------------------------------
# Discord alerting
# ---------------------------------------------------------------------------
def _format_alert(new_opps: list[dict]) -> str:
    top = sorted(new_opps, key=lambda o: o["fee_adjusted_roi"], reverse=True)[:5]
    lines = [f"New arbitrage opportunities: {len(new_opps)} (top {len(top)} shown)"]
    for i, o in enumerate(top, 1):
        kind = f"{o.get('kind')}/{o.get('pairing')}"
        away = str(o.get("away_team") or "").strip()
        home = str(o.get("home_team") or "").strip()
        matchup = f"{away} @ {home}".strip(" @") or str(o.get("event_id"))
        seg = str(o.get("market_segment") or "")
        player = str(o.get("player_name") or "").strip()
        line = o.get("line")
        seg_bits = seg
        if player:
            seg_bits += f" {player}"
        if line is not None:
            seg_bits += f" {float(line):g}"
        leg_a = f"{o.get('leg_a_source')}@{_to_float(o.get('leg_a_price')):g}"
        leg_b = f"{o.get('leg_b_source')}@{_to_float(o.get('leg_b_price')):g}"
        roi_pct = f"{float(o['fee_adjusted_roi']) * 100:.2f}%"
        lines.append(
            f"{i}) {kind} | {matchup} | {seg_bits} | {leg_a} x {leg_b} | ROI {roi_pct}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_build_arbs(engine=None) -> dict:
    """Detect arbs from fresh book + venue rows and upsert into arb_opportunities.

    ``engine`` may be injected (integration tests pass a sqlite engine);
    otherwise one is created from ``DATABASE_URL``.
    """
    builder_min_roi = float(os.getenv("BUILDER_MIN_ROI", "0.0"))
    alert_min_roi = float(os.getenv("BG_ARB_ALERT_MIN_ROI", "0.01"))

    owns_engine = engine is None
    if engine is None:
        engine = _engine()

    now = datetime.now(timezone.utc)
    game_cutoff = now - timedelta(minutes=_GAME_FRESH_MIN)
    props_cutoff = now - timedelta(minutes=_PROPS_FRESH_MIN)
    venue_cutoff = now - timedelta(minutes=_VENUE_FRESH_MIN)

    try:
        with engine.begin() as conn:
            game_rows = _load_game_book_rows(conn, cutoff=game_cutoff, now=now)
            prop_rows = _load_prop_book_rows(conn, cutoff=props_cutoff, now=now)
            venue_rows = _load_venue_rows(conn, cutoff=venue_cutoff, now=now)

            # Split venue rows: game feed (Yes/No -> team), prop feed (literal Yes/No).
            game_venue_rows = _build_game_venue_rows(venue_rows)
            # Prop matcher consumes ALL linked venue rows; its title gate filters.

            # Run the four pure builders.
            game_bb = pair_game_lines(
                game_rows, base_wager=_BASE_WAGER, min_roi=builder_min_roi)
            prop_bb = pair_props(
                prop_rows, base_wager=_BASE_WAGER, min_roi=builder_min_roi)
            game_bv, venue_stats = pair_book_venue_game(
                game_rows, game_venue_rows,
                base_wager=_BASE_WAGER, min_roi=builder_min_roi)
            prop_bv, props_match_stats = pair_book_venue_props(
                prop_rows, venue_rows,
                base_wager=_BASE_WAGER, min_roi=builder_min_roi)

            all_opps = game_bb + prop_bb + game_bv + prop_bv

            # De-dup across builders on hash (a hash should be unique to one path,
            # but guard against accidental collisions before the upsert).
            by_hash: dict[str, dict] = {}
            for o in all_opps:
                by_hash.setdefault(o["opportunity_hash"], o)
            candidates = list(by_hash.values())
            cand_hashes = [o["opportunity_hash"] for o in candidates]

            existing = _existing_hashes(conn, cand_hashes) if cand_hashes else set()
            new_opps = [o for o in candidates if o["opportunity_hash"] not in existing]

            if candidates:
                _upsert(conn, candidates, now)

            inserted = len(new_opps)
            refreshed = len(candidates) - inserted

            # Discord alert for NEW opps clearing the threshold.
            alert_opps = [o for o in new_opps if o["fee_adjusted_roi"] >= alert_min_roi]
            alerts = len(alert_opps)
            if alert_opps:
                notify(_format_alert(alert_opps), level="warning", source="airflow:build_arbs")

        result = {
            "game_bb": len(game_bb),
            "prop_bb": len(prop_bb),
            "game_bv": len(game_bv),
            "prop_bv": len(prop_bv),
            "candidates": len(candidates),
            "inserted": inserted,
            "refreshed": refreshed,
            "alerts": alerts,
            "venue_stats": venue_stats,
            "props_match_stats": props_match_stats,
        }
        print(
            f"[build_arbs] game_bb={result['game_bb']} prop_bb={result['prop_bb']} "
            f"game_bv={result['game_bv']} prop_bv={result['prop_bv']} "
            f"candidates={result['candidates']} inserted={result['inserted']} "
            f"refreshed={result['refreshed']} alerts={result['alerts']}"
        )
        return result
    finally:
        if owns_engine:
            engine.dispose()
