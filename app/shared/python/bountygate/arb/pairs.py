"""Pure book x book arbitrage pair builders for game lines and player props.

Faithful ports of the archive pandas pipelines:
  - game lines: airflow/dags/_archive_pre_pivot/bg_game_arb_pipeline_lib/builder.py
  - props:      airflow/dags/_archive_pre_pivot/bg_arb_pipeline_lib/builder.py

House style: pure functions over lists of dicts, no pandas. Output rows match
the `arb_opportunities` table (db/migrations/018_arb_opportunities.sql).

A 2-way arb pairs two legs from DIFFERENT books where:
  - h2h:     opposing outcomes, no point on either side
  - spreads: opposing outcomes, leg_a.point == -leg_b.point
  - totals:  over/under at the same point
  - props:   an under leg x an over leg at the same (event, player, canonical
             market, line) from two different books

Standard arb math (overround formulation, identical across game + props):

    implied_a = 1 / price_a
    implied_b = 1 / price_b
    overround = implied_a + implied_b
    stake_a   = base_wager / overround * implied_a
    stake_b   = base_wager / overround * implied_b
    payout    = base_wager / overround        # guaranteed return on either leg
    arb_ev    = payout - base_wager
    roi       = arb_ev / base_wager

An arb exists iff overround < 1 (equivalently roi > 0).

Two DELIBERATE deviations from the archive (both documented here):

  1. **Overround stake normalization for props.** The archive props builder used
     a different stake split (wager_under = base, payout = base * under_price,
     wager_over = payout / over_price, roi = (payout - total) / total). That is
     the SAME arb condition and the SAME roi sign / zero-crossing, but the roi
     magnitude differs. We use the overround formulation here so props and game
     lines share one `_economics` helper and report ROI on the same basis.

  2. **Dict rows instead of pandas DataFrames.** Pure-python list-of-dict
     processing, no pandas dependency.

Prop hash & schema note: the unified `arb_opportunities` schema has no per-leg
market_key column, so std/alt-ness of each prop leg is carried two ways. For the
TABLE row, leg outcomes stay the plain strings 'under'/'over' and `pairing_type`
records the std/alt classification; the raw under/over market keys go into
`details`. For the HASH, however, we must preserve the archive's identity
precision (which keyed on BOTH market keys), so we feed the hash legs whose
outcome encodes side+market-key as ``f"under:{under_market_key}"`` /
``f"over:{over_market_key}"``. This keeps std_alt distinct from alt_std in the
PK while leaving the stored row human-readable.
"""
from __future__ import annotations

from typing import Optional

from bountygate.arb.hashing import derive_pairing_type, opportunity_hash
from bountygate.arb.markets import ARB_MARKET_BLACKLIST

_ALT_SUFFIX = "_alternate"


def _strip_alt_suffix(market_key: str) -> str:
    if market_key.endswith(_ALT_SUFFIX):
        return market_key[: -len(_ALT_SUFFIX)]
    return market_key


def _economics(
    price_a: float, price_b: float, base_wager: float
) -> tuple[float, float, float, float, float]:
    """Overround arb economics shared by game lines and props.

    Returns (stake_a, stake_b, payout, arb_ev, roi).
    """
    implied_a = 1.0 / price_a
    implied_b = 1.0 / price_b
    overround = implied_a + implied_b
    payout = base_wager / overround
    stake_a = payout * implied_a
    stake_b = payout * implied_b
    arb_ev = payout - base_wager
    roi = arb_ev / base_wager
    return stake_a, stake_b, payout, arb_ev, roi


def _hours_until_commence(commence_time, captured_at_a, captured_at_b) -> float:
    latest_capture = max(captured_at_a, captured_at_b)
    return (commence_time - latest_capture).total_seconds() / 3600.0


# ---------------------------------------------------------------------------
# Game lines
# ---------------------------------------------------------------------------
def pair_game_lines(
    rows: list[dict], *, base_wager: float = 100.0, min_roi: float = 0.0
) -> list[dict]:
    """Build h2h/spreads/totals book x book arb pairs from game-line rows.

    Input row shape::

        {event_id, sport_key, home_team, away_team, commence_time (tz-aware),
         market_type ('h2h'|'spreads'|'totals'), bookmaker, outcome_name,
         point (float|None), decimal_price (float|None), captured_at (tz-aware)}

    Faithful port of bg_game_arb_pipeline_lib/builder.py. See module docstring
    for the economics. ``min_roi`` defaults to 0.0 (archive kept roi > 0).
    """
    if not rows:
        return []

    # Drop rows with missing/non-positive price.
    clean = [
        r
        for r in rows
        if r.get("decimal_price") is not None and r["decimal_price"] > 0
    ]
    if not clean:
        return []

    by_market: dict[str, list[dict]] = {"h2h": [], "spreads": [], "totals": []}
    for r in clean:
        mt = r["market_type"]
        if mt in by_market:
            by_market[mt].append(r)

    out: list[dict] = []
    seen_hashes: set[str] = set()

    for market_type, market_rows in by_market.items():
        if not market_rows:
            continue
        if market_type in ("spreads", "totals"):
            market_rows = [r for r in market_rows if r.get("point") is not None]
            if not market_rows:
                continue

        # Group by event so we only pair within a game.
        by_event: dict[object, list[dict]] = {}
        for r in market_rows:
            by_event.setdefault(r["event_id"], []).append(r)

        for event_rows in by_event.values():
            n = len(event_rows)
            for i in range(n):
                for j in range(i + 1, n):
                    a, b = event_rows[i], event_rows[j]
                    pair = _try_pair_game(a, b, market_type)
                    if pair is None:
                        continue
                    leg_a, leg_b = pair  # symmetric-dedupe-ordered (book_a < book_b)

                    opp = _build_game_opp(
                        leg_a,
                        leg_b,
                        market_type,
                        base_wager=base_wager,
                        min_roi=min_roi,
                    )
                    if opp is None:
                        continue
                    h = opp["opportunity_hash"]
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)
                    out.append(opp)

    return out


def _try_pair_game(a: dict, b: dict, market_type: str) -> Optional[tuple[dict, dict]]:
    """Apply the per-market pairing rules + symmetric dedupe.

    Returns the legs ordered so bookmaker_a < bookmaker_b, or None if the two
    rows don't form a valid pair.
    """
    if a["bookmaker"] == b["bookmaker"]:
        return None

    if market_type == "h2h":
        if a["outcome_name"] == b["outcome_name"]:
            return None
    elif market_type == "spreads":
        if a["outcome_name"] == b["outcome_name"]:
            return None
        if a["point"] != -b["point"]:
            return None
    elif market_type == "totals":
        oa = a["outcome_name"].lower()
        ob = b["outcome_name"].lower()
        if oa == ob:
            return None
        if a["point"] != b["point"]:
            return None
        # Defense: real totals outcomes are over/under.
        if oa not in ("over", "under") or ob not in ("over", "under"):
            return None
    else:
        return None

    # Symmetric dedupe: keep only bookmaker_a < bookmaker_b.
    if a["bookmaker"] < b["bookmaker"]:
        return a, b
    return b, a


def _build_game_opp(
    leg_a: dict,
    leg_b: dict,
    market_type: str,
    *,
    base_wager: float,
    min_roi: float,
) -> Optional[dict]:
    stake_a, stake_b, payout, arb_ev, roi = _economics(
        leg_a["decimal_price"], leg_b["decimal_price"], base_wager
    )
    if not roi > min_roi:
        return None

    hours = _hours_until_commence(
        leg_a["commence_time"], leg_a["captured_at"], leg_b["captured_at"]
    )
    if not hours > 0:
        return None

    point_a = leg_a.get("point")
    point_b = leg_b.get("point")
    # Totals: line == the shared point. Spreads/h2h: line None (leg points carry it).
    line = point_a if market_type == "totals" else None

    h = opportunity_hash(
        kind="game",
        pairing="book_book",
        event_id=leg_a["event_id"],
        market_segment=market_type,
        legs=[
            (leg_a["bookmaker"], leg_a["outcome_name"], point_a, leg_a["decimal_price"]),
            (leg_b["bookmaker"], leg_b["outcome_name"], point_b, leg_b["decimal_price"]),
        ],
        player_name=None,
        line=line,
    )

    return {
        "opportunity_hash": h,
        "kind": "game",
        "pairing": "book_book",
        "event_id": leg_a["event_id"],
        "sport_key": leg_a["sport_key"],
        "home_team": leg_a["home_team"],
        "away_team": leg_a["away_team"],
        "commence_time": leg_a["commence_time"],
        "market_segment": market_type,
        "player_name": None,
        "line": line,
        "pairing_type": None,
        "leg_a_kind": "book",
        "leg_a_source": leg_a["bookmaker"],
        "leg_a_outcome": leg_a["outcome_name"],
        "leg_a_point": point_a,
        "leg_a_price": leg_a["decimal_price"],
        "leg_a_stake": stake_a,
        "leg_b_kind": "book",
        "leg_b_source": leg_b["bookmaker"],
        "leg_b_outcome": leg_b["outcome_name"],
        "leg_b_point": point_b,
        "leg_b_price": leg_b["decimal_price"],
        "leg_b_stake": stake_b,
        "payout": payout,
        "arb_ev": arb_ev,
        "roi": roi,
        "fee_adjusted_roi": roi,
        "hours_until_commence": hours,
        "details": None,
    }


# ---------------------------------------------------------------------------
# Player props
# ---------------------------------------------------------------------------
def pair_props(
    rows: list[dict], *, base_wager: float = 100.0, min_roi: float = 0.0
) -> list[dict]:
    """Build book x book under x over prop arb pairs.

    Input row shape::

        {event_id, sport_key, home_team, away_team, commence_time (tz-aware),
         market_key, player_name, line (float), side ('over'|'under'),
         bookmaker, decimal_price (float|None), captured_at (tz-aware)}

    Faithful port of bg_arb_pipeline_lib/builder.py. See module docstring for
    the economics and the two deliberate deviations.
    """
    if not rows:
        return []

    # Drop rows with missing/non-positive price or missing line, and blacklisted markets.
    clean: list[dict] = []
    for r in rows:
        if r.get("decimal_price") is None or r["decimal_price"] <= 0:
            continue
        if r.get("line") is None:
            continue
        if r["market_key"] in ARB_MARKET_BLACKLIST:
            continue
        clean.append(r)
    if not clean:
        return []

    # Split into unders and overs keyed on (event, player, canonical_market, line).
    unders: dict[tuple, list[dict]] = {}
    overs: dict[tuple, list[dict]] = {}
    for r in clean:
        canonical = _strip_alt_suffix(r["market_key"])
        key = (r["event_id"], r["player_name"], canonical, r["line"])
        if r["side"] == "under":
            unders.setdefault(key, []).append(r)
        elif r["side"] == "over":
            overs.setdefault(key, []).append(r)

    out: list[dict] = []
    seen_hashes: set[str] = set()

    for key, under_rows in unders.items():
        over_rows = overs.get(key)
        if not over_rows:
            continue
        event_id, player_name, canonical, line = key
        for u in under_rows:
            for o in over_rows:
                if u["bookmaker"] == o["bookmaker"]:
                    continue
                opp = _build_prop_opp(
                    u,
                    o,
                    event_id=event_id,
                    player_name=player_name,
                    canonical=canonical,
                    line=line,
                    base_wager=base_wager,
                    min_roi=min_roi,
                )
                if opp is None:
                    continue
                h = opp["opportunity_hash"]
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)
                out.append(opp)

    return out


def _build_prop_opp(
    under: dict,
    over: dict,
    *,
    event_id,
    player_name,
    canonical: str,
    line: float,
    base_wager: float,
    min_roi: float,
) -> Optional[dict]:
    stake_u, stake_o, payout, arb_ev, roi = _economics(
        under["decimal_price"], over["decimal_price"], base_wager
    )
    if not roi > min_roi:
        return None

    hours = _hours_until_commence(
        under["commence_time"], under["captured_at"], over["captured_at"]
    )
    if not hours > 0:
        return None

    under_market_key = under["market_key"]
    over_market_key = over["market_key"]
    pairing_type = derive_pairing_type(under_market_key, over_market_key)

    # Hash legs encode side + raw market key so std_alt stays distinct from
    # alt_std (archive identity precision). leg_a == under, leg_b == over by
    # construction, so input order is irrelevant.
    h = opportunity_hash(
        kind="prop",
        pairing="book_book",
        event_id=event_id,
        market_segment=canonical,
        legs=[
            (under["bookmaker"], f"under:{under_market_key}", line, under["decimal_price"]),
            (over["bookmaker"], f"over:{over_market_key}", line, over["decimal_price"]),
        ],
        player_name=player_name,
        line=line,
    )

    return {
        "opportunity_hash": h,
        "kind": "prop",
        "pairing": "book_book",
        "event_id": event_id,
        "sport_key": under["sport_key"],
        "home_team": under["home_team"],
        "away_team": under["away_team"],
        "commence_time": under["commence_time"],
        "market_segment": canonical,
        "player_name": player_name,
        "line": line,
        "pairing_type": pairing_type,
        "leg_a_kind": "book",
        "leg_a_source": under["bookmaker"],
        "leg_a_outcome": "under",
        "leg_a_point": line,
        "leg_a_price": under["decimal_price"],
        "leg_a_stake": stake_u,
        "leg_b_kind": "book",
        "leg_b_source": over["bookmaker"],
        "leg_b_outcome": "over",
        "leg_b_point": line,
        "leg_b_price": over["decimal_price"],
        "leg_b_stake": stake_o,
        "payout": payout,
        "arb_ev": arb_ev,
        "roi": roi,
        "fee_adjusted_roi": roi,
        "hours_until_commence": hours,
        "details": {
            "under_market_key": under_market_key,
            "over_market_key": over_market_key,
        },
    }
