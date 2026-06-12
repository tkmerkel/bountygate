"""Book x prediction-market (book x venue) game-line arbitrage detection.

A book-x-venue game arb pairs a sportsbook h2h price on team X with a
prediction-market contract (Kalshi / Polymarket) whose YES outcome resolves
on the OTHER team Y. The book leg covers X winning; the venue leg covers Y
winning — between them exactly one resolves, so the pair is a 2-way arb iff the
combined cost of $1 of payout on each side is < $1.

Economics (`_book_venue_economics`, shared with props_matching.py):

    cost_book  = 1 / book_decimal            # $ to lock $1 book payout
    cost_venue = venue_effective_cost(ask)   # ask + Kalshi taker fee (Poly: ask)
    T          = cost_book + cost_venue
    payout     = base_wager / T              # guaranteed return on either leg
    stake_book = base_wager * cost_book / T
    stake_venue= base_wager * cost_venue / T # NOTE: includes the venue fee
    arb_ev     = payout - base_wager

The reported ``roi`` is the PRE-fee roi and ``fee_adjusted_roi`` is the
fee-inclusive roi, both straight from ``book_venue_roi`` (fees.py). The
min_roi filter is applied to the fee-adjusted roi (the real edge).

Venue ask is a probability in dollars (0..1); rows with ask None / <=0 / >=1
are dropped. Freshness (staleness) filtering is the orchestrator's job — we
only require hours_until_commence > 0 (max of the two leg captured_ats).

Venue outcome -> team mapping (precision-first; ambiguous/unmapped is counted,
never guessed): we canonicalize the venue outcome_name and the event home/away
via ``canonical_team`` for sports with an alias file (NFL/NBA/MLB). Sports with
no alias file (e.g. NHL) fall back to case-insensitive containment between the
outcome_name and the home/away strings. A row resolving to neither team, or
ambiguously to both, is skipped and counted.
"""
from __future__ import annotations

from typing import Optional

from bountygate.arb.fees import book_venue_roi, venue_effective_cost
from bountygate.arb.hashing import opportunity_hash
from bountygate.transforms.matching.aliases import canonical_team

# sport_key (Odds API) -> alias-file sport tag (aliases.py uses NFL/NBA/MLB).
_SPORT_KEY_TO_ALIAS = {
    "americanfootball_nfl": "NFL",
    "basketball_nba": "NBA",
    "baseball_mlb": "MLB",
}


def _book_venue_economics(
    book_decimal: float, venue_ask: float, venue_key: str, base_wager: float
) -> tuple[float, float, float, float, float, float]:
    """Shared book-x-venue stake math.

    Returns (stake_book, stake_venue, payout, arb_ev, roi_pre, roi_fee).
    stake_venue INCLUDES the venue fee (cost to acquire the contract + fee).
    """
    roi_pre, roi_fee = book_venue_roi(book_decimal, venue_ask, venue_key)
    cost_book = 1.0 / book_decimal
    cost_venue = venue_effective_cost(venue_ask, venue_key)
    t = cost_book + cost_venue
    payout = base_wager / t
    stake_book = base_wager * cost_book / t
    stake_venue = base_wager * cost_venue / t
    arb_ev = payout - base_wager
    return stake_book, stake_venue, payout, arb_ev, roi_pre, roi_fee


def _iso(dt) -> Optional[str]:
    if dt is None:
        return None
    try:
        return dt.isoformat()
    except AttributeError:
        return str(dt)


def _resolve_team(outcome_name, home_team, away_team, sport_key) -> Optional[str]:
    """Map a venue outcome to 'home'/'away' for the event, or None.

    None means "did not resolve to exactly one team" (neither, or ambiguously
    both). Uses alias canonicalization where available; otherwise a
    case-insensitive containment fallback.
    """
    if not outcome_name:
        return None
    alias_sport = _SPORT_KEY_TO_ALIAS.get(sport_key)
    if alias_sport is not None:
        c_out = canonical_team(outcome_name, alias_sport)
        c_home = canonical_team(home_team, alias_sport)
        c_away = canonical_team(away_team, alias_sport)
        if c_out is None:
            return None
        is_home = c_out == c_home
        is_away = c_out == c_away
    else:
        # Fallback: case-insensitive containment either direction.
        o = str(outcome_name).strip().lower()
        h = str(home_team or "").strip().lower()
        a = str(away_team or "").strip().lower()
        is_home = bool(o) and bool(h) and (o in h or h in o)
        is_away = bool(o) and bool(a) and (o in a or a in o)
    if is_home and is_away:
        return None  # ambiguous
    if is_home:
        return "home"
    if is_away:
        return "away"
    return None


def pair_book_venue_game(
    book_rows: list[dict],
    venue_rows: list[dict],
    *,
    base_wager: float = 100.0,
    min_roi: float = 0.0,
) -> tuple[list[dict], dict]:
    """Pair sportsbook h2h prices with prediction-market contracts on the other team.

    ``book_rows``: same game-row shape as pairs.pair_game_lines (we filter
    ``market_type == 'h2h'`` defensively)::

        {event_id, sport_key, home_team, away_team, commence_time (tz-aware),
         market_type, bookmaker, outcome_name, point, decimal_price,
         captured_at (tz-aware)}

    ``venue_rows`` (one row per venue outcome with its latest ask)::

        {market_id, venue_key, event_id, sport_key, home_team, away_team,
         commence_time (tz-aware), outcome_name, ask (float|None),
         captured_at (tz-aware), title}

    Returns ``(opps, stats)`` where stats keys are: paired, no_team_match,
    ambiguous, stale_skipped (always 0 — freshness is the orchestrator's job),
    missing_ask.
    """
    stats = {
        "paired": 0,
        "no_team_match": 0,
        "ambiguous": 0,
        "stale_skipped": 0,
        "missing_ask": 0,
    }

    # Index book h2h rows by (event_id, resolved team side).
    books = [r for r in book_rows if r.get("market_type") == "h2h"]
    books = [
        r
        for r in books
        if r.get("decimal_price") is not None and r["decimal_price"] > 0
    ]
    book_by_event_side: dict[tuple, list[dict]] = {}
    for r in books:
        side = _resolve_team(
            r.get("outcome_name"), r.get("home_team"), r.get("away_team"),
            r.get("sport_key"),
        )
        if side is None:
            continue  # book-side mapping failures aren't counted (book is source-of-truth)
        book_by_event_side.setdefault((r["event_id"], side), []).append(r)

    opps: list[dict] = []
    seen_hashes: set[str] = set()

    for v in venue_rows:
        ask = v.get("ask")
        if ask is None:
            stats["missing_ask"] += 1
            continue
        if ask <= 0 or ask >= 1:
            # Out-of-range probability; drop silently (not a team-match failure).
            continue

        # Resolve the venue outcome to a team side.
        v_side = _resolve_team(
            v.get("outcome_name"), v.get("home_team"), v.get("away_team"),
            v.get("sport_key"),
        )
        if v_side is None:
            # Distinguish ambiguous (resolved to both) from no-match for stats.
            if _is_ambiguous(
                v.get("outcome_name"), v.get("home_team"), v.get("away_team"),
                v.get("sport_key"),
            ):
                stats["ambiguous"] += 1
            else:
                stats["no_team_match"] += 1
            continue

        # The venue leg covers v_side; the book leg must be on the OTHER side.
        book_side = "away" if v_side == "home" else "home"
        candidates = book_by_event_side.get((v["event_id"], book_side), [])
        for b in candidates:
            opp = _build_game_venue_opp(
                b, v, ask, base_wager=base_wager, min_roi=min_roi
            )
            if opp is None:
                continue
            h = opp["opportunity_hash"]
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            opps.append(opp)
            stats["paired"] += 1

    return opps, stats


def _is_ambiguous(outcome_name, home_team, away_team, sport_key) -> bool:
    if not outcome_name:
        return False
    alias_sport = _SPORT_KEY_TO_ALIAS.get(sport_key)
    if alias_sport is not None:
        c_out = canonical_team(outcome_name, alias_sport)
        if c_out is None:
            return False
        return c_out == canonical_team(home_team, alias_sport) and c_out == canonical_team(
            away_team, alias_sport
        )
    o = str(outcome_name).strip().lower()
    h = str(home_team or "").strip().lower()
    a = str(away_team or "").strip().lower()
    is_home = bool(o) and bool(h) and (o in h or h in o)
    is_away = bool(o) and bool(a) and (o in a or a in o)
    return is_home and is_away


def _build_game_venue_opp(
    book: dict, venue: dict, ask: float, *, base_wager: float, min_roi: float
) -> Optional[dict]:
    d = book["decimal_price"]
    venue_key = venue["venue_key"]
    (
        stake_book,
        stake_venue,
        payout,
        arb_ev,
        roi_pre,
        roi_fee,
    ) = _book_venue_economics(d, ask, venue_key, base_wager)
    if not roi_fee > min_roi:
        return None

    captured_book = book.get("captured_at")
    captured_venue = venue.get("captured_at")
    commence = book.get("commence_time") or venue.get("commence_time")
    latest_capture = max(c for c in (captured_book, captured_venue) if c is not None)
    hours = (commence - latest_capture).total_seconds() / 3600.0
    if not hours > 0:
        return None

    fee_amount = venue_effective_cost(ask, venue_key) - ask

    h = opportunity_hash(
        kind="game",
        pairing="book_venue",
        event_id=book["event_id"],
        market_segment="h2h",
        legs=[
            (book["bookmaker"], book["outcome_name"], None, d),
            (venue_key, venue["outcome_name"], None, ask),
        ],
        player_name=None,
        line=None,
    )

    return {
        "opportunity_hash": h,
        "kind": "game",
        "pairing": "book_venue",
        "event_id": book["event_id"],
        "sport_key": book["sport_key"],
        "home_team": book["home_team"],
        "away_team": book["away_team"],
        "commence_time": commence,
        "market_segment": "h2h",
        "player_name": None,
        "line": None,
        "pairing_type": None,
        "leg_a_kind": "book",
        "leg_a_source": book["bookmaker"],
        "leg_a_outcome": book["outcome_name"],
        "leg_a_point": None,
        "leg_a_price": d,
        "leg_a_stake": stake_book,
        "leg_b_kind": "venue",
        "leg_b_source": venue_key,
        "leg_b_outcome": venue["outcome_name"],
        "leg_b_point": None,
        "leg_b_price": ask,
        "leg_b_stake": stake_venue,
        "payout": payout,
        "arb_ev": arb_ev,
        "roi": roi_pre,
        "fee_adjusted_roi": roi_fee,
        "hours_until_commence": hours,
        "details": {
            "market_id": venue.get("market_id"),
            "book_captured_at": _iso(captured_book),
            "venue_captured_at": _iso(captured_venue),
            "fee_inputs": {
                "venue_key": venue_key,
                "ask": ask,
                "fee": fee_amount,
            },
        },
    }
