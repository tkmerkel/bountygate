"""Book x prediction-market (book x venue) player-prop arbitrage detection.

Prediction-market prop markets (mostly Kalshi) phrase a prop as a yes/no
question: "Will LeBron James score 30+ points?". A YES contract resolves true
iff the actual stat is >= the threshold k; a NO contract resolves true iff the
stat is < k. That is exactly the book Over/Under split at line (k - 0.5):

    venue YES (>= k)   <->  book UNDER (< k-0.5)?  NO — opposite:
        YES wins iff actual >= k; book UNDER (line k-0.5) wins iff actual < k-0.5,
        i.e. actual <= k-1 (integer stats) i.e. actual < k. Exactly one wins.
        => venue YES pairs with book UNDER at line k-0.5.
    venue NO (< k)     <->  book OVER  at line k-0.5
        NO wins iff actual < k; book OVER (line k-0.5) wins iff actual >= k.
        => venue NO pairs with book OVER at line k-0.5.

This module is PRECISION-FIRST: we NEVER guess a player or stat. A title that
doesn't cleanly parse is counted as unparsed and dropped; a parse that matches
two stat patterns is ambiguous and dropped. Matching a parsed venue market to a
book leg requires event_id equality, exact normalized-player equality, canonical
market-key equality, and exact line equality.

Economics are shared with venue.py (``_book_venue_economics``): the book leg
costs 1/decimal, the venue leg costs ask + Kalshi taker fee, stakes normalize to
``base_wager`` total, and the min_roi filter applies to the fee-adjusted roi.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

from bountygate.arb.hashing import opportunity_hash
from bountygate.arb.venue import _book_venue_economics, _iso
from bountygate.arb.fees import venue_effective_cost

_ALT_SUFFIX = "_alternate"

# stat regexes keyed by sport_key -> {canonical book market_key: pattern}.
# Patterns capture the integer threshold k as group(1).
STAT_PATTERNS: dict[str, dict[str, str]] = {
    "basketball_nba": {
        "player_points": r"(?:scores?\s+)?(\d+)\+\s+points",
        "player_assists": r"(\d+)\+\s+assists",
        "player_rebounds": r"(\d+)\+\s+rebounds",
        "player_threes": r"(\d+)\+\s+(?:three|3)-?pointers?(?:\s+made)?",
    },
    "icehockey_nhl": {
        "player_shots_on_goal": r"(\d+)\+\s+shots\s+on\s+goal",
        "player_points": r"(\d+)\+\s+points",
    },
    "baseball_mlb": {},  # MLB props on Kalshi are mostly yes/no (home run) — not arbable in v1
}

# Basketball variants reuse the NBA stat dict.
_BASKETBALL_NBA = STAT_PATTERNS["basketball_nba"]


def _patterns_for(sport_key: str) -> dict[str, str]:
    """Resolve the stat-pattern dict for a sport_key (basketball_* -> NBA dict)."""
    if sport_key in STAT_PATTERNS:
        return STAT_PATTERNS[sport_key]
    if sport_key and sport_key.startswith("basketball_"):
        return _BASKETBALL_NBA
    return {}


def normalize_player(name: str) -> str:
    """Casefold, strip accents (NFKD->ascii), strip punctuation, collapse whitespace.

    Known v1 limitation: name suffixes (Jr./Sr./II/III) are NOT stripped — a
    suffix mismatch between book and venue names fails precision-first to
    unmatched (drops real arbs, never creates false ones).
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.casefold()
    s = re.sub(r"[^\w\s]", " ", s)  # drop punctuation (keep word chars + whitespace)
    s = re.sub(r"\s+", " ", s).strip()
    return s


_WILL_PREFIX = re.compile(r"^\s*will\s+", re.IGNORECASE)

# Whitelist of allowed trailing text after the matched stat pattern. Anything
# else (e.g. "in the first half", "in Q1", "vs the Celtics") indicates a
# partial-game / conditional scope the full-game book lines do NOT cover, so we
# reject it precision-first rather than emit a phantom arb.
_ALLOWED_TAIL = re.compile(r"^\s*(?:tonight|today)?\s*[?.!]*\s*$", re.IGNORECASE)


def parse_prop_title(title: str, sport_key: str) -> Optional[dict]:
    """Parse a venue prop title into {player_name, market_segment, line} or None.

    Precision-first: returns None unless the title starts with "Will " (case
    insensitive), exactly one stat pattern matches, and the player segment
    (text between "Will " and the matched pattern) is a plausible name (1..4
    words, no "or"/comma). ``line`` is ``threshold - 0.5`` (Kalshi k+ YES is
    book Over at k-0.5).
    """
    if not title:
        return None
    patterns = _patterns_for(sport_key)
    if not patterns:
        return None

    low = str(title)
    if not _WILL_PREFIX.match(low):
        return None

    matches = []  # (market_segment, match_obj)
    for market_segment, pat in patterns.items():
        m = re.search(pat, low, re.IGNORECASE)
        if m:
            matches.append((market_segment, m))
    if len(matches) != 1:
        return None  # zero -> unknown stat; >1 -> ambiguous

    market_segment, m = matches[0]
    try:
        threshold = int(m.group(1))
    except (TypeError, ValueError):
        return None

    # Player = text between "Will " and the matched stat pattern.
    after_will = _WILL_PREFIX.sub("", low, count=1)
    # The match was found in `low`; recompute its start relative to after_will.
    m2 = re.search(patterns[market_segment], after_will, re.IGNORECASE)
    if not m2:
        return None

    # TAIL WHITELIST: text after the matched stat must be empty / punctuation /
    # an allowed "tonight"/"today" word. Anything else is a partial-game or
    # conditional scope (e.g. "in the first half", "in Q1") -> reject.
    tail = after_will[m2.end():]
    if not _ALLOWED_TAIL.match(tail):
        return None

    player_segment = after_will[: m2.start()]

    # Strip common filler verbs that sit between the name and the stat phrase.
    player_segment = re.sub(
        r"\b(?:to|record|records|get|gets|have|has|tally|tallies|make|makes|grab|grabs|dish|dishes|score|scores)\b",
        " ",
        player_segment,
        flags=re.IGNORECASE,
    )
    raw = player_segment.strip()
    if not raw:
        return None
    # Leading-"the " guard: rejects team totals ("the Lakers score 110+ points")
    # at parse time rather than relying on downstream unmatched_player.
    if raw.casefold().startswith("the "):
        return None
    # Conservative guards: reject conjunctions / lists / over-long segments.
    if "," in raw or re.search(r"\bor\b", raw, re.IGNORECASE):
        return None
    if len(raw.split()) > 4:
        return None

    player_norm = normalize_player(raw)
    if not player_norm:
        return None

    return {
        "player_name": player_norm,
        "market_segment": market_segment,
        "line": threshold - 0.5,
    }


def _strip_alt_suffix(market_key: str) -> str:
    if market_key.endswith(_ALT_SUFFIX):
        return market_key[: -len(_ALT_SUFFIX)]
    return market_key


def pair_book_venue_props(
    prop_rows: list[dict],
    venue_prop_rows: list[dict],
    *,
    base_wager: float = 100.0,
    min_roi: float = 0.0,
) -> tuple[list[dict], dict]:
    """Pair sportsbook over/under prop legs with venue YES/NO prop contracts.

    ``prop_rows``: pairs.pair_props row shape::

        {event_id, sport_key, home_team, away_team, commence_time (tz-aware),
         market_key, player_name, line (float), side ('over'|'under'),
         bookmaker, decimal_price (float|None), captured_at (tz-aware)}

    ``venue_prop_rows``: same shape as pair_book_venue_game's venue_rows (one
    row per venue outcome with ask + title + event_id via link). A Kalshi prop
    market has Yes/No outcomes; a 'Yes' row's ask buys YES, a 'No' row buys NO.

    Pairing: venue YES at threshold k pairs with book UNDER at k-0.5; venue NO
    pairs with book OVER at k-0.5.

    Returns ``(opps, stats)`` with stats keys: parsed, unparsed, ambiguous,
    matched, unmatched_player, unmatched_line, paired.
    """
    stats = {
        "parsed": 0,
        "unparsed": 0,
        "ambiguous": 0,
        "matched": 0,
        "unmatched_player": 0,
        "unmatched_line": 0,
        "paired": 0,
    }

    # Index book prop legs by (event_id, normalized player, canonical market, side).
    book_index: dict[tuple, list[dict]] = {}
    for r in prop_rows:
        if r.get("decimal_price") is None or r["decimal_price"] <= 0:
            continue
        if r.get("line") is None:
            continue
        side = r.get("side")
        if side not in ("over", "under"):
            continue
        canonical = _strip_alt_suffix(r["market_key"])
        key = (
            r["event_id"],
            normalize_player(r.get("player_name")),
            canonical,
            side,
        )
        book_index.setdefault(key, []).append(r)

    opps: list[dict] = []
    seen_hashes: set[str] = set()

    for v in venue_prop_rows:
        title = v.get("title")
        parsed = parse_prop_title(title, v.get("sport_key"))
        if parsed is None:
            # Distinguish ambiguous (2 patterns) from unparsed for stats.
            if _title_ambiguous(title, v.get("sport_key")):
                stats["ambiguous"] += 1
            else:
                stats["unparsed"] += 1
            continue
        stats["parsed"] += 1

        ask = v.get("ask")
        if ask is None or ask <= 0 or ask >= 1:
            continue

        outcome = str(v.get("outcome_name") or "").strip().lower()
        if outcome == "yes":
            book_side = "under"
            venue_outcome = "yes"
        elif outcome == "no":
            book_side = "over"
            venue_outcome = "no"
        else:
            continue  # not a Yes/No contract row

        key = (
            v.get("event_id"),
            parsed["player_name"],
            parsed["market_segment"],
            book_side,
        )
        candidates = book_index.get(key)
        if not candidates:
            # Diagnose whether the player exists for this event/market at all
            # (line mismatch) vs. player/market not present (unmatched_player).
            if _player_market_present(
                book_index, v.get("event_id"), parsed["player_name"],
                parsed["market_segment"], book_side,
            ):
                stats["unmatched_line"] += 1
            else:
                stats["unmatched_player"] += 1
            continue

        # Filter candidates to exact line match.
        line_matches = [b for b in candidates if b["line"] == parsed["line"]]
        if not line_matches:
            stats["unmatched_line"] += 1
            continue

        stats["matched"] += 1
        for b in line_matches:
            opp = _build_prop_venue_opp(
                b,
                v,
                ask=ask,
                venue_outcome=venue_outcome,
                parsed=parsed,
                base_wager=base_wager,
                min_roi=min_roi,
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


def _title_ambiguous(title, sport_key) -> bool:
    if not title:
        return False
    patterns = _patterns_for(sport_key)
    if not patterns:
        return False
    if not _WILL_PREFIX.match(str(title)):
        return False
    n = sum(1 for pat in patterns.values() if re.search(pat, str(title), re.IGNORECASE))
    return n > 1


def _player_market_present(book_index, event_id, player_norm, market_segment, side) -> bool:
    """True if any book leg exists for (event, player, market, side) regardless of line."""
    return (event_id, player_norm, market_segment, side) in book_index


def _build_prop_venue_opp(
    book: dict,
    venue: dict,
    *,
    ask: float,
    venue_outcome: str,
    parsed: dict,
    base_wager: float,
    min_roi: float,
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

    market_key = book["market_key"]
    canonical = _strip_alt_suffix(market_key)
    line = book["line"]
    side = book["side"]
    threshold = parsed["line"] + 0.5
    fee_amount = venue_effective_cost(ask, venue_key) - ask

    h = opportunity_hash(
        kind="prop",
        pairing="book_venue",
        event_id=book["event_id"],
        market_segment=canonical,
        legs=[
            (book["bookmaker"], f"{side}:{market_key}", line, d),
            (venue_key, venue_outcome, line, ask),
        ],
        player_name=book["player_name"],
        line=line,
    )

    return {
        "opportunity_hash": h,
        "kind": "prop",
        "pairing": "book_venue",
        "event_id": book["event_id"],
        "sport_key": book["sport_key"],
        "home_team": book["home_team"],
        "away_team": book["away_team"],
        "commence_time": commence,
        "market_segment": canonical,
        "player_name": book["player_name"],
        "line": line,
        "pairing_type": None,
        "leg_a_kind": "book",
        "leg_a_source": book["bookmaker"],
        "leg_a_outcome": side,
        "leg_a_point": line,
        "leg_a_price": d,
        "leg_a_stake": stake_book,
        "leg_b_kind": "venue",
        "leg_b_source": venue_key,
        "leg_b_outcome": venue_outcome,
        "leg_b_point": line,
        "leg_b_price": ask,
        "leg_b_stake": stake_venue,
        "payout": payout,
        "arb_ev": arb_ev,
        "roi": roi_pre,
        "fee_adjusted_roi": roi_fee,
        "hours_until_commence": hours,
        "details": {
            "market_id": venue.get("market_id"),
            "title": venue.get("title"),
            "threshold": threshold,
            "book_captured_at": _iso(captured_book),
            "venue_captured_at": _iso(captured_venue),
            "fee_inputs": {
                "venue_key": venue_key,
                "ask": ask,
                "fee": fee_amount,
            },
        },
    }
