"""Deterministic pick matching for bet selection.

Player names are matched fuzzily (see ``text_match.fuzzy_contains``);
EVERYTHING else — side (Over/Under), line, and threshold — is matched EXACTLY
here. This module turns a rendered pick's text into a structured ``Pick`` and
selects the *unique* pick that matches a target. No substring matching, no
confidence ranking, no "first match" fallback: a target matches exactly one
rendered pick or we raise.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple, TypeVar

from bet_placer import BetPlacerError


class NoPickError(BetPlacerError):
    """No rendered pick matched the target line/side/threshold exactly."""


class AmbiguousPickError(BetPlacerError):
    """More than one rendered pick matched the target — refuse to guess."""


_LINE_TOL = 1e-6

# Side token (whole word) followed by the FULL numeric line token. Anchoring
# the number with \b on both sides is what prevents "1.5" from matching inside
# "11.5" and "O 1" from matching inside "O 11.5".
_PICK_RE = re.compile(
    r"\b(over|under|o|u)\b[\s,:]*([0-9]+(?:\.[0-9]+)?)\b",
    re.IGNORECASE,
)
# A standalone decimal price (>= 1.0) appearing after the line = odds.
_ODDS_RE = re.compile(r"\b([1-9][0-9]*\.[0-9]+)\b")
# "N+" threshold, exact integer (not preceded by another digit, so "5" is not
# matched inside "15+").
_THRESHOLD_RE = re.compile(r"(?<!\d)(\d+)\+")
# FanDuel MLB threshold-one labels render as verbs, not "1+ X".
_THRESHOLD_ONE_PHRASES = (
    "to hit a single", "to hit a double", "to hit a triple",
    "to record an rbi", "to record a walk", "to record a strikeout",
    "to hit a home run", "to record a stolen base", "to record a hit",
    "to record a run", "to record a total base",
)


@dataclass(frozen=True)
class Pick:
    side: str               # "over" | "under"
    line: float
    odds: Optional[float] = None


def _norm(text: str) -> str:
    return " ".join((text or "").split())


def parse_pick(text: str) -> Optional[Pick]:
    """Parse a rendered pick's text into a ``Pick``, or ``None`` if it isn't a
    pick. Handles BetMGM "O 11.5  2.00" and FanDuel aria-labels that embed the
    side+line in a sentence. Captures the FULL numeric line token."""
    norm = _norm(text)
    if not norm:
        return None
    m = _PICK_RE.search(norm)
    if not m:
        return None
    token, num = m.group(1).lower(), m.group(2)
    side = "over" if token in ("o", "over") else "under"
    line = float(num)
    odds = None
    om = _ODDS_RE.search(norm[m.end():])
    if om:
        odds = float(om.group(1))
    return Pick(side=side, line=line, odds=odds)


def parse_threshold(text: str) -> Optional[int]:
    """Parse an exact 'N+' threshold (so '5+' != '15+'), or a FanDuel MLB
    threshold-one verb label (-> 1), or ``None``."""
    norm = _norm(text)
    if not norm:
        return None
    low = norm.lower()
    for phrase in _THRESHOLD_ONE_PHRASES:
        if phrase in low:
            return 1
    m = _THRESHOLD_RE.search(norm)
    return int(m.group(1)) if m else None


def line_equals(a: float, b: float) -> bool:
    """Exact numeric line equality (never substring)."""
    return abs(float(a) - float(b)) < _LINE_TOL


T = TypeVar("T")


def select_unique(
    items: Iterable[Tuple[T, str]],
    target_line: Optional[float],
    target_side: Optional[str],
    *,
    threshold: bool = False,
) -> T:
    """Return the single item whose text matches the target exactly.

    ``items``: iterable of ``(element, text)``. When ``threshold=True``, match
    ``parse_threshold(text) == int(target_line + 0.5)`` (and ``target_side`` is
    'over' or None); otherwise match ``parse_pick(text).side == target_side``
    and ``line_equals(parse_pick(text).line, target_line)``.

    Raises ``NoPickError`` on 0 matches, ``AmbiguousPickError`` on >1. No
    fallback.
    """
    matches: List[T] = []
    if threshold:
        want = int(float(target_line) + 0.5)
        for elem, text in items:
            if target_side is not None and target_side != "over":
                continue
            if parse_threshold(text) == want:
                matches.append(elem)
    else:
        for elem, text in items:
            p = parse_pick(text)
            if p is None:
                continue
            if target_side is not None and p.side != target_side:
                continue
            if target_line is not None and not line_equals(p.line, target_line):
                continue
            matches.append(elem)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise NoPickError(
            f"No pick matched target_line={target_line} "
            f"target_side={target_side} threshold={threshold}"
        )
    raise AmbiguousPickError(
        f"{len(matches)} picks matched target_line={target_line} "
        f"target_side={target_side} threshold={threshold} — refusing to guess"
    )
