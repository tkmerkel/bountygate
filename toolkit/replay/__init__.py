"""Replay package: re-execute a recorder trace against a live page.

Replay structurally halts at any record marked terminal=True (the place-bet
click), so this package is safe against the user's logged-in profile.
"""
