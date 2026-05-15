from bg_arb_pipeline_lib.hashing import opportunity_hash, derive_pairing_type


def _sample_row(**overrides):
    base = {
        "event_id": "evt_abc",
        "player_name": "Dennis Schroder",
        "under_book": "betmgm",
        "under_market_key": "player_assists",
        "over_book": "fanduel",
        "over_market_key": "player_assists_alternate",
        "under_line": 2.5,
        "under_price": 1.90,
        "over_line": 2.5,
        "over_price": 1.85,
    }
    base.update(overrides)
    return base


class TestOpportunityHash:
    def test_stable_across_runs(self):
        row = _sample_row()
        assert opportunity_hash(row) == opportunity_hash(row)

    def test_differs_when_price_changes(self):
        a = _sample_row(under_price=1.90)
        b = _sample_row(under_price=1.91)
        assert opportunity_hash(a) != opportunity_hash(b)

    def test_differs_when_book_changes(self):
        a = _sample_row(under_book="betmgm")
        b = _sample_row(under_book="fanduel")
        assert opportunity_hash(a) != opportunity_hash(b)

    def test_rounding_stable_at_third_decimal_for_line(self):
        a = _sample_row(under_line=2.5)
        b = _sample_row(under_line=2.5000001)
        assert opportunity_hash(a) == opportunity_hash(b)

    def test_rounding_stable_at_sixth_decimal_for_price(self):
        a = _sample_row(under_price=1.90)
        b = _sample_row(under_price=1.9000001)
        assert opportunity_hash(a) == opportunity_hash(b)

    def test_returns_sha256_hex(self):
        h = opportunity_hash(_sample_row())
        assert len(h) == 64
        int(h, 16)


class TestDerivePairingType:
    def test_std_std_when_neither_ends_in_alternate(self):
        assert derive_pairing_type("player_assists", "player_assists") == "std_std"

    def test_std_alt_when_only_over_is_alternate(self):
        assert derive_pairing_type("player_assists", "player_assists_alternate") == "std_alt"

    def test_alt_std_when_only_under_is_alternate(self):
        assert derive_pairing_type("player_assists_alternate", "player_assists") == "alt_std"

    def test_alt_alt_when_both_are_alternate(self):
        assert derive_pairing_type(
            "player_assists_alternate", "player_assists_alternate"
        ) == "alt_alt"

    def test_substring_match_does_not_count(self):
        assert derive_pairing_type("alternate_player_assists", "player_assists") == "std_std"
