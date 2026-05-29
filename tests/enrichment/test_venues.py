"""Unit tests for bountygate.enrichment.venues — home-team → ballpark lat/lon."""

from bountygate.enrichment import venues


class TestLookupPark:
    def test_known_outdoor_park(self):
        got = venues.lookup_park("New York Yankees")
        assert got is not None
        assert 40.5 < got["lat"] < 41.1
        assert -74.2 < got["lon"] < -73.7
        assert got["is_dome"] is False

    def test_normalizes_team_name(self):
        # lookup should be normalization-insensitive (case / spacing)
        assert venues.lookup_park("  los angeles DODGERS ") == venues.lookup_park(
            "Los Angeles Dodgers"
        )

    def test_dome_flagged(self):
        got = venues.lookup_park("Tampa Bay Rays")
        assert got is not None
        assert got["is_dome"] is True

    def test_unknown_team_returns_none(self):
        assert venues.lookup_park("Toronto Maple Leafs") is None

    def test_all_thirty_mlb_teams_present(self):
        assert len(venues.MLB_PARKS) == 30

    def test_every_park_has_required_fields(self):
        for key, park in venues.MLB_PARKS.items():
            assert set(park) >= {"lat", "lon", "park", "is_dome"}
            assert -90 <= park["lat"] <= 90
            assert -180 <= park["lon"] <= 180
