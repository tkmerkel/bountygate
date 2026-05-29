"""Unit tests for bountygate.enrichment.weather — Open-Meteo URL + nearest-hour
parse, asserted against the real fixture."""

from bountygate.enrichment import weather


class TestForecastUrl:
    def test_includes_coords_and_hourly_fields(self):
        url = weather.build_forecast_url(40.8296, -73.9262)
        assert "latitude=40.8296" in url
        assert "longitude=-73.9262" in url
        assert "temperature_2m" in url
        assert "wind_speed_10m" in url
        assert "timezone=UTC" in url


class TestNearestHourly:
    def test_exact_hour_match(self, load_fixture):
        payload = load_fixture("open_meteo.json")
        # fixture hour index 2 == 2026-05-29T02:00, temp 64.2, wind 9.2
        got = weather.nearest_hourly(payload, "2026-05-29T02:00:00+00:00")
        assert got["temp_f"] == 64.2
        assert got["wind_mph"] == 9.2
        assert set(got) >= {
            "temp_f", "wind_mph", "wind_dir_deg", "precip_prob", "humidity",
        }

    def test_rounds_to_closest_hour(self, load_fixture):
        payload = load_fixture("open_meteo.json")
        # 02:20 is closest to the 02:00 bucket
        got = weather.nearest_hourly(payload, "2026-05-29T02:20:00+00:00")
        assert got["temp_f"] == 64.2

    def test_missing_hourly_returns_none(self):
        assert weather.nearest_hourly({}, "2026-05-29T02:00:00+00:00") is None

    def test_bad_target_returns_none(self, load_fixture):
        payload = load_fixture("open_meteo.json")
        assert weather.nearest_hourly(payload, None) is None
