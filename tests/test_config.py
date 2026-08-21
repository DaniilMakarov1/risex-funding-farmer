from risex_farmer.config import PAPER_CONFIG


def test_extended_cache_and_timeout_constants_are_exact() -> None:
    assert PAPER_CONFIG.extended_required_markets_max_age_seconds == 300
    assert PAPER_CONFIG.extended_universe_refresh_seconds == 600
    assert PAPER_CONFIG.extended_universe_max_age_seconds == 1200
    assert PAPER_CONFIG.extended_universe_request_timeout_seconds == 60
