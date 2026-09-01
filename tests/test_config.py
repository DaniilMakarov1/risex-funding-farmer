from dataclasses import fields
from decimal import Decimal

import pytest

from risex_farmer.config import (
    OTHER_ASSET_HARD_BASIS_EXPANSION_RATE,
    PAPER_CONFIG,
    SYNTHETIC_TEST_OVERLAY_USD,
    PaperConfig,
)


def test_extended_cache_and_timeout_constants_are_exact() -> None:
    assert PAPER_CONFIG.extended_required_markets_max_age_seconds == 300
    assert PAPER_CONFIG.extended_universe_refresh_seconds == 600
    assert PAPER_CONFIG.extended_universe_max_age_seconds == 1200
    assert PAPER_CONFIG.extended_universe_request_timeout_seconds == 60


def test_paper_maker_activation_window_is_three_minutes() -> None:
    assert PAPER_CONFIG.entry_maker_start_before_funding_seconds == 180
    assert PAPER_CONFIG.entry_maker_cancel_before_funding_seconds == 5


def test_other_asset_hard_basis_threshold_uses_spec_name_and_value() -> None:
    assert PAPER_CONFIG.other_asset_hard_basis_expansion_rate == Decimal("0.06")
    assert OTHER_ASSET_HARD_BASIS_EXPANSION_RATE == Decimal("0.06")
    names = {field.name for field in fields(PAPER_CONFIG)}
    assert "other_asset_hard_basis_expansion_rate" in names
    assert not any("top5" in name for name in names)


def test_synthetic_test_overlay_is_disabled_by_default_and_authorized_only() -> None:
    assert PAPER_CONFIG.synthetic_test_pnl_overlay_usd == Decimal("0")
    assert PaperConfig(
        synthetic_test_pnl_overlay_usd=Decimal("0.500")
    ).synthetic_test_pnl_overlay_usd == SYNTHETIC_TEST_OVERLAY_USD
    with pytest.raises(ValueError, match="exactly 0 or 0.50"):
        PaperConfig(synthetic_test_pnl_overlay_usd=Decimal("0.49"))
