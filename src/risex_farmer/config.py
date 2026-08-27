"""Frozen PAPER-001 configuration values."""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class PaperConfig:
    paper_balance_usd: Decimal = Decimal("10000")
    target_notional_per_leg_usd: Decimal = Decimal("500")
    max_open_positions: int = 1
    normal_scan_seconds: int = 120
    focused_window_seconds: int = 300
    focused_scan_seconds: int = 10
    entry_maker_start_before_funding_seconds: int = 120
    entry_maker_cancel_before_funding_seconds: int = 5
    entry_order_reprice_seconds: int = 10
    open_position_monitor_seconds: int = 10
    normal_exit_aggressive_after_seconds: int = 10
    websocket_health_check_seconds: int = 10
    max_market_stream_silence_seconds: int = 25
    default_max_funding_data_age_seconds: int = 120
    extended_required_markets_max_age_seconds: int = 300
    extended_universe_refresh_seconds: int = 600
    extended_universe_max_age_seconds: int = 1200
    extended_universe_request_timeout_seconds: int = 60
    risex_maker_fee_rate: Decimal = Decimal("0.00005")
    risex_taker_fee_rate: Decimal = Decimal("0.00021")
    extended_maker_fee_rate: Decimal = Decimal("0")
    extended_taker_fee_rate: Decimal = Decimal("0.00025")
    nado_maker_fee_rate: Decimal = Decimal("0.0001")
    nado_taker_fee_rate: Decimal = Decimal("0.00035")
    expected_basis_convergence_pnl_usd: Decimal = Decimal("0")
    points_value_usd: Decimal = Decimal("0")
    paper_entry_min_planned_net_pnl_usd: Decimal = Decimal("0")
    btc_eth_hard_basis_expansion_rate: Decimal = Decimal("0.04")
    other_top5_hard_basis_expansion_rate: Decimal = Decimal("0.06")
    risex_paper_fallback_assumptions_enabled: bool = True


PAPER_CONFIG = PaperConfig()

# Keep the frozen specification's names available to callers while retaining a
# single immutable configuration object for dependency-free composition.
PAPER_BALANCE_USD = PAPER_CONFIG.paper_balance_usd
TARGET_NOTIONAL_PER_LEG_USD = PAPER_CONFIG.target_notional_per_leg_usd
MAX_OPEN_POSITIONS = PAPER_CONFIG.max_open_positions
NORMAL_SCAN_SECONDS = PAPER_CONFIG.normal_scan_seconds
FOCUSED_WINDOW_SECONDS = PAPER_CONFIG.focused_window_seconds
FOCUSED_SCAN_SECONDS = PAPER_CONFIG.focused_scan_seconds
ENTRY_MAKER_START_BEFORE_FUNDING_SECONDS = PAPER_CONFIG.entry_maker_start_before_funding_seconds
ENTRY_MAKER_CANCEL_BEFORE_FUNDING_SECONDS = PAPER_CONFIG.entry_maker_cancel_before_funding_seconds
ENTRY_ORDER_REPRICE_SECONDS = PAPER_CONFIG.entry_order_reprice_seconds
OPEN_POSITION_MONITOR_SECONDS = PAPER_CONFIG.open_position_monitor_seconds
NORMAL_EXIT_AGGRESSIVE_AFTER_SECONDS = PAPER_CONFIG.normal_exit_aggressive_after_seconds
WEBSOCKET_HEALTH_CHECK_SECONDS = PAPER_CONFIG.websocket_health_check_seconds
MAX_MARKET_STREAM_SILENCE_SECONDS = PAPER_CONFIG.max_market_stream_silence_seconds
DEFAULT_MAX_FUNDING_DATA_AGE_SECONDS = PAPER_CONFIG.default_max_funding_data_age_seconds
EXTENDED_REQUIRED_MARKETS_MAX_AGE_SECONDS = PAPER_CONFIG.extended_required_markets_max_age_seconds
EXTENDED_UNIVERSE_REFRESH_SECONDS = PAPER_CONFIG.extended_universe_refresh_seconds
EXTENDED_UNIVERSE_MAX_AGE_SECONDS = PAPER_CONFIG.extended_universe_max_age_seconds
EXTENDED_UNIVERSE_REQUEST_TIMEOUT_SECONDS = PAPER_CONFIG.extended_universe_request_timeout_seconds
RISEX_MAKER_FEE_RATE = PAPER_CONFIG.risex_maker_fee_rate
RISEX_TAKER_FEE_RATE = PAPER_CONFIG.risex_taker_fee_rate
EXTENDED_MAKER_FEE_RATE = PAPER_CONFIG.extended_maker_fee_rate
EXTENDED_TAKER_FEE_RATE = PAPER_CONFIG.extended_taker_fee_rate
NADO_MAKER_FEE_RATE = PAPER_CONFIG.nado_maker_fee_rate
NADO_TAKER_FEE_RATE = PAPER_CONFIG.nado_taker_fee_rate
EXPECTED_BASIS_CONVERGENCE_PNL_USD = PAPER_CONFIG.expected_basis_convergence_pnl_usd
POINTS_VALUE_USD = PAPER_CONFIG.points_value_usd
PAPER_ENTRY_MIN_PLANNED_NET_PNL_USD = PAPER_CONFIG.paper_entry_min_planned_net_pnl_usd
BTC_ETH_HARD_BASIS_EXPANSION_RATE = PAPER_CONFIG.btc_eth_hard_basis_expansion_rate
OTHER_TOP5_HARD_BASIS_EXPANSION_RATE = PAPER_CONFIG.other_top5_hard_basis_expansion_rate
RISEX_PAPER_FALLBACK_ASSUMPTIONS_ENABLED = (
    PAPER_CONFIG.risex_paper_fallback_assumptions_enabled
)
