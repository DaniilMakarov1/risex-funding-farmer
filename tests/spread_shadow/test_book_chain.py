from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal as D
import json
from pathlib import Path
import time

import pytest

from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    MarketType,
    Venue,
)
from risex_spread_shadow import (
    AppendOnlyEvidenceStore,
    BookEvidence,
    BookRevisionChainError,
    BookRevisionEncoder,
    BookRevisionReconstructor,
    FeedBookEvent,
    FeedTradeEvent,
    MarketPair,
    MAX_EVIDENCE_FILE_BYTES,
    ShadowConfig,
    Side,
    SpreadObserver,
    TradeEvidence,
    audit_book_revisions,
    build_report,
    book_state_sha256,
    iter_records,
    reconstruct_book_records,
    store_permissions,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 3, tzinfo=UTC)


def _market(venue: Venue, symbol: str, asset: str = "BTC") -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset=asset,
        venue=venue,
        venue_symbol=symbol,
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=D("1"),
        quote_asset="USDC",
        settlement_asset="USDC",
        tick_size_raw=D("1"),
        quantity_step_raw=D("1"),
        minimum_quantity_raw=D("1"),
        minimum_notional_usd=D("0"),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


PAIR = MarketPair(
    "BTC",
    _market(Venue.RISEX, "BTC/USDC"),
    _market(Venue.LIGHTER, "BTC"),
)


def _book(
    venue: Venue,
    *,
    revision: int,
    received: int,
    session: str = "session",
    recovery: int = 0,
    bids: tuple[tuple[str, str], ...] = (("99", "10"),),
    asks: tuple[tuple[str, str], ...] = (("101", "10"),),
) -> BookEvidence:
    return BookEvidence(
        venue=venue,
        canonical_market="BTC",
        bids=tuple(BookLevel(D(price), D(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(D(price), D(quantity)) for price, quantity in asks),
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        book_revision=revision,
        sequence=revision,
        checksum=revision,
        sequence_valid=True,
        checksum_valid=True,
        received_utc=NOW,
        fresh=True,
    )


def _store_records(root: Path, records: tuple[dict, ...]) -> Path:
    store = AppendOnlyEvidenceStore.create(
        root,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    store.append_batch(
        (
            *records,
            {"kind": "RUN_STOP", "fatal_reason": None, "observed_monotonic_ns": 10_000},
        )
    )
    path = store.path
    store.close()
    return path


def test_book_chain_reconstructs_exact_changes_and_keeps_only_current_levels() -> None:
    first = _book(
        Venue.LIGHTER,
        revision=1,
        received=1,
        bids=(("100", "5"), ("99", "4")),
        asks=(("101", "3"), ("102", "2")),
    )
    second = _book(
        Venue.LIGHTER,
        revision=2,
        received=2,
        bids=(("100", "7"), ("98", "1")),
        asks=(("101", "3"), ("103", "6")),
    )
    encoder = BookRevisionEncoder()
    full = encoder.encode(first, source_kind="SNAPSHOT")
    delta = encoder.encode(second, source_kind="DELTA")

    assert full["book_encoding"] == "FULL"
    assert delta["book_encoding"] == "DELTA"
    assert delta["predecessor_book_revision"] == 1
    assert delta["predecessor_book_revision_id"] == first.book_revision_id
    assert {row["price"] for row in delta["bids"]} == {"100", "99", "98"}
    assert {row["price"] for row in delta["asks"]} == {"102", "103"}
    assert {row["price"] for row in delta["bids"] if row["quantity"] == "0"} == {"99"}
    assert {row["price"] for row in delta["asks"] if row["quantity"] == "0"} == {"102"}
    assert encoder.current_level_count == len(second.bids) + len(second.asks)

    reconstructor = BookRevisionReconstructor()
    reconstructed = tuple(reconstructor.append(record) for record in (full, delta))
    assert reconstructed == (first, second)
    assert reconstructor.current_level_count == len(second.bids) + len(second.asks)
    assert reconstructor.audit().maximum_level_count == len(second.bids) + len(second.asks)
    assert delta["book_state_sha256"] == book_state_sha256(second)


def test_book_chain_uses_canonical_numeric_rendering() -> None:
    first = _book(
        Venue.LIGHTER,
        revision=1,
        received=1,
        bids=(("100.00", "5.00"),),
        asks=(("101.0", "3.0"),),
    )
    second = _book(
        Venue.LIGHTER,
        revision=2,
        received=2,
        bids=(("100", "5"),),
        asks=(("101", "3"),),
    )
    encoder = BookRevisionEncoder()
    full = encoder.encode(first, source_kind="SNAPSHOT")
    delta = encoder.encode(second, source_kind="DELTA")
    assert full["bids"] == ({"price": "100", "quantity": "5"},)
    assert full["asks"] == ({"price": "101", "quantity": "3"},)
    assert delta["bids"] == ()
    assert delta["asks"] == ()
    assert delta["book_state_sha256"] == full["book_state_sha256"]
    reconstructor = BookRevisionReconstructor()
    assert tuple(reconstructor.append(record) for record in (full, delta)) == (
        first,
        second,
    )


def test_book_chain_rejects_missing_or_ambiguous_predecessors() -> None:
    first = _book(Venue.RISEX, revision=1, received=1)
    second = _book(Venue.RISEX, revision=2, received=2, bids=(("99", "9"),))
    encoder = BookRevisionEncoder()
    with pytest.raises(BookRevisionChainError, match="MISSING_PREDECESSOR_SNAPSHOT"):
        encoder.encode(second, source_kind="DELTA")

    encoder.encode(first, source_kind="SNAPSHOT")
    skipped = _book(Venue.RISEX, revision=3, received=3)
    with pytest.raises(BookRevisionChainError, match="PREDECESSOR_REVISION_MISMATCH"):
        encoder.encode(skipped, source_kind="DELTA")

    full = BookRevisionEncoder().encode(first, source_kind="SNAPSHOT")
    valid_delta = BookRevisionEncoder()
    valid_delta.encode(first, source_kind="SNAPSHOT")
    delta = valid_delta.encode(second, source_kind="DELTA")
    reconstructor = BookRevisionReconstructor()
    reconstructor.append(full)
    corrupted = dict(delta)
    corrupted["predecessor_book_revision_id"] = "wrong-predecessor"
    with pytest.raises(BookRevisionChainError, match="PREDECESSOR_REFERENCE_MISMATCH"):
        reconstructor.append(corrupted)

    reconstructor.mark_gap(
        venue=Venue.RISEX,
        market="BTC",
        session="session",
        recovery=0,
    )
    with pytest.raises(BookRevisionChainError, match="MISSING_PREDECESSOR_CHAIN"):
        reconstructor.append(delta)

    with pytest.raises(BookRevisionChainError, match="MISSING_PREDECESSOR_CHAIN"):
        tuple(
            reconstruct_book_records(
                (
                    full,
                    {
                        "kind": "DATA_GAP",
                        "venue": "RISEX",
                        "canonical_market": "BTC",
                        "stream_session_id": "session",
                        "recovery_generation": 0,
                    },
                    delta,
                )
            )
        )


def test_book_chain_reanchors_session_recovery_and_survives_replay_restart() -> None:
    first = _book(Venue.LIGHTER, revision=1, received=1, session="old", recovery=0)
    fresh = _book(
        Venue.LIGHTER,
        revision=1,
        received=2,
        session="new",
        recovery=1,
        bids=(("100", "8"),),
    )
    next_book = _book(
        Venue.LIGHTER,
        revision=2,
        received=3,
        session="new",
        recovery=1,
        bids=(("100", "9"),),
    )
    encoder = BookRevisionEncoder()
    first_record = encoder.encode(first, source_kind="SNAPSHOT")
    fresh_record = encoder.encode(fresh, source_kind="SNAPSHOT")
    next_record = encoder.encode(next_book, source_kind="DELTA")
    assert fresh_record["book_encoding"] == "FULL"
    assert next_record["predecessor_book_revision_id"] == fresh.book_revision_id
    assert encoder.chain_count == 1

    replayed = tuple(
        reconstruct_book_records((first_record, fresh_record, next_record))
    )
    assert replayed == (first, fresh, next_book)
    restarted = tuple(
        reconstruct_book_records((first_record, fresh_record, next_record))
    )
    assert restarted == replayed

    encoder.reset()
    with pytest.raises(BookRevisionChainError, match="MISSING_PREDECESSOR_SNAPSHOT"):
        encoder.encode(next_book, source_kind="DELTA")


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    (
        ("book_chain_id", "wrong-chain", "BOOK_CHAIN_ID_MISMATCH"),
        ("book_state_sha256", "0" * 64, "BOOK_STATE_DIGEST_MISMATCH"),
        ("source_kind", "SNAPSHOT", "BOOK_ENCODING_SOURCE_MISMATCH"),
    ),
)
def test_book_chain_replay_rejects_corrupt_explicit_metadata(
    field: str,
    value: str,
    reason: str,
) -> None:
    first = _book(Venue.LIGHTER, revision=1, received=1)
    second = _book(Venue.LIGHTER, revision=2, received=2, bids=(("99", "9"),))
    encoder = BookRevisionEncoder()
    full = encoder.encode(first, source_kind="SNAPSHOT")
    delta = encoder.encode(second, source_kind="DELTA")
    corrupted = dict(delta)
    corrupted[field] = value
    reconstructor = BookRevisionReconstructor()
    reconstructor.append(full)
    with pytest.raises(BookRevisionChainError, match=reason):
        reconstructor.append(corrupted)


def test_book_chain_replay_rejects_non_changes_in_a_delta() -> None:
    first = _book(Venue.LIGHTER, revision=1, received=1)
    second = _book(Venue.LIGHTER, revision=2, received=2, bids=(("99", "9"),))
    encoder = BookRevisionEncoder()
    full = encoder.encode(first, source_kind="SNAPSHOT")
    delta = encoder.encode(second, source_kind="DELTA")
    reconstructor = BookRevisionReconstructor()
    reconstructor.append(full)

    noop = dict(delta)
    noop["bids"] = ({"price": "99", "quantity": "10"},)
    with pytest.raises(BookRevisionChainError, match="BOOK_CHANGE_IS_NOOP"):
        reconstructor.append(noop)

    missing_delete = dict(delta)
    missing_delete["bids"] = ({"price": "98", "quantity": "0"},)
    with pytest.raises(BookRevisionChainError, match="BOOK_DELETE_MISSING_LEVEL"):
        reconstructor.append(missing_delete)


def test_legacy_full_book_rows_remain_deterministic(tmp_path: Path) -> None:
    full = BookRevisionEncoder().encode(
        _book(Venue.LIGHTER, revision=1, received=1),
        source_kind="SNAPSHOT",
    )
    legacy = dict(full)
    legacy["source_kind"] = "DELTA"
    for field in (
        "book_encoding",
        "book_chain_id",
        "book_revision_id",
        "book_state_sha256",
        "predecessor_book_revision_id",
        "predecessor_book_revision",
    ):
        legacy.pop(field, None)
    legacy_horizon = {
        "kind": "HEDGE_HORIZON",
        "canonical_market": "BTC",
        "venue": "LIGHTER",
        "expected_stream_session_id": "session",
        "expected_recovery_generation": 0,
        "book_stream_session_id": "session",
        "book_recovery_generation": 0,
        "book_revision": 1,
        "observed_monotonic_ns": 2,
    }
    path = _store_records(tmp_path, (legacy, legacy_horizon))

    first = build_report(path)
    second = build_report(path)
    assert first == second
    assert first["book_record_count"] == 1
    assert first["full_book_snapshot_count"] == 1
    assert first["book_delta_count"] == 0
    assert tuple(reconstruct_book_records(iter_records(path)))


def test_report_rejects_corrupt_delta_predecessor(tmp_path: Path) -> None:
    first = _book(Venue.LIGHTER, revision=1, received=1)
    second = _book(Venue.LIGHTER, revision=2, received=2, bids=(("99", "9"),))
    encoder = BookRevisionEncoder()
    full = encoder.encode(first, source_kind="SNAPSHOT")
    delta = encoder.encode(second, source_kind="DELTA")
    path = _store_records(tmp_path, (full, delta))
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    next(row for row in rows if row.get("kind") == "BOOK" and row.get("book_revision") == 2)[
        "predecessor_book_revision_id"
    ] = "wrong"
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows) + "\n"
    )
    with pytest.raises(ValueError, match="PREDECESSOR_REFERENCE_MISMATCH"):
        build_report(path)


def test_report_preserves_displaced_horizon_book_witness(tmp_path: Path) -> None:
    expected = _book(Venue.LIGHTER, revision=1, received=1, session="old")
    displaced = _book(Venue.LIGHTER, revision=1, received=2, session="new", recovery=1)
    encoder = BookRevisionEncoder()
    expected_record = encoder.encode(expected, source_kind="SNAPSHOT")
    displaced_record = encoder.encode(displaced, source_kind="SNAPSHOT")
    horizon = {
        "kind": "HEDGE_HORIZON",
        "canonical_market": "BTC",
        "venue": "LIGHTER",
        "expected_stream_session_id": "old",
        "expected_recovery_generation": 0,
        "book_stream_session_id": "new",
        "book_recovery_generation": 1,
        "book_revision": 1,
        "book_revision_id": displaced.book_revision_id,
        "book_state_sha256": displaced_record["book_state_sha256"],
        "outcome": "HEDGE_SESSION_DISPLACED",
        "observed_monotonic_ns": 3,
    }
    path = _store_records(tmp_path, (expected_record, displaced_record, horizon))
    report = build_report(path)
    assert report["book_record_count"] == 2


def test_observer_quotes_and_horizons_bind_exact_book_revisions(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    clock = [200]
    observer = SpreadObserver(
        ShadowConfig(
            target_notionals_usd=(D("100"),),
            target_margins_bps=(D("1"),),
            ingress_queue_capacity=16,
            freshness_max_age_ns=2_000_000_000,
        ),
        (PAIR,),
        store,
        monotonic_ns=lambda: clock[0],
    )

    async def run() -> None:
        observer._replay_mode = True
        await observer.handle_book(
            FeedBookEvent(_book(Venue.RISEX, revision=1, received=100, session="risex"), PAIR, "SNAPSHOT", "fixture")
        )
        await observer.handle_book(
            FeedBookEvent(
                _book(
                    Venue.LIGHTER,
                    revision=1,
                    received=100,
                    session="lighter",
                    bids=(("100", "10"),),
                    asks=(("102", "10"),),
                ),
                PAIR,
                "SNAPSHOT",
                "fixture",
            )
        )
        await observer.handle_book(
            FeedBookEvent(
                _book(
                    Venue.LIGHTER,
                    revision=2,
                    received=150,
                    session="lighter",
                    bids=(("100", "11"),),
                    asks=(("102", "10"),),
                ),
                PAIR,
                "DELTA",
                "fixture",
            )
        )
        await observer.handle_trade(
            FeedTradeEvent(
                TradeEvidence(
                    trade_event_key="trade-1",
                    venue=Venue.RISEX,
                    canonical_market="BTC",
                    canonical_price=D("98"),
                    canonical_quantity=D("1"),
                    aggressor_side=Side.SELL,
                    received_utc=NOW,
                    received_monotonic_ns=200,
                    stream_session_id="risex",
                    recovery_generation=0,
                    exchange_event_utc=NOW,
                    exchange_event_time_provenance="fixture",
                ),
                PAIR,
                "fixture",
            )
        )
        observer._replay_mode = False
        await observer.flush_pending(force=True)
        await observer.append_terminal(
            {"kind": "RUN_STOP", "fatal_reason": None, "observed_monotonic_ns": 1_000}
        )
        await observer.close()

    import asyncio

    asyncio.run(run())
    store.close()
    rows = list(iter_records(store.path))
    quote_rows = [row for row in rows if row.get("kind") == "QUOTE"]
    assert quote_rows
    for row in quote_rows:
        assert row["risex_book_revision_id"] == "RISEX|BTC|risex|0|1"
        assert row["lighter_book_revision_id"] in {
            "LIGHTER|BTC|lighter|0|1",
            "LIGHTER|BTC|lighter|0|2",
        }
        assert len(row["risex_book_state_sha256"]) == 64
        assert len(row["lighter_book_state_sha256"]) == 64

    books = {
        book.book_revision_id: book
        for book in reconstruct_book_records(rows)
    }
    horizon_rows = [row for row in rows if row.get("kind") == "HEDGE_HORIZON"]
    assert horizon_rows
    for row in horizon_rows:
        assert row["book_revision_id"] in books
        assert row["book_revision_id"].startswith("LIGHTER|BTC|lighter|0|")
        assert row["book_state_sha256"] == book_state_sha256(books[row["book_revision_id"]])
    assert build_report(store.path)["book_delta_count"] == 1

    corrupted_quote_rows = [dict(row) for row in rows]
    next(row for row in corrupted_quote_rows if row.get("kind") == "QUOTE")[
        "lighter_book_revision_id"
    ] = "wrong"
    store.path.write_text(
        "\n".join(
            json.dumps(row, sort_keys=True, separators=(",", ":"))
            for row in corrupted_quote_rows
        )
        + "\n"
    )
    with pytest.raises(ValueError, match="REFERENCE_REVISION_ID_MISMATCH"):
        build_report(store.path)

    corrupted_horizon_rows = [dict(row) for row in rows]
    next(row for row in corrupted_horizon_rows if row.get("kind") == "HEDGE_HORIZON")[
        "book_revision_id"
    ] = "wrong"
    store.path.write_text(
        "\n".join(
            json.dumps(row, sort_keys=True, separators=(",", ":"))
            for row in corrupted_horizon_rows
        )
        + "\n"
    )
    with pytest.raises(ValueError, match="REFERENCE_REVISION_ID_MISMATCH"):
        build_report(store.path)


def test_three_market_deep_book_delta_serialization_is_lossless_and_bounded(tmp_path: Path) -> None:
    market_names = ("BTC", "ETH", "SOL")
    chains = tuple(
        (venue, market_name, f"{venue.value.lower()}-{market_name.lower()}")
        for market_name in market_names
        for venue in (Venue.RISEX, Venue.LIGHTER)
    )
    level_count_per_side = 950
    revisions_per_chain = 120
    encoder = BookRevisionEncoder()
    records: list[dict] = []
    started = time.perf_counter()
    for ordinal, (venue, market_name, session) in enumerate(chains, 1):
        bids = [BookLevel(D(10_000 - index), D("1")) for index in range(level_count_per_side)]
        asks = [BookLevel(D(20_000 + index), D("1")) for index in range(level_count_per_side)]
        initial = BookEvidence(
            venue=venue,
            canonical_market=market_name,
            bids=tuple(bids),
            asks=tuple(asks),
            received_monotonic_ns=ordinal,
            stream_session_id=session,
            recovery_generation=0,
            book_revision=1,
            sequence=1,
            checksum=1,
            received_utc=NOW,
        )
        records.append(encoder.encode(initial, source_kind="SNAPSHOT"))
        for revision in range(2, revisions_per_chain + 1):
            index = (revision * 17 + ordinal) % level_count_per_side
            bids[index] = BookLevel(bids[index].canonical_price, D("1") + D(revision % 9) / D("10"))
            current = BookEvidence(
                venue=venue,
                canonical_market=market_name,
                bids=tuple(bids),
                asks=tuple(asks),
                received_monotonic_ns=ordinal + revision * 10,
                stream_session_id=session,
                recovery_generation=0,
                book_revision=revision,
                sequence=revision,
                checksum=revision,
                received_utc=NOW,
            )
            records.append(encoder.encode(current, source_kind="DELTA"))

    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    store.append_batch(tuple(records), sync=False)
    store.append_batch(
        ({"kind": "RUN_STOP", "fatal_reason": None, "observed_monotonic_ns": 10_000_000},),
        sync=True,
    )
    elapsed = max(time.perf_counter() - started, 0.001)
    path = store.path
    store.close()

    rows = list(iter_records(path))
    books = [row for row in rows if row.get("kind") == "BOOK"]
    expected_books = len(chains) * revisions_per_chain
    assert len(books) == expected_books
    assert [row["record_index"] for row in rows] == list(range(len(rows)))
    assert rows[-1]["kind"] == "RUN_STOP"
    assert store_permissions(path) == 0o600
    assert store.byte_count < MAX_EVIDENCE_FILE_BYTES // 100
    assert len(books) / elapsed > 34

    revisions: dict[tuple[str, str, str], list[int]] = {}
    for row in books:
        key = (row["venue"], row["canonical_market"], row["stream_session_id"])
        revisions.setdefault(key, []).append(row["book_revision"])
        if row["book_encoding"] == "DELTA":
            assert len(row["bids"]) + len(row["asks"]) <= 1
    assert all(values == list(range(1, revisions_per_chain + 1)) for values in revisions.values())
    assert encoder.chain_count == len(chains)
    assert encoder.current_level_count == len(chains) * level_count_per_side * 2

    audit = audit_book_revisions(books)
    assert audit.book_count == expected_books
    assert audit.full_snapshot_count == len(chains)
    assert audit.delta_count == len(chains) * (revisions_per_chain - 1)
    assert audit.chain_count == len(chains)
    assert audit.maximum_level_count == level_count_per_side * 2
    assert audit.current_level_count == len(chains) * level_count_per_side * 2

    first_report = build_report(path)
    second_report = build_report(path)
    assert json.dumps(first_report, sort_keys=True, separators=(",", ":")) == json.dumps(
        second_report,
        sort_keys=True,
        separators=(",", ":"),
    )
