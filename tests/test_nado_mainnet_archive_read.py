from __future__ import annotations

import ast
from copy import deepcopy
import inspect
import json
from pathlib import Path
import stat
import sys

import pytest

from risex_farmer import nado_mainnet_archive_read as gate
from risex_farmer import nado_mainnet_onboarding as onboarding


ROOT = Path(__file__).parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "nado_mainnet_archive_read"


def _identity() -> onboarding.NadoPublicIdentity:
    return onboarding.NadoPublicIdentity(
        wallet_address=gate.EXPECTED_WALLET_ADDRESS,
        subaccount_name=gate.EXPECTED_SUBACCOUNT_NAME,
        subaccount=gate.EXPECTED_SUBACCOUNT,
    )


def _load(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_ROOT / name).read_text())


def _default_responses() -> dict[tuple[str, int | None], object]:
    return {
        ("orders", None): _load("orders_page_0.json"),
        ("matches", None): _load("matches_page_0.json"),
        ("events", None): _load("events_page_0.json"),
        ("interest_and_funding", None): _load("funding_page_0_zero.json"),
    }


class FixtureTransport:
    def __init__(
        self,
        responses: dict[tuple[str, int | None], object] | None = None,
        *,
        interruptions: dict[tuple[str, int | None], int] | None = None,
        status: dict[tuple[str, int | None], int] | None = None,
        final_urls: dict[tuple[str, int | None], str] | None = None,
    ) -> None:
        self.responses = _default_responses() if responses is None else deepcopy(responses)
        self.interruptions = {} if interruptions is None else dict(interruptions)
        self.status = {} if status is None else dict(status)
        self.final_urls = {} if final_urls is None else dict(final_urls)
        self.calls: list[gate.ArchiveRequest] = []

    @staticmethod
    def _key(request: gate.ArchiveRequest) -> tuple[str, int | None]:
        params = request.body[request.query_type]
        raw_cursor = params.get("idx", params.get("max_idx"))
        return request.query_type, None if raw_cursor is None else int(raw_cursor)

    async def post(self, request: gate.ArchiveRequest) -> gate.ArchiveReply:
        self.calls.append(request)
        key = self._key(request)
        if self.interruptions.get(key, 0):
            self.interruptions[key] -= 1
            raise gate.TransportInterruption()
        body = self.responses.get(key)
        if body is None:
            raise AssertionError(f"unexpected fixture key: {key}")
        return gate.ArchiveReply(
            self.status.get(key, 200),
            self.final_urls.get(key, request.url),
            deepcopy(body),
        )


async def _run(
    tmp_path: Path,
    transport: FixtureTransport,
    *,
    invocation_id: str = "archive-fixture-1",
    identity: object | None = None,
) -> gate.ReadResult:
    return await gate.run_fixture(
        store_path=tmp_path / "runs.sqlite3",
        invocation_id=invocation_id,
        identity=_identity() if identity is None else identity,
        transport=transport,
        clock_ms=iter(range(1, 50_000)).__next__,
    )


@pytest.mark.asyncio
async def test_complete_archive_history_and_zero_funding_are_observed_without_authority(
    tmp_path: Path,
) -> None:
    transport = FixtureTransport()
    result = await _run(tmp_path, transport)

    assert result.status == gate.STATUS_BLOCKED
    assert result.reason == "ARCHIVE_READ_COMPLETE_NO_MAINNET_WRITE_AUTHORITY"
    assert result.read_complete is True
    assert result.ready is False
    assert result.write_ready is False
    assert result.mainnet_write_authority == gate.NO_MAINNET_WRITE_AUTHORITY
    assert result.history["orders"]["count"] == 1
    assert result.history["orders"]["high_water_submission_idx"] == 100
    assert result.history["matches"]["count"] == 1
    assert result.history["matches"]["high_water_submission_idx"] == 100
    assert result.history["events"]["count"] == 1
    assert result.history["events"]["high_water_submission_idx"] == 100
    assert result.cross_agreement["status"] == "AGREE_EXACT_ACCOUNT_HISTORY"
    assert result.funding["payment_status"] == "ACTUAL_ZERO"
    assert result.funding["funding_payment_count"] == 0
    assert "ALL_MAINNET_WRITES_FORBIDDEN" in result.blockers
    assert "PRIVATE_SECRET_FIXTURE" not in result.evidence()

    assert len(transport.calls) == 4
    for request in transport.calls:
        assert request.method == "POST"
        assert request.url == gate.MAINNET_ARCHIVE_URL
        assert request.attempt == 1
        assert request.body[request.query_type]  # Every official query is account-bound.
        assert "Authorization" not in json.dumps(request.body)
        assert "signature" not in request.body
        assert "txns" not in request.body
        assert "pre_state" not in request.body
    assert transport.calls[0].body == {
        "orders": {
            "subaccounts": [gate.EXPECTED_SUBACCOUNT],
            "limit": gate.ARCHIVE_PAGE_LIMIT + 1,
        }
    }
    assert transport.calls[1].body == {
        "matches": {
            "subaccounts": [gate.EXPECTED_SUBACCOUNT],
            "limit": gate.ARCHIVE_PAGE_LIMIT + 1,
        }
    }
    assert transport.calls[2].body == {
        "events": {
            "subaccounts": [gate.EXPECTED_SUBACCOUNT],
            "desc": True,
            "limit": {"txs": gate.ARCHIVE_PAGE_LIMIT + 1},
        }
    }
    assert transport.calls[3].body == {
        "interest_and_funding": {
            "subaccount": gate.EXPECTED_SUBACCOUNT,
            "product_ids": list(range(94)),
            "limit": gate.ARCHIVE_PAGE_LIMIT,
        }
    }


@pytest.mark.asyncio
async def test_archive_cross_margin_order_accepts_official_null_closed_margin(
    tmp_path: Path,
) -> None:
    responses = _default_responses()
    orders = deepcopy(responses[("orders", None)])
    assert isinstance(orders, dict)
    assert isinstance(orders["orders"], list)
    assert isinstance(orders["orders"][0], dict)
    orders["orders"][0]["isolated"] = False
    orders["orders"][0]["closed_margin"] = None
    responses[("orders", None)] = orders

    result = await _run(tmp_path, FixtureTransport(responses))

    assert result.read_complete is True
    assert result.status == gate.STATUS_BLOCKED
    assert result.reason == "ARCHIVE_READ_COMPLETE_NO_MAINNET_WRITE_AUTHORITY"
    assert result.history["orders"]["count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "closed_margin",
    ["", " 0", "0 ", "01", "+0", "-00", "0.0", 0, False, [], {}],
)
async def test_archive_order_rejects_malformed_non_null_closed_margin(
    tmp_path: Path,
    closed_margin: object,
) -> None:
    responses = _default_responses()
    orders = deepcopy(responses[("orders", None)])
    assert isinstance(orders, dict)
    assert isinstance(orders["orders"], list)
    assert isinstance(orders["orders"][0], dict)
    orders["orders"][0]["closed_margin"] = closed_margin
    responses[("orders", None)] = orders

    result = await _run(tmp_path, FixtureTransport(responses))

    assert result.read_complete is False
    assert result.failure_class == "SCHEMA"
    assert result.reason == "INTEGER_TEXT_INVALID_ORDER_0_CLOSED_MARGIN"


@pytest.mark.asyncio
async def test_positive_negative_funding_is_distinguished_from_actual_zero(
    tmp_path: Path,
) -> None:
    responses = _default_responses()
    responses[("interest_and_funding", None)] = _load("funding_page_0_nonzero.json")
    result = await _run(tmp_path, FixtureTransport(responses))

    assert result.read_complete is True
    assert result.funding["payment_status"] == "OBSERVED_NONZERO"
    assert result.funding["funding_payment_count"] == 2
    assert result.funding["high_water_idx"] == 9
    assert result.funding["positive_count"] == 1
    assert result.funding["negative_count"] == 1
    assert result.funding["zero_count"] == 0
    assert result.funding["interest_payment_count"] == 1
    assert "-50" not in result.evidence()
    assert "10000000000000000000" not in result.evidence()


@pytest.mark.asyncio
async def test_missing_funding_is_unknown_and_never_fabricated_as_zero(
    tmp_path: Path,
) -> None:
    responses = _default_responses()
    missing = _load("funding_page_0_zero.json")
    del missing["funding_payments"]
    responses[("interest_and_funding", None)] = missing
    result = await _run(tmp_path, FixtureTransport(responses))

    assert result.read_complete is False
    assert result.failure_class == "SCHEMA"
    assert result.reason == "FIELD_MISSING_FUNDING_PAYMENTS"
    assert result.funding["payment_status"] == "UNKNOWN"
    assert result.funding["historical_payments"] == "UNKNOWN"
    assert result.funding["funding_payment_count"] == 0
    assert result.funding["payment_status"] != "ACTUAL_ZERO"


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["orders", "matches", "events"])
async def test_archive_account_identity_mismatch_is_terminal(
    tmp_path: Path,
    surface: str,
) -> None:
    responses = _default_responses()
    bad = responses[(surface, None)]
    assert isinstance(bad, dict)
    bad = deepcopy(bad)
    if surface == "orders":
        assert isinstance(bad["orders"], list)
        assert isinstance(bad["orders"][0], dict)
        bad["orders"][0]["subaccount"] = "0x" + "00" * 32
    elif surface == "matches":
        assert isinstance(bad["matches"], list)
        assert isinstance(bad["matches"][0], dict)
        assert isinstance(bad["matches"][0]["order"], dict)
        bad["matches"][0]["order"]["sender"] = "0x" + "00" * 32
    else:
        assert isinstance(bad["events"], list)
        assert isinstance(bad["events"][0], dict)
        bad["events"][0]["subaccount"] = "0x" + "00" * 32
    responses[(surface, None)] = bad
    result = await _run(tmp_path, FixtureTransport(responses))

    assert result.read_complete is False
    assert result.failure_class == "IDENTITY"
    assert "EXACT_SUBACCOUNT_MISMATCH" in result.reason


@pytest.mark.asyncio
async def test_event_match_cross_agreement_is_required(tmp_path: Path) -> None:
    responses = _default_responses()
    events = deepcopy(responses[("events", None)])
    assert isinstance(events, dict)
    assert isinstance(events["events"], list)
    assert isinstance(events["events"][0], dict)
    assert isinstance(events["txs"], list)
    assert isinstance(events["txs"][0], dict)
    events["events"][0]["submission_idx"] = "101"
    events["txs"][0]["submission_idx"] = "101"
    responses[("events", None)] = events
    result = await _run(tmp_path, FixtureTransport(responses))

    assert result.read_complete is False
    assert result.reason == "HISTORY_EVENT_MATCH_AGREEMENT_MISMATCH"
    assert result.failure_class == "SCHEMA"


def _order_row(index: int) -> dict[str, object]:
    row = deepcopy(_load("orders_page_0.json")["orders"][0])
    assert isinstance(row, dict)
    row["digest"] = "0x" + f"{index:064x}"
    row["submission_idx"] = str(index)
    row["last_fill_submission_idx"] = str(index)
    return row


@pytest.mark.asyncio
async def test_archive_idx_pagination_is_bounded_and_uses_first_omitted_row(
    tmp_path: Path,
) -> None:
    responses = _default_responses()
    responses[("orders", None)] = {"orders": [_order_row(index) for index in range(101, 0, -1)]}
    responses[("orders", 1)] = {"orders": []}
    transport = FixtureTransport(responses)
    result = await _run(tmp_path, transport)

    order_calls = [call for call in transport.calls if call.query_type == "orders"]
    assert [call.attempt for call in order_calls] == [1, 1]
    assert "idx" not in order_calls[0].body["orders"]
    assert order_calls[1].body["orders"]["idx"] == "1"
    assert result.read_complete is False
    assert result.reason == "HISTORY_CROSS_AGREEMENT_MISMATCH"
    assert result.history["orders"]["count"] == 100


@pytest.mark.asyncio
async def test_reordered_and_duplicate_archive_pages_fail_closed(tmp_path: Path) -> None:
    responses = _default_responses()
    responses[("orders", None)] = {"orders": [_order_row(1), _order_row(2)]}
    result = await _run(tmp_path, FixtureTransport(responses))
    assert result.reason == "ARCHIVE_PAGE_REORDERED"
    assert result.failure_class == "SCHEMA"

    responses = _default_responses()
    duplicate = _order_row(2)
    responses[("orders", None)] = {"orders": [duplicate, deepcopy(duplicate)]}
    duplicate_path = tmp_path / "duplicate"
    duplicate_path.mkdir()
    result = await _run(duplicate_path, FixtureTransport(responses))
    assert result.reason == "ORDER_DIGEST_REPEATED"
    assert result.failure_class == "SCHEMA"


def test_replayed_page_is_rejected_by_durable_seen_identity() -> None:
    rows = [_order_row(2), _order_row(1)]
    seen: dict[object, dict[str, object]] = {}
    first, cursor, boundary = gate._page_rows(
        raw_rows=rows,
        cursor=None,
        boundary=None,
        seen=seen,
        key=lambda row: row["digest"],
        index=lambda row: row["submission_idx"],
    )
    assert first == tuple(rows)
    assert cursor is None
    assert boundary is None
    with pytest.raises(gate.GateFailure, match="ARCHIVE_PAGE_REPEATED"):
        gate._page_rows(
            raw_rows=rows,
            cursor=None,
            boundary=None,
            seen=seen,
            key=lambda row: row["digest"],
            index=lambda row: row["submission_idx"],
        )


def _payment(index: int, amount: str = "1") -> dict[str, object]:
    return {
        "product_id": 4,
        "idx": str(index),
        "timestamp": "1700000000000",
        "amount": amount,
        "balance_amount": amount,
        "rate_x18": "1",
        "oracle_price_x18": "100000000000000000000",
    }


@pytest.mark.asyncio
async def test_funding_next_idx_pagination_and_terminal_null_are_required(
    tmp_path: Path,
) -> None:
    responses = _default_responses()
    responses[("interest_and_funding", None)] = {
        "interest_payments": [],
        "funding_payments": [_payment(10)],
        "next_idx": "5",
    }
    responses[("interest_and_funding", 5)] = {
        "interest_payments": [],
        "funding_payments": [],
        "next_idx": None,
    }
    transport = FixtureTransport(responses)
    result = await _run(tmp_path, transport)

    funding_calls = [
        call for call in transport.calls if call.query_type == "interest_and_funding"
    ]
    assert len(funding_calls) == 2
    assert "max_idx" not in funding_calls[0].body["interest_and_funding"]
    assert funding_calls[1].body["interest_and_funding"]["max_idx"] == "5"
    assert result.read_complete is True
    assert result.funding["funding_payment_count"] == 1
    assert result.funding["payment_status"] == "OBSERVED_NONZERO"
    assert result.funding["pages"] == 2


@pytest.mark.asyncio
async def test_transport_interruption_has_only_one_fresh_retry(tmp_path: Path) -> None:
    transport = FixtureTransport(interruptions={("orders", None): 1})
    result = await _run(tmp_path, transport)

    order_calls = [call for call in transport.calls if call.query_type == "orders"]
    assert [call.attempt for call in order_calls] == [1, 2]
    assert result.read_complete is True
    assert result.counters["archive_order_history_page_0_attempts"] == 2
    assert result.counters["archive_order_history_page_0_completions"] == 1


@pytest.mark.asyncio
async def test_second_transport_interruption_is_terminal_and_does_not_advance(
    tmp_path: Path,
) -> None:
    transport = FixtureTransport(interruptions={("orders", None): 2})
    result = await _run(tmp_path, transport)

    assert result.reason == "TRANSPORT_RETRY_EXHAUSTED"
    assert result.failure_class == "TRANSPORT"
    assert result.read_complete is False
    assert [call.query_type for call in transport.calls] == ["orders", "orders"]
    assert all(call.attempt in {1, 2} for call in transport.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "reason", "failure_class"),
    [
        (401, "ARCHIVE_QUERY_AUTH_REJECTED", "AUTH"),
        (500, "HTTP_STATUS_UNACCEPTED", "HTTP"),
        (302, "REDIRECT_FORBIDDEN", "SAFETY"),
    ],
)
async def test_http_and_auth_failures_are_not_retried(
    tmp_path: Path,
    status: int,
    reason: str,
    failure_class: str,
) -> None:
    transport = FixtureTransport(status={("orders", None): status})
    result = await _run(tmp_path, transport)

    assert result.reason == reason
    assert result.failure_class == failure_class
    assert len(transport.calls) == 1
    assert transport.calls[0].attempt == 1


@pytest.mark.asyncio
async def test_redirect_is_forbidden_and_error_payload_is_redacted(tmp_path: Path) -> None:
    responses = _default_responses()
    responses[("orders", None)] = {
        "status": "failure",
        "error": "PRIVATE_SECRET_FIXTURE",
    }
    transport = FixtureTransport(
        responses,
        final_urls={("orders", None): "https://archive.prod.nado.xyz/v2"},
    )
    result = await _run(tmp_path, transport)

    assert result.reason == "REDIRECT_FORBIDDEN"
    assert result.failure_class == "SAFETY"
    assert "PRIVATE_SECRET_FIXTURE" not in result.evidence()


def test_unsafe_request_shapes_are_rejected_before_transport() -> None:
    unsafe = gate.ArchiveRequest(
        "orders",
        {
            "orders": {
                "subaccounts": [gate.EXPECTED_SUBACCOUNT],
                "limit": gate.ARCHIVE_PAGE_LIMIT + 1,
                "txns": True,
            }
        },
        1,
    )
    with pytest.raises(gate.GateFailure, match="ARCHIVE_BODY_UNSAFE_FIELD"):
        gate._validate_archive_request(unsafe)

    wrong_scope = gate.ArchiveRequest(
        "interest_and_funding",
        {
            "interest_and_funding": {
                "subaccount": gate.EXPECTED_SUBACCOUNT,
                "product_ids": [],
                "limit": gate.ARCHIVE_PAGE_LIMIT,
            }
        },
        1,
    )
    with pytest.raises(gate.GateFailure, match="ARCHIVE_PRODUCT_SCOPE_INVALID"):
        gate._validate_archive_request(wrong_scope)

    valid = gate._expected_body("orders", None)

    class WrongHostRequest(gate.ArchiveRequest):
        @property
        def url(self) -> str:
            return "https://archive.prod.nado.xyz/v2"

    class WrongMethodRequest(gate.ArchiveRequest):
        @property
        def method(self) -> str:
            return "GET"

    with pytest.raises(gate.GateFailure, match="ARCHIVE_TRANSPORT_REQUEST_INVALID"):
        gate._validate_archive_request(WrongHostRequest("orders", valid, 1))
    with pytest.raises(gate.GateFailure, match="ARCHIVE_TRANSPORT_REQUEST_INVALID"):
        gate._validate_archive_request(WrongMethodRequest("orders", valid, 1))


@pytest.mark.asyncio
async def test_durable_terminal_result_is_reused_without_network(tmp_path: Path) -> None:
    first_transport = FixtureTransport()
    first = await _run(tmp_path, first_transport, invocation_id="same-invocation")
    second_transport = FixtureTransport()
    second = await _run(tmp_path, second_transport, invocation_id="same-invocation")

    assert second.evidence() == first.evidence()
    assert second.phase == "TERMINAL"
    assert second_transport.calls == []
    assert stat.S_IMODE((tmp_path / "runs.sqlite3").stat().st_mode) == gate.RUN_STORE_MODE


def test_running_invocation_is_terminalized_on_restart_without_network(tmp_path: Path) -> None:
    path = tmp_path / "restart.sqlite3"
    first_store = gate.RunStore(path, "restart-invocation")
    assert first_store.claim() is None

    restarted = gate.RunStore(path, "restart-invocation")
    result = restarted.claim()
    assert result is not None
    assert result.reason == "INTERRUPTED_RUNNING_INVOCATION"
    assert result.failure_class == "SAFETY"
    assert result.phase == "TERMINAL"
    assert result.read_complete is False
    assert result.mainnet_write_authority == gate.NO_MAINNET_WRITE_AUTHORITY


def test_run_directory_and_store_reject_symlink_targets(tmp_path: Path) -> None:
    directory = tmp_path / "protected"
    gate._ensure_run_directory(directory)
    assert stat.S_IMODE(directory.stat().st_mode) == gate.RUN_DIRECTORY_MODE

    real = tmp_path / "real.sqlite3"
    real.write_bytes(b"")
    link = tmp_path / "link.sqlite3"
    link.symlink_to(real)
    with pytest.raises(gate.StoreFailure, match="STORE_INVALID"):
        gate.RunStore(link, "symlink-invocation")


def test_static_archive_transport_and_cli_have_no_write_or_override_surface() -> None:
    transport_source = inspect.getsource(gate.MainnetArchiveTransport)
    lowered = transport_source.lower()
    for forbidden in (
        "gateway",
        "trigger",
        "execute",
        "sign",
        "private",
        "withdraw",
        "transfer",
        "ws_connect",
    ):
        assert forbidden not in lowered
    transport_tree = ast.parse(transport_source)
    called_attrs = {
        node.attr
        for node in ast.walk(transport_tree)
        if isinstance(node, ast.Attribute)
    }
    assert not called_attrs & {"get", "put", "patch", "delete", "ws_connect"}
    assert called_attrs & {"post"}

    module_source = inspect.getsource(gate)
    assert "nado_mainnet_unsigned_read" not in module_source
    assert "os.environ" not in module_source
    assert "getenv" not in module_source
    module_tree = ast.parse(module_source)
    main_nodes = [
        node
        for node in ast.walk(module_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "main"
    ]
    assert len(main_nodes) == 1
    assert not main_nodes[0].args.args


def test_cli_rejects_arguments_without_starting_the_archive_runner(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "argv", ["nado-mainnet-archive-read", "--url"])
    assert gate.main() == 2
    output = capsys.readouterr().out
    assert "ARGUMENTS_FORBIDDEN" in output
    assert gate.NO_MAINNET_WRITE_AUTHORITY in output
