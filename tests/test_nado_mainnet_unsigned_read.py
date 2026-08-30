from __future__ import annotations

import ast
from copy import deepcopy
import inspect
import json
from pathlib import Path
import sys

import pytest

from risex_farmer import nado_mainnet_onboarding as onboarding
from risex_farmer import nado_mainnet_unsigned_read as gate


ROOT = Path(__file__).parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "nado_mainnet_unsigned_read"


def _identity() -> onboarding.NadoPublicIdentity:
    return onboarding.NadoPublicIdentity(
        wallet_address=gate.EXPECTED_WALLET_ADDRESS,
        subaccount_name=gate.EXPECTED_SUBACCOUNT_NAME,
        subaccount=gate.EXPECTED_SUBACCOUNT,
    )


def _load(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_ROOT / name).read_text())


def _fixtures() -> dict[tuple[str, str | None], dict[str, object]]:
    result: dict[tuple[str, str | None], dict[str, object]] = {
        ("contracts", None): _load("contracts.json"),
        ("all_products", None): _load("all_products.json"),
        ("subaccount_info", None): _load("subaccount_info_zero.json"),
        ("fee_rates", None): _load("fee_rates.json"),
        ("linked_signer", None): _load("linked_signer.json"),
        ("isolated_positions", None): _load("isolated_positions_zero.json"),
    }
    for product_id in ("1", "2"):
        result[("subaccount_orders", product_id)] = _load(
            f"subaccount_orders_{product_id}.json"
        )
    return result


class FixtureTransport:
    def __init__(
        self,
        fixtures: dict[tuple[str, str | None], dict[str, object]] | None = None,
        *,
        interruptions: dict[tuple[str, str | None], int] | None = None,
    ) -> None:
        self.fixtures = _fixtures() if fixtures is None else fixtures
        self.interruptions = {} if interruptions is None else dict(interruptions)
        self.calls: list[gate.GetRequest] = []

    async def get(self, request: gate.GetRequest) -> gate.GetReply:
        self.calls.append(request)
        product_id = dict(request.params).get("product_id")
        key = (request.query_type, product_id)
        if self.interruptions.get(key, 0):
            self.interruptions[key] -= 1
            raise gate.TransportInterruption()
        if key not in self.fixtures:
            raise AssertionError(f"unexpected fixture key: {key}")
        return gate.GetReply(
            status=200,
            final_url=request.url,
            body=deepcopy(self.fixtures[key]),
        )


class AdverseReplyTransport(FixtureTransport):
    def __init__(
        self,
        *,
        status: int | None = None,
        final_url: str | None = None,
        body: object | None = None,
    ) -> None:
        super().__init__()
        self.status_override = status
        self.final_url_override = final_url
        self.body_override = body

    async def get(self, request: gate.GetRequest) -> gate.GetReply:
        self.calls.append(request)
        return gate.GetReply(
            status=200 if self.status_override is None else self.status_override,
            final_url=request.url if self.final_url_override is None else self.final_url_override,
            body=(
                _load("contracts.json")
                if self.body_override is None
                else deepcopy(self.body_override)
            ),
        )


async def _run(
    tmp_path: Path,
    transport: FixtureTransport,
    *,
    invocation_id: str = "fixture-zero-1",
) -> gate.ReadResult:
    return await gate.run_fixture(
        store_path=tmp_path / "runs.sqlite3",
        invocation_id=invocation_id,
        identity=_identity(),
        transport=transport,
        clock_ms=iter(range(1, 20_000)).__next__,
    )


@pytest.mark.asyncio
async def test_complete_zero_vectors_are_observed_as_exact_flat_and_not_fabricated_async(
    tmp_path: Path,
) -> None:
    transport = FixtureTransport()
    result = await _run(tmp_path, transport)
    assert result.status == gate.STATUS_BLOCKED
    assert result.reason == "UNSIGNED_GET_COMPLETE_FUNDING_BLOCKED"
    assert result.read_complete is True
    assert result.mainnet_write_authority == gate.NO_MAINNET_WRITE_AUTHORITY
    assert result.write_ready is False
    assert result.account is not None
    assert result.account["flatness"]["status"] == "EXACT_FLAT"
    assert result.account["collateral"]["quote_balance_x18"] == "0"
    assert result.account["orders"]["open_total"] == 0
    assert result.account["regular_positions"]["count"] == 0
    assert result.account["isolated_positions"]["count"] == 0
    assert "ARCHIVE_FUNDING_PAYMENTS_POST_ONLY" in result.blockers
    assert "PRIVATE_SECRET_FIXTURE" not in result.evidence()
    assert all(request.method == "GET" for request in transport.calls)
    assert all(request.body is None for request in transport.calls)
    assert all(request.url.startswith(gate.MAINNET_QUERY_URL + "?") for request in transport.calls)
    assert all(
        "txns" not in dict(request.params)
        and "pre_state" not in dict(request.params)
        for request in transport.calls
    )
    assert {
        dict(request.params).get("product_id")
        for request in transport.calls
        if request.query_type == "subaccount_orders"
    } == {"1", "2"}
    assert not any(
        dict(request.params).get("product_id") in {"0", "11"}
        for request in transport.calls
        if request.query_type == "subaccount_orders"
    )
    assert all(request.attempt == 1 for request in transport.calls)


@pytest.mark.asyncio
async def test_incomplete_balance_vector_blocks_before_flatness_claim(tmp_path: Path) -> None:
    fixtures = _fixtures()
    info = fixtures[("subaccount_info", None)]
    assert isinstance(info["data"], dict)
    info_data = info["data"]
    assert isinstance(info_data, dict)
    info_data["spot_balances"] = [info_data["spot_balances"][0]]
    info_data["spot_count"] = 1
    transport = FixtureTransport(fixtures)
    result = await _run(tmp_path, transport)
    assert result.failure_class == "SCHEMA"
    assert result.reason == "SPOT_BALANCE_COVERAGE_INCOMPLETE"
    assert result.read_complete is False
    assert result.account is None
    assert "EXACT_FLAT" not in result.evidence()


@pytest.mark.asyncio
async def test_health_contribution_max_product_coverage_and_unused_slots_are_strict(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures()
    info = fixtures[("subaccount_info", None)]
    assert isinstance(info["data"], dict)
    info_data = info["data"]
    assert isinstance(info_data, dict)
    info_data["health_contributions"] = info_data["health_contributions"][:-1]
    result = await _run(tmp_path, FixtureTransport(fixtures))
    assert result.failure_class == "SCHEMA"
    assert result.reason == "HEALTH_CONTRIBUTION_COVERAGE_INCOMPLETE"

    with pytest.raises(gate.GateFailure, match="UNUSED_HEALTH_CONTRIBUTION_NONZERO"):
        gate._decode_health_contributions(
            [["0", "0", "0"], ["1", "0", "0"], ["0", "0", "0"]],
            frozenset({0, 2}),
        )


@pytest.mark.asyncio
async def test_nonquote_spot_balance_is_explicitly_blocked(tmp_path: Path) -> None:
    fixtures = _fixtures()
    info = fixtures[("subaccount_info", None)]
    assert isinstance(info["data"], dict)
    info_data = info["data"]
    assert isinstance(info_data, dict)
    spot_rows = info_data["spot_balances"]
    assert isinstance(spot_rows, list)
    assert isinstance(spot_rows[1], dict)
    assert isinstance(spot_rows[1]["balance"], dict)
    spot_rows[1]["balance"]["amount"] = "10"
    result = await _run(tmp_path, FixtureTransport(fixtures))
    assert result.read_complete is True
    assert result.account is not None
    assert result.account["flatness"]["status"] == "NOT_FLAT"
    assert result.account["flatness"]["spot_nonquote_nonzero_count"] == 1
    assert "NONQUOTE_SPOT_BALANCE_PRESENT" in result.blockers


@pytest.mark.asyncio
async def test_perp_v_quote_residue_blocks_flatness_even_with_zero_amount(
    tmp_path: Path,
) -> None:
    fixtures = _fixtures()
    info = fixtures[("subaccount_info", None)]
    assert isinstance(info["data"], dict)
    info_data = info["data"]
    assert isinstance(info_data, dict)
    perp_rows = info_data["perp_balances"]
    assert isinstance(perp_rows, list)
    assert isinstance(perp_rows[0], dict)
    assert isinstance(perp_rows[0]["balance"], dict)
    perp_rows[0]["balance"]["v_quote_balance"] = "1"
    result = await _run(tmp_path, FixtureTransport(fixtures))
    assert result.read_complete is True
    assert result.account is not None
    assert result.account["regular_positions"]["count"] == 1
    assert result.account["flatness"]["perp_residue_count"] == 1
    assert result.account["flatness"]["status"] == "NOT_FLAT"
    assert "PERP_V_QUOTE_OR_POSITION_RESIDUE_PRESENT" in result.blockers
    assert "POSITIONS_NOT_FLAT" in result.blockers


@pytest.mark.asyncio
async def test_isolated_base_v_quote_residue_blocks_flatness(tmp_path: Path) -> None:
    fixtures = _fixtures()
    isolated = fixtures[("isolated_positions", None)]
    isolated["data"] = {
        "isolated_positions": [
            {
                "subaccount": gate.EXPECTED_WALLET_ADDRESS
                + "000000000000000000000000",
                "quote_balance": {"product_id": 0, "balance": {"amount": "0"}},
                "base_balance": {
                    "product_id": 2,
                    "balance": {
                        "amount": "0",
                        "v_quote_balance": "1",
                        "last_cumulative_funding_x18": "0",
                    },
                },
                "quote_product": {"product_id": 0},
                "base_product": {"product_id": 2},
                "healths": [
                    {"assets": "0", "liabilities": "0", "health": "0"},
                    {"assets": "0", "liabilities": "0", "health": "0"},
                    {"assets": "0", "liabilities": "0", "health": "0"},
                ],
                "quote_healths": [],
                "base_healths": [],
            }
        ]
    }
    result = await _run(tmp_path, FixtureTransport(fixtures))
    assert result.read_complete is True
    assert result.account is not None
    assert result.account["flatness"]["isolated_residue_count"] == 1
    assert result.account["flatness"]["status"] == "NOT_FLAT"
    assert "ISOLATED_POSITION_RESIDUE_PRESENT" in result.blockers


@pytest.mark.asyncio
async def test_allowed_transport_retry_is_exactly_one_retry(tmp_path: Path) -> None:
    transport = FixtureTransport(
        interruptions={("all_products", None): 1}
    )
    result = await _run(tmp_path, transport)
    assert result.read_complete is True
    all_products_calls = [
        request for request in transport.calls if request.query_type == "all_products"
    ]
    assert [request.attempt for request in all_products_calls] == [1, 2]
    assert result.counters["all_products_attempts"] == 2
    assert result.counters["all_products_completions"] == 1


@pytest.mark.asyncio
async def test_second_transport_failure_is_terminal_and_does_not_continue(
    tmp_path: Path,
) -> None:
    transport = FixtureTransport(
        interruptions={("subaccount_info", None): 2}
    )
    result = await _run(tmp_path, transport)
    assert result.failure_class == "TRANSPORT"
    assert result.reason == "TRANSPORT_RETRY_EXHAUSTED"
    assert result.read_complete is False
    assert [request.attempt for request in transport.calls if request.query_type == "subaccount_info"] == [1, 2]
    assert not any(request.query_type == "fee_rates" for request in transport.calls)
    assert result.queries[-1]["reason"] == "TRANSPORT_RETRY_EXHAUSTED"
    assert result.queries[-1]["attempts"] == 2


@pytest.mark.asyncio
async def test_identity_mismatch_is_terminal_without_retry(tmp_path: Path) -> None:
    fixtures = _fixtures()
    info = fixtures[("subaccount_info", None)]
    assert isinstance(info["data"], dict)
    info_data = info["data"]
    assert isinstance(info_data, dict)
    info_data["subaccount"] = "0x" + "11" * 32
    transport = FixtureTransport(fixtures)
    result = await _run(tmp_path, transport)
    assert result.failure_class == "IDENTITY"
    assert result.reason == "SUBACCOUNT_RESPONSE_IDENTITY_MISMATCH"
    assert [request.attempt for request in transport.calls if request.query_type == "subaccount_info"] == [1]
    assert not any(request.query_type == "fee_rates" for request in transport.calls)


@pytest.mark.asyncio
async def test_failure_payload_and_unknown_fields_are_not_echoed(tmp_path: Path) -> None:
    fixtures = _fixtures()
    fixtures[("contracts", None)] = {
        "status": "failure",
        "error": "PRIVATE_SECRET_FIXTURE_SHOULD_NOT_APPEAR",
        "error_code": 9001,
        "request_type": "query_contracts",
        "irrelevant": {"secret": "PRIVATE_SECRET_FIXTURE"},
    }
    result = await _run(tmp_path, FixtureTransport(fixtures))
    assert result.failure_class == "HTTP"
    assert result.reason == "NADO_QUERY_FAILURE"
    assert "PRIVATE_SECRET_FIXTURE" not in result.evidence()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "failure_class", "reason"),
    [
        (400, "HTTP", "HTTP_STATUS_UNACCEPTED"),
        (401, "AUTH", "UNSIGNED_QUERY_AUTH_REJECTED"),
        (403, "AUTH", "UNSIGNED_QUERY_AUTH_REJECTED"),
        (500, "HTTP", "HTTP_STATUS_UNACCEPTED"),
    ],
)
async def test_http_and_auth_failures_are_terminal_without_retry(
    tmp_path: Path, status: int, failure_class: str, reason: str
) -> None:
    transport = AdverseReplyTransport(status=status)
    result = await _run(tmp_path, transport)
    assert result.failure_class == failure_class
    assert result.reason == reason
    assert result.read_complete is False
    assert len(transport.calls) == 1
    assert result.counters["contracts_attempts"] == 1
    assert "contracts_completions" not in result.counters


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "failure_class", "reason"),
    [
        (
            {
                "status": "success",
                "data": {"chain_id": "57073", "endpoint_addr": "0x" + "11" * 20},
                "request_type": "query_wrong",
            },
            "SCHEMA",
            "RESPONSE_REQUEST_TYPE_MISMATCH",
        ),
        (
            {
                "status": "success",
                "data": {"chain_id": "57073"},
                "request_type": "query_contracts",
            },
            "SCHEMA",
            "FIELD_MISSING_CONTRACTS_ENDPOINT",
        ),
    ],
)
async def test_schema_failures_are_terminal_without_retry(
    tmp_path: Path,
    body: dict[str, object],
    failure_class: str,
    reason: str,
) -> None:
    transport = AdverseReplyTransport(body=body)
    result = await _run(tmp_path, transport)
    assert result.failure_class == failure_class
    assert result.reason == reason
    assert len(transport.calls) == 1
    assert result.counters["contracts_attempts"] == 1


@pytest.mark.asyncio
async def test_final_url_mismatch_is_safety_terminal_without_retry(tmp_path: Path) -> None:
    transport = AdverseReplyTransport(
        final_url="https://gateway.prod.nado.xyz/v1/query?type=other"
    )
    result = await _run(tmp_path, transport)
    assert result.failure_class == "SAFETY"
    assert result.reason == "REDIRECT_FORBIDDEN"
    assert len(transport.calls) == 1
    assert result.counters["contracts_attempts"] == 1


@pytest.mark.asyncio
async def test_open_order_is_reported_and_never_normalized_to_zero(tmp_path: Path) -> None:
    fixtures = _fixtures()
    orders = fixtures[("subaccount_orders", "1")]
    assert isinstance(orders["data"], dict)
    orders_data = orders["data"]
    assert isinstance(orders_data, dict)
    orders_data["orders"] = [
        {
            "product_id": 1,
            "sender": gate.EXPECTED_SUBACCOUNT,
            "price_x18": "1000000000000000000",
            "amount": "1",
            "expiration": "2000000000",
            "nonce": "1",
            "unfilled_amount": "1",
            "digest": "0x" + "22" * 32,
            "placed_at": 1700000000,
            "appendix": "0",
            "order_type": "default",
        }
    ]
    result = await _run(tmp_path, FixtureTransport(fixtures))
    assert result.read_complete is True
    assert result.account is not None
    assert result.account["orders"]["open_total"] == 1
    assert result.account["orders"]["regular_open"] == 1
    assert result.account["orders"]["isolated_open"] == 0
    assert "OPEN_ORDERS_PRESENT" in result.blockers


@pytest.mark.asyncio
async def test_unsupported_surfaces_and_funding_are_explicit_blockers_not_zero_state(
    tmp_path: Path,
) -> None:
    result = await _run(tmp_path, FixtureTransport())
    assert all(reason in result.blockers for reason in gate.PRIVATE_ONLY_BLOCKERS)
    inventory = {item["surface"]: item for item in gate._surface_inventory()}
    assert all(item["status"] == "BLOCKED" for item in inventory.values())
    assert inventory["trigger_orders"]["reason"] == "TRIGGER_ORDER_LIST_SIGNED_POST_ONLY"
    assert inventory["archive_order_history"]["reason"] == "ARCHIVE_ORDER_HISTORY_POST_ONLY"
    assert inventory["archive_match_history"]["reason"] == "ARCHIVE_MATCH_HISTORY_POST_ONLY"
    assert inventory["archive_event_history"]["reason"] == "ARCHIVE_EVENT_HISTORY_POST_ONLY"
    assert inventory["archive_funding_payments"]["reason"] == "ARCHIVE_FUNDING_PAYMENTS_POST_ONLY"
    assert inventory["private_order_fill_position_stream"]["reason"] == (
        "PRIVATE_ORDER_FILL_POSITION_STREAM_SIGNED_AUTH_REQUIRED"
    )
    assert gate._empty_funding()["payment_status"] == "BLOCKED"
    assert gate._empty_funding()["historical_payments"] == "UNKNOWN"
    assert result.funding["payment_status"] == "BLOCKED"
    assert result.funding["historical_payments"] == "UNKNOWN"


def test_protected_store_rejects_unsafe_mode_symlink_and_directory() -> None:
    # These checks use temporary paths only; no production journal is touched.
    import tempfile

    with tempfile.TemporaryDirectory() as directory_name:
        directory = Path(directory_name)
        unsafe = directory / "unsafe.sqlite3"
        unsafe.write_bytes(b"")
        unsafe.chmod(0o644)
        with pytest.raises(gate.StoreFailure, match="STORE_INVALID"):
            gate.RunStore(unsafe, "unsafe-mode-1")

        target = directory / "target.sqlite3"
        target.write_bytes(b"")
        target.chmod(0o600)
        link = directory / "link.sqlite3"
        link.symlink_to(target)
        with pytest.raises(gate.StoreFailure, match="STORE_INVALID"):
            gate.RunStore(link, "unsafe-link-1")

        directory_link = directory / "journal-link"
        directory_link.symlink_to(directory, target_is_directory=True)
        with pytest.raises(gate.StoreFailure, match="RUN_DIRECTORY_INVALID"):
            gate._ensure_run_directory(directory_link)


def test_production_boundary_has_no_env_or_path_override_and_body_is_none(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr(sys, "argv", ["nado-mainnet-unsigned-read", "--path", "/tmp/x"])
    assert gate.main() == 2
    assert gate.MAINNET_QUERY_URL == "https://gateway.prod.nado.xyz/v1/query"
    assert gate.RUN_STORE_PATH.name == "runs-v1.sqlite3"
    source = inspect.getsource(gate._production_run) + inspect.getsource(gate.main)
    assert "os.environ" not in source
    assert "getenv" not in source
    assert "argparse" not in source
    request = gate.GetRequest("contracts", (("type", "contracts"),), 1)
    assert request.body is None
    assert "ARGUMENTS_FORBIDDEN" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_running_invocation_is_terminalized_on_restart_and_never_replayed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runs.sqlite3"
    store = gate.RunStore(path, "restart-fixture-1")
    assert store.claim() is None
    first_transport = FixtureTransport()
    result = await gate.run_fixture(
        store_path=path,
        invocation_id="restart-fixture-1",
        identity=_identity(),
        transport=first_transport,
        clock_ms=iter(range(1, 100)).__next__,
    )
    assert result.reason == "INTERRUPTED_RUNNING_INVOCATION"
    assert first_transport.calls == []
    second_transport = FixtureTransport()
    replay = await gate.run_fixture(
        store_path=path,
        invocation_id="restart-fixture-1",
        identity=_identity(),
        transport=second_transport,
        clock_ms=iter(range(1, 100)).__next__,
    )
    assert replay.evidence() == result.evidence()
    assert second_transport.calls == []


def test_transport_is_get_only_and_cli_rejects_arguments(monkeypatch, capsys) -> None:
    source = inspect.getsource(gate.MainnetGetTransport)
    tree = ast.parse(source)
    called = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
    }
    assert not called & {"post", "put", "patch", "delete", "execute", "send"}
    assert "session.post" not in source
    monkeypatch.setattr(sys, "argv", ["nado-mainnet-unsigned-read", "--override"])
    assert gate.main() == 2
    output = capsys.readouterr().out
    assert "ARGUMENTS_FORBIDDEN" in output
    assert gate.NO_MAINNET_WRITE_AUTHORITY in output


def test_orderbook_coverage_excludes_non_orderbook_products_and_is_bounded() -> None:
    catalog = gate.ProductCatalog(
        ids=(0, 1, 2, 11),
        spot_ids=frozenset({0, 1, 11}),
        perp_ids=frozenset({2}),
    )
    assert catalog.orderbook_ids == (1, 2)
    with pytest.raises(gate.GateFailure, match="PRODUCT_CATALOG_TOO_LARGE"):
        gate._decode_all_products(
            {
                "spot_products": [
                    {"product_id": product_id} for product_id in range(256)
                ],
                "perp_products": [{"product_id": 256}],
            }
        )
