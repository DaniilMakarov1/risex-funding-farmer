"""Fan-out (owner request 2026-09-26): one source LIMIT filled by several receiver MARKETs."""
from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from itertools import count

import pytest

from risex_spread_shadow.hood_handoff import (
    DepthLevel, Direction, HistoryPage, MutationReceipt, OrderBookSnapshot, OrderSnapshot, Outcome,
    PreflightBlocked, TradeReceipt, run_random_cycle,
)
from risex_spread_shadow.hood_handoff.fanout_cycle import (
    FANOUT_SIZE_MARGIN, compute_fanout_bounds, fanout_quantity_floor, split_fanout_ticks,
)
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, account, cycle_config, metadata

EXTERNAL_ASK, EXTERNAL_BID = 999, 998


class FanoutVenue:
    """A tiny price-time book: our LIMITs, injected external makers and the base book liquidity."""

    def __init__(self, clock, *, source=11, receivers=(22, 33), balances=None, ack_code=200,
                 base_bid=Decimal("100.0")):
        self.clock = clock
        self.source_account_index = source
        self.receiver_account_index = receivers[0]
        self.extra_receiver_account_indices = tuple(receivers[1:])
        self.accounts = (source, *receivers)
        self.positions = {index: Decimal(0) for index in self.accounts}
        self.balances = balances or {}
        self.orders: dict[str, OrderSnapshot] = {}
        self.trades: dict[str, list[TradeReceipt]] = {}
        self.levels: list[dict] = []   # resting makers: our LIMITs and injected external quotes
        self.ids = count(1)
        self.sends: list[tuple] = []
        self.cancellations: list[str] = []
        self.ack_code = ack_code
        self.base_bid = base_bid
        # Adverse hooks, applied when the first receiver MARKET of a phase arrives.
        self.inject_maker: tuple[str, Decimal, Decimal] | None = None   # (side, price, quantity)
        self.external_take: Decimal | None = None
        self.unknown_send: set[int] = set()
        self.admission_veto: str | None = None
        self._phase_hooked = False

    # ----- reads -----------------------------------------------------------
    async def market_metadata(self, market_id):
        return metadata(self.clock.now())

    def _book(self) -> OrderBookSnapshot:
        now = self.clock.now()
        asks = [DepthLevel(Decimal("100.2"), Decimal("100"), "base-ask", EXTERNAL_ASK)]
        bids = [DepthLevel(self.base_bid, Decimal("100"), "base-bid", EXTERNAL_BID)]
        for level in self.levels:
            if level["remaining"] <= 0:
                continue
            row = DepthLevel(level["price"], level["remaining"], level["order_id"], level["account"])
            (asks if level["side"] == "SELL" else bids).append(row)
        merged = lambda rows, reverse: tuple(sorted(rows, key=lambda r: r.price, reverse=reverse))
        return OrderBookSnapshot(market_id=7, symbol="BTC", bids=merged(bids, True), asks=merged(asks, False),
                                 observed_at=now, market_type="perp", venue="robinhood")

    async def order_book(self, market_id):
        return self._book()

    async def account_snapshot(self, account_index, market_id):
        active = tuple(order for order in self.orders.values()
                       if order.account_index == account_index and order.active)
        balance = self.balances.get(account_index, Decimal("1000"))
        return account(account_index, self.positions[account_index], observed_at=self.clock.now(),
                       available_balance=balance, active_orders=active)

    async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
        for order in self.orders.values():
            if order.account_index != account_index:
                continue
            if order_id is not None and order.order_id != str(order_id):
                continue
            if client_order_index is not None and str(order.client_order_index) != str(client_order_index):
                continue
            return replace(order, observed_at=self.clock.now())
        return None

    async def list_trades(self, account_index, market_id, *, order_id=None, cursor=None, limit=100):
        return HistoryPage(trades=tuple(self.trades.get(str(order_id), ())))

    # ----- stream admission ------------------------------------------------
    def begin_ws_admission(self):
        return object()

    def ws_admission_view(self, anchor, plan, order_id):
        book = self._book()
        veto, self.admission_veto = self.admission_veto, None
        side = "asks" if plan.side == "SELL" else "bids"
        if veto == "better":
            better = plan.price - Decimal("0.1") if plan.side == "SELL" else plan.price + Decimal("0.1")
            book = replace(book, **{side: (DepthLevel(better, Decimal("1"), "rival", 555), *getattr(book, side))})
        elif veto == "same":
            rows = list(getattr(book, side))
            rows[0] = replace(rows[0], quantity=rows[0].quantity + Decimal("1"))
            book = replace(book, **{side: tuple(rows)})
        return None, book

    # ----- mutations --------------------------------------------------------
    async def prepare_order(self, plan, *, reserved_nonce=None):
        return {"plan": plan, "ready": True}

    async def invalidate_prepared_order(self, token):
        token["ready"] = False

    async def submit_prepared_order(self, plan, prepared, *, deadline=None):
        assert prepared["ready"] and prepared["plan"] == plan
        prepared["ready"] = False
        return await self.submit_order(plan)

    def _save(self, order: OrderSnapshot) -> OrderSnapshot:
        self.orders[order.order_id] = order
        return order

    def _trade(self, trade_id, account_index, order, side, quantity, price, peer, peer_order, peer_client):
        return TradeReceipt(trade_id, account_index, 7, order.order_id, side, quantity, price, None, peer,
                            self.clock.now(), counterparty_order_id=peer_order,
                            counterparty_client_order_index=peer_client, client_order_index=order.client_order_index)

    def _fill_maker(self, level, quantity):
        level["remaining"] -= quantity
        order = self.orders.get(level["order_id"])
        if order is not None:
            filled = order.filled_quantity + quantity
            self._save(replace(order, filled_quantity=filled, remaining_quantity=order.initial_quantity - filled,
                               status="filled" if filled == order.initial_quantity else order.status,
                               observed_at=self.clock.now()))

    def _hooks(self, plan):
        if self._phase_hooked:
            return
        self._phase_hooked = True
        if self.inject_maker is not None:
            side, price, quantity = self.inject_maker
            self.levels.append({"side": side, "price": price, "remaining": quantity, "order_id": "rival-maker",
                                "account": 777, "at": -1})
            self.inject_maker = None
        if self.external_take is not None:
            ours = next(level for level in self.levels if level["account"] in self.accounts and level["remaining"] > 0)
            take, self.external_take = self.external_take, None
            order = self.orders[ours["order_id"]]
            trade_id = f"external-take-{next(self.ids)}"
            self.trades.setdefault(order.order_id, []).append(self._trade(
                trade_id, order.account_index, order, order.side, take, order.price, 888, "external-order", 4242))
            self.positions[order.account_index] += take if order.side == "BUY" else -take
            self._fill_maker(ours, take)

    async def submit_order(self, plan):
        self.sends.append((plan.order_type, plan.account_index, plan.side, plan.quantity, plan.reduce_only))
        order_id = f"order-{next(self.ids)}"
        if plan.order_type == "LIMIT":
            self._phase_hooked = False
            order = self._save(OrderSnapshot(
                account_index=plan.account_index, market_id=plan.market_id, order_id=order_id,
                client_order_index=plan.client_order_index, status="open", side=plan.side, order_type="LIMIT",
                time_in_force="POST_ONLY", reduce_only=plan.reduce_only, initial_quantity=plan.quantity,
                remaining_quantity=plan.quantity, filled_quantity=Decimal(0), price=plan.price,
                observed_at=self.clock.now()))
            self.levels.append({"side": plan.side, "price": plan.price, "remaining": plan.quantity,
                                "order_id": order.order_id, "account": plan.account_index, "at": len(self.levels)})
            return MutationReceipt(True, None, f"tx-{order_id}", response_code=self.ack_code)
        if not plan.reduce_only or plan.account_index != self.source_account_index or True:
            self._hooks(plan)
        if plan.account_index in self.unknown_send:
            self.unknown_send.discard(plan.account_index)
            raise TimeoutError("synthetic send timeout")
        remaining = plan.quantity
        order = OrderSnapshot(
            account_index=plan.account_index, market_id=plan.market_id, order_id=order_id,
            client_order_index=plan.client_order_index, status="open", side=plan.side, order_type="MARKET",
            time_in_force="IOC", reduce_only=plan.reduce_only, initial_quantity=plan.quantity,
            remaining_quantity=plan.quantity, filled_quantity=Decimal(0), price=plan.price,
            observed_at=self.clock.now())
        makers = [level for level in self.levels if level["remaining"] > 0 and level["side"] != plan.side
                  and level["account"] != plan.account_index]
        if plan.side == "BUY":
            makers = [level for level in makers if level["price"] <= plan.price]
            makers.sort(key=lambda level: (level["price"], level["at"]))
            base = (Decimal("100.2"), EXTERNAL_ASK)
        else:
            makers = [level for level in makers if level["price"] >= plan.price]
            makers.sort(key=lambda level: (-level["price"], level["at"]))
            base = (self.base_bid, EXTERNAL_BID)
        fills = []
        for level in makers:
            if remaining <= 0:
                break
            quantity = min(remaining, level["remaining"])
            fills.append((level, quantity, level["price"]))
            remaining -= quantity
        if remaining > 0 and ((plan.side == "BUY" and base[0] <= plan.price) or (plan.side == "SELL" and base[0] >= plan.price)):
            fills.append((None, remaining, base[0]))
            remaining = 0
        filled = plan.quantity - remaining
        order = self._save(replace(order, status="filled" if remaining == 0 else "canceled-not-enough-liquidity",
                                   remaining_quantity=Decimal(0) if remaining else Decimal(0), filled_quantity=filled))
        self.trades[order.order_id] = []
        for level, quantity, price in fills:
            trade_id = f"trade-{next(self.ids)}"
            if level is None:
                peer, peer_order, peer_client = base[1], "base-order", 1
            else:
                maker_order = self.orders.get(level["order_id"])
                peer = level["account"]
                peer_order = level["order_id"]
                peer_client = maker_order.client_order_index if maker_order is not None else 4343
                if maker_order is not None:
                    self.trades.setdefault(maker_order.order_id, []).append(self._trade(
                        trade_id, peer, maker_order, maker_order.side, quantity, price,
                        plan.account_index, order.order_id, order.client_order_index))
                    self.positions[peer] += quantity if maker_order.side == "BUY" else -quantity
                self._fill_maker(level, quantity)
            self.trades[order.order_id].append(self._trade(trade_id, plan.account_index, order, plan.side, quantity,
                                                           price, peer, peer_order, peer_client))
            self.positions[plan.account_index] += quantity if plan.side == "BUY" else -quantity
        return MutationReceipt(True, order.order_id, f"tx-{order.order_id}", response_code=200)

    async def cancel_order(self, account_index, market_id, order_id):
        self.cancellations.append(order_id)
        order = self.orders.get(str(order_id))
        if order is not None and order.active:
            self._save(replace(order, status="canceled", remaining_quantity=Decimal(0), observed_at=self.clock.now()))
            for level in self.levels:
                if level["order_id"] == order.order_id:
                    level["remaining"] = Decimal(0)
        return MutationReceipt(True, str(order_id), f"tx-cancel-{order_id}", response_code=200)


def fanout_config(path, *, receivers=(22, 33), direction=Direction.LONG, admission="ack", **overrides):
    return cycle_config(path, direction=direction, receiver_admission=admission, price_improvement_ticks=1,
                        source_account_index=11, receiver_account_index=receivers[0],
                        extra_receiver_account_indices=tuple(receivers[1:]), **overrides)


def journal_events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


# ----- sizing ---------------------------------------------------------------

def test_limit_floor_is_k_minimums_plus_ten_percent_on_the_grid():
    assert FANOUT_SIZE_MARGIN == Decimal("1.10")
    # 0.0002 BTC minimum on a 0.00001 grid is 20 ticks: 2 x 20 x 1.1 = 44, 3 x 20 x 1.1 = 66.
    assert fanout_quantity_floor(2, 20) == 44 and fanout_quantity_floor(3, 20) == 66
    assert fanout_quantity_floor(2, 7) == 16  # 15.4 rounds up to the grid


def test_split_is_random_legal_and_exact():
    caps = (40, 40, 40)
    parts = split_fanout_ticks(70, caps, 20, FixedRng(3, 7))   # cuts 3 and 7 of the 10 extra ticks
    assert parts == (23, 24, 23) and sum(parts) == 70
    with pytest.raises(PreflightBlocked):
        split_fanout_ticks(59, caps, 20, FixedRng())            # below 3 minimums
    capped = split_fanout_ticks(60, (20, 20, 30), 20, FixedRng(*([0] * 128)))
    assert capped == (20, 20, 20)


def test_bounds_refuse_when_the_source_cannot_hold_two_minimums_plus_margin():
    meta = metadata()
    rich = account(22, Decimal(0))
    poor_source = account(11, Decimal(0), available_balance=Decimal("5.5"))  # 4x -> 0.21 < 0.22
    with pytest.raises(PreflightBlocked, match="10 %"):
        compute_fanout_bounds(meta, poor_source, (rich, account(33, Decimal(0))), Decimal("100.1"),
                              receiver_bound=Decimal("100.1"), direction=Direction.LONG)
    bounds = compute_fanout_bounds(meta, account(11, Decimal(0)), (rich, account(33, Decimal(0))), Decimal("100.1"),
                                   receiver_bound=Decimal("100.1"), direction=Direction.LONG)
    assert bounds.part_minimum_tick == 10 and bounds.quantity.lower_tick == 22


# ----- whole cycles -----------------------------------------------------------

@pytest.mark.parametrize("direction", list(Direction))
@pytest.mark.parametrize("receivers", [(22, 33), (22, 33, 44)])
async def test_full_fanout_cycle_opens_holds_and_closes_flat(tmp_path, direction, receivers):
    clock = AdvancingClock()
    venue = FanoutVenue(clock, receivers=receivers)
    # 40 ticks of 0.01: two parts over 2 x 10 minimum (+20 extra), three over 3 x 10 (+10 extra).
    extra = 40 - 10 * len(receivers)
    cuts = [5] if len(receivers) == 2 else [3, 7]
    rng = FixedRng(40, 20, *cuts)
    result = await run_random_cycle(fanout_config(tmp_path / "cycle", receivers=receivers, direction=direction),
                                    venue, clock=clock, rng=rng)
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == "CONFIRMED_FLAT" and result.paired_execution == "SUCCESS"
    assert all(value == 0 for value in venue.positions.values())
    opening = result.opening
    assert opening.mutual_execution_proven and opening.source.filled_quantity == Decimal("0.40")
    assert [leg.filled_quantity for leg in opening.receivers] == list(result.selection.receiver_parts)
    assert sum(result.selection.receiver_parts) == Decimal("0.40") and extra >= 0
    assert result.selection.receiver_parts == ((Decimal("0.15"), Decimal("0.25")) if len(receivers) == 2
                                               else (Decimal("0.13"), Decimal("0.14"), Decimal("0.13")))
    assert result.closing.mutual_execution_proven and not result.fallbacks
    markets = [send for send in venue.sends if send[0] == "MARKET"]
    assert sorted(send[1] for send in markets) == sorted(list(receivers) * 2)  # opening and closing only
    events = [row["event"] for row in journal_events(tmp_path / "cycle" / "opening.jsonl")]
    intents = [event for event in events if event.endswith("_DISPATCH_INTENT")]
    assert intents[0] == "SOURCE_DISPATCH_INTENT"
    assert intents[1:] == ["RECEIVER_DISPATCH_INTENT", *(f"RECEIVER_{i}_DISPATCH_INTENT" for i in range(2, len(receivers) + 1))]
    first_result = min(i for i, event in enumerate(events) if event.startswith("RECEIVER") and event.endswith("_RESULT"))
    assert all(i < first_result for i, event in enumerate(events) if event.startswith("RECEIVER") and event.endswith("_INTENT"))
    cycle = journal_events(tmp_path / "cycle" / "cycle.jsonl")
    complete = next(row for row in cycle if row["event"] == "CYCLE_COMPLETE")["payload"]
    assert set(complete["remaining_positions"]) == {"source", "receiver", *(f"receiver_{i}" for i in range(2, len(receivers) + 1))}


async def test_external_maker_inside_our_price_leaves_partial_and_everything_is_closed(tmp_path):
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    venue.inject_maker = ("SELL", Decimal("100.1"), Decimal("0.05"))   # a rival quote ahead of the MARKETs
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(30, 20, 5))
    assert result.outcome is Outcome.PARTIAL, result.reason
    assert result.closing is None  # no hold: straight to reduce-only recovery
    assert result.inventory == "CONFIRMED_FLAT"
    assert all(value == 0 for value in venue.positions.values())
    assert result.opening.joint_match_status in {"PARTIAL", "CONFLICTING"}
    assert not result.opening.mutual_execution_proven
    assert all(order.status != "open" for order in venue.orders.values())


async def test_external_taker_before_our_markets_partial_then_recovery(tmp_path):
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    venue.external_take = Decimal("0.05")   # parts 0.15/0.15: the second receiver still gets one minimum
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(30, 20, 5))
    assert result.outcome is Outcome.PARTIAL and result.closing is None, result.reason
    assert result.inventory == "CONFIRMED_FLAT" and all(value == 0 for value in venue.positions.values())


async def test_partial_receiver_fill_below_the_venue_minimum_stops_with_a_known_residual(tmp_path):
    """A part filled below one venue minimum cannot be closed reduce-only under the current rules."""
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    venue.external_take = Decimal("0.10")   # second receiver fills 0.05 < 0.10 minimum
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(30, 20, 5))
    assert result.inventory == "KNOWN_RESIDUAL" and result.closing is None
    assert venue.positions == {11: Decimal("0.00"), 22: Decimal("0.00"), 33: Decimal("0.05")}
    assert "below a documented venue minimum" in result.reason
    assert result.remaining_extra_positions == (Decimal("0.05"),)


async def test_external_maker_at_closing_leaves_a_source_residual_closed_reduce_only(tmp_path):
    class ClosingRival(FanoutVenue):
        armed = True

        async def submit_order(self, plan):
            result = await super().submit_order(plan)
            if plan.order_type == "LIMIT" and plan.reduce_only and self.armed:
                self.armed = False   # a rival bid ahead of our closing LIMIT, taken by the first MARKET
                self.inject_maker = ("BUY", Decimal("100.1"), Decimal("0.10"))
            return result

    clock = AdvancingClock()
    venue = ClosingRival(clock)
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(40, 20, 5))
    assert result.outcome is Outcome.PARTIAL and result.inventory == "CONFIRMED_FLAT", result.reason
    assert result.opening.mutual_execution_proven and not result.closing.mutual_execution_proven
    # 0.15 + 0.25 closed by the receivers; 0.10 of it went to the rival, so our LIMIT kept 0.10 open.
    assert venue.sends[-1] == ("MARKET", 11, "BUY", Decimal("0.10"), True)
    assert [item.account_index for item in result.fallbacks] == [11]
    assert all(value == 0 for value in venue.positions.values())


async def test_rejected_receiver_market_stops_without_dependent_writes(tmp_path):
    from risex_spread_shadow.hood_handoff import MutationReceipt, load_saved_cycle_report

    class Reject(FanoutVenue):
        async def submit_order(self, plan):
            if plan.order_type == "MARKET" and plan.account_index == 33 and not plan.reduce_only:
                self.sends.append((plan.order_type, plan.account_index, plan.side, plan.quantity, plan.reduce_only))
                return MutationReceipt(False, None, None, response_code=400, error="synthetic rejection")
            return await super().submit_order(plan)

    clock = AdvancingClock()
    venue = Reject(clock)
    slot = tmp_path / "cycle"
    result = await run_random_cycle(fanout_config(slot), venue, clock=clock, rng=FixedRng(40, 20, 5))
    # As for a 1 -> 1 cycle, an unresolved receiver order is never followed by automatic writes:
    # the known positions stay for /close and the series stops.
    assert result.outcome is Outcome.UNKNOWN and not result.fallbacks
    assert venue.positions == {11: Decimal("-0.15"), 22: Decimal("0.15"), 33: Decimal(0)}
    assert not any(send[4] for send in venue.sends)   # no reduce-only order was sent
    report = load_saved_cycle_report(slot)
    assert report["inventory"]["status"] != "CONFIRMED_FLAT"


async def test_unknown_receiver_send_stops_dependent_writes(tmp_path):
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    venue.unknown_send = {33}
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(30, 20, 5))
    assert result.outcome is Outcome.UNKNOWN, result.reason
    assert result.inventory != "CONFIRMED_FLAT"
    assert any("receiver_2 dispatch outcome unknown" in reason for reason in result.opening.unknown_reasons)
    # No reduce-only order may follow an unknown send.
    assert not [send for send in venue.sends if send[0] == "MARKET" and send[4]]


@pytest.mark.parametrize("veto", ["better", "same"])
async def test_admission_refusal_is_zero_mutation_and_retried(tmp_path, veto):
    clock = AdvancingClock()
    venue = FanoutVenue(clock, base_bid=Decimal("99.8"))
    venue.admission_veto = veto
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(30, 20, 5))
    assert result.outcome is Outcome.SUCCESS, result.reason
    limits = [send for send in venue.sends if send[0] == "LIMIT"]
    assert len(limits) == 3  # refused opening LIMIT (cancelled), retried opening, closing
    assert (tmp_path / "cycle" / "opening-attempt-002.jsonl").exists()
    first = result.opening  # the retried attempt; the first one sent no MARKET
    assert first.mutual_execution_proven
    refused = [json.loads(line) for line in (tmp_path / "cycle" / "opening.jsonl").read_text().splitlines()]
    assert not any(row["event"].startswith("RECEIVER") and row["event"].endswith("_INTENT") for row in refused)


# ----- offline report ------------------------------------------------------------

@pytest.mark.parametrize("receivers", [(22, 33), (22, 33, 44)])
async def test_offline_report_proves_a_full_fanout_cycle(tmp_path, receivers):
    from risex_spread_shadow.hood_handoff import load_saved_cycle_report
    clock = AdvancingClock()
    venue = FanoutVenue(clock, receivers=receivers)
    cuts = [5] if len(receivers) == 2 else [3, 7]
    result = await run_random_cycle(fanout_config(tmp_path / "cycle", receivers=receivers), venue, clock=clock,
                                    rng=FixedRng(40, 20, *cuts))
    assert result.outcome is Outcome.SUCCESS, result.reason
    report = load_saved_cycle_report(tmp_path / "cycle")
    assert report["status"] == "COMPLETE", report["issues"]
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    roles = ["source", "receiver", *(f"receiver_{i}" for i in range(2, len(receivers) + 1))]
    assert all(report["inventory"][role] == "0.00" for role in roles)
    assert report["paired_execution"]["status"] == "SUCCESS", report["paired_execution"]["reasons"]
    rows = report["paired_execution"]["direct_counterparty_match"]
    assert [row["status"] for row in rows] == ["MATCHED", "MATCHED"]
    assert all(row["matched_quantity"] == "0.40" and row["external_source_quantity"] == "0" for row in rows)
    assert report["order_state"]["unresolved_intents"] == [] and report["order_state"]["unresolved_observed_orders"] == []
    pnl = report["economics"]["closed_execution_pnl"]
    assert pnl["status"] in {"PROVEN", "GROSS_ONLY"} and {row["account_index"] for row in pnl["per_account"]} == {
        "11", *(str(r) for r in receivers)}
    # Every account's own fills are receipts of the cycle; each trade is counted once per account.
    accounts = {fill["account_index"] for fill in report["confirmed_fills"]}
    assert accounts == {11, *receivers}
    assert report["binding"]["extra_receiver_account_indices"] == list(receivers[1:])


async def test_offline_report_of_a_partial_fanout_names_external_volume(tmp_path):
    from risex_spread_shadow.hood_handoff import load_saved_cycle_report
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    venue.inject_maker = ("SELL", Decimal("100.1"), Decimal("0.05"))
    await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(30, 20, 5))
    report = load_saved_cycle_report(tmp_path / "cycle")
    assert report["status"] == "COMPLETE", report["issues"]
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    opening = report["paired_execution"]["direct_counterparty_match"][0]
    assert opening["status"] == "NOT_MATCHED" and opening["external_receiver_quantity"] == "0.05"
    assert opening["external_receiver_accounts"] == [777]
    assert report["paired_execution"]["status"] == "FAILED"


async def test_offline_report_keeps_a_sub_minimum_residual_open(tmp_path):
    from risex_spread_shadow.hood_handoff import load_saved_cycle_report
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    venue.external_take = Decimal("0.10")
    await run_random_cycle(fanout_config(tmp_path / "cycle"), venue, clock=clock, rng=FixedRng(30, 20, 5))
    report = load_saved_cycle_report(tmp_path / "cycle")
    # As for a 1 -> 1 cycle, a refused below-minimum recovery never becomes flat or complete.
    assert report["status"] == "INCOMPLETE" and report["inventory"]["status"] != "CONFIRMED_FLAT"
    assert report["inventory"]["receiver_2"] == "0.05"


# ----- operator recovery ----------------------------------------------------------------

@pytest.mark.parametrize("unknown", [False, True])
async def test_prior_intents_cover_every_fanout_receiver(tmp_path, unknown):
    from risex_spread_shadow.hood_handoff.operator_recovery import prior_intents
    tmp_path.chmod(0o700)
    clock = AdvancingClock()
    venue = FanoutVenue(clock, receivers=(22, 33, 44))
    if unknown:
        venue.unknown_send = {44}
    config = fanout_config(tmp_path / "cycle-001", receivers=(22, 33, 44))
    await run_random_cycle(config, venue, clock=clock, rng=FixedRng(40, 20, 3, 7))
    intents, files, _ = prior_intents(tmp_path, config, indices=(22, 33, 44))
    receivers = {item["plan"].account_index for item in intents if item["plan"].order_type == "MARKET"
                 and not item["plan"].reduce_only}
    assert receivers == {22, 33, 44}  # every opening MARKET, including receiver_2 and receiver_3
    unresolved = [item for item in intents if not item["resolved"]]
    if unknown:
        # The unknown send of receiver_3 has no terminal proof in the journal; recovery must look it up.
        assert [item["plan"].account_index for item in unresolved] == [44]
    else:
        assert unresolved == []
        assert len(intents) == 8  # opening and closing: source + three receivers each


# ----- wallet draw ------------------------------------------------------------------------

def test_fanout_requirements_are_k_minimums_plus_margin_at_4x(tmp_path):
    from risex_spread_shadow.hood_handoff import wallet_pool as wp
    from test_hood_handoff_random_cycle import book
    config = cycle_config(tmp_path)
    # Independent arithmetic (as for one minimum order): bid 100.0 / ask 100.2, no mark -> ask;
    # 25 % margin, taker cap 0.00035, adverse 0.2, 5 % safety; minimum 10 ticks of 0.01.
    per_btc = Decimal("100.2") * Decimal("0.25") + Decimal("100.2") * Decimal("0.00035") + Decimal("0.2")
    expected = {None: Decimal("0.10"), 1: Decimal("0.11"), 2: Decimal("0.22"), 3: Decimal("0.33")}
    for parts, quantity in expected.items():
        assert wp.wallet_requirement(metadata(), book(), config, fanout_parts=parts) == quantity * per_btc * Decimal("1.05")
    with pytest.raises(Exception):
        wp.wallet_requirement(metadata(), book(), config, fanout_parts=0)


def test_fanout_draw_enumerates_every_source_count_and_receiver_order():
    from itertools import permutations
    from risex_spread_shadow.hood_handoff import wallet_pool as wp
    from test_hood_wallet_pool import EnumeratingRng
    ready = [44, 11, 33, 22]
    capacity = {11: 3, 22: 2, 33: 0, 44: 3}   # 33 cannot fund a two-part LIMIT
    outcomes = []
    for source in range(3):
        top = {0: 3, 1: 2, 2: 3}[source]
        for size in range(2, top + 1):
            for first in range(3):
                for second in range(2):
                    for third in (range(1) if size == 3 else [None]):
                        draws = [source, size, first, second] + ([third] if third is not None else [])
                        rng = EnumeratingRng(draws)
                        outcomes.append(wp.draw_fanout(ready, capacity, rng))
                        assert rng.bounds[:2] == [(0, 2), (2, top)] and rng.values == []
    sources = {11: [], 22: [], 44: []}
    for source, receivers in outcomes:
        assert source not in receivers and len(set(receivers)) == len(receivers) and 2 <= len(receivers)
        sources[source].append(receivers)
    for source, seen in sources.items():
        others = [w for w in (11, 22, 33, 44) if w != source]
        allowed = [p for k in range(2, capacity[source] + 1) for p in permutations(others, k)]
        assert sorted(seen) == sorted(allowed)   # each ordered receiver tuple exactly once
    assert wp.draw_fanout([11, 22], {11: 3, 22: 3}) is None          # fewer than three ready wallets
    assert wp.draw_fanout([11, 22, 33], {11: 1, 22: 0, 33: 1}) is None  # no funded source


async def test_fanout_selection_records_the_draw_and_the_requirements(tmp_path):
    from risex_spread_shadow.hood_handoff import wallet_pool as wp
    from test_hood_wallet_pool import EnumeratingRng, SelectionClient
    from test_hood_handoff_random_cycle import book
    clock = AdvancingClock()
    config = cycle_config(tmp_path)
    one = wp.wallet_requirement(metadata(), book(), config)
    part = wp.wallet_requirement(metadata(), book(), config, fanout_parts=1)
    two = wp.wallet_requirement(metadata(), book(), config, fanout_parts=2)
    states = {11: (str(two), '0', True),                       # funds exactly a two-part LIMIT
              22: (str(part), '0', True),                      # one receiver part only
              33: (str((one + part) / 2), '0', True),          # eligible, but not a fan-out receiver
              44: ('1000', '0', True), 55: ('1000', '0.1', True)}
    pool = wp.WalletPool(active=(11, 22, 33, 44, 55), from_file=True)
    rng = EnumeratingRng([0, 2, 1, 0])
    record = await wp.select_wallet_pair(config, pool, SelectionClient(clock, states), has_key=lambda _: True,
                                         rng=rng, clock=clock.now, fanout=True)
    assert record['mode'] == 'fanout' and record['pair'] is None
    assert record['eligible'] == [11, 22, 33, 44] and record['skipped'] == [{'account_index': 55, 'reason': 'NOT_FLAT'}]
    fanout = record['fanout']
    assert fanout['receiver_ready'] == [11, 22, 44]
    assert fanout['source_capacity'] == {'11': 2, '22': 0, '44': 2}
    assert Decimal(fanout['receiver_requirement_quote']) == part
    assert fanout['source_requirement_quote'] == {'2': format(two, 'f')}
    # Sources [11, 44]: draw 0 -> 11; k in [2, 2]; receivers from [22, 44]: 44 then 22.
    assert (fanout['source'], fanout['receivers']) == (11, [44, 22])
    assert rng.bounds == [(0, 1), (2, 2), (0, 1), (0, 0)]


# ----- simple --fanout launcher -----------------------------------------------------------

def fanout_launcher(tmp_path, monkeypatch, wallets, *, states=None, draw=(), direction=0, rng=None,
                    admission=('--receiver-admission', 'ack', '--price-improvement-ticks', '1')):
    from risex_spread_shadow.hood_handoff import cli as cli_module
    from risex_spread_shadow.hood_handoff import fanout_cycle as fanout_cycle_module
    from risex_spread_shadow.hood_handoff import operator_recovery as recovery
    from risex_spread_shadow.hood_handoff import wallet_pool as wp
    from risex_spread_shadow.hood_handoff.keychain import (
        KeychainBinding, KeychainSecretProvider, MemoryKeychainBackend,
    )
    from test_hood_handoff_random_cycle import _write_simple_launcher_fixture
    from test_hood_wallet_pool import EnumeratingRng, write_pool
    config_path, operator_dir, evidence_path = _write_simple_launcher_fixture(tmp_path)
    if wallets is not None:
        write_pool(operator_dir, wallets)
    clock = AdvancingClock()
    states = states or {index: ('1000', '0', True) for index in wallets or ()}
    calls = {'primed': [], 'clients': [], 'configs': [], 'venues': []}

    class FakeSdkClient:
        def __init__(self, config, *, source_account_index, receiver_account_index, secrets, market_evidence,
                     extra_receiver_account_indices=()):
            extras = tuple(extra_receiver_account_indices)
            calls['clients'].append((source_account_index, receiver_account_index, extras, type(secrets).__name__))
            self._venue = FanoutVenue(clock, source=source_account_index,
                                      receivers=(receiver_account_index, *extras))
            calls['venues'].append(self._venue)

        def __getattr__(self, name):
            return getattr(self._venue, name)

        async def public_account_state(self, index, market_id):
            balance, position, ready = states[index]
            return wp.WalletState(index, Decimal(balance), Decimal(position), ready, clock.now())

        async def aclose(self):
            return None

    backend = MemoryKeychainBackend()

    def provider(config, indices, *, replace, prompt=None):
        for index in wallets or (11, 22):
            backend.values.setdefault(KeychainBinding.from_config(config, index), f'synthetic-{index}')
        return KeychainSecretProvider.from_config(config, indices, backend=backend, replace=replace, prompt=prompt)

    def prime(provider_, indices):
        calls['primed'].append(tuple(indices))
        for index in indices:
            provider_.private_key(index, provider_.api_key_index)

    async def resolve(config, client, operator, **kwargs):
        return {}

    async def run(config, client, *, stop_requested=None):
        calls['configs'].append(config)
        return await run_random_cycle(config, client, clock=clock, rng=rng)

    real_select = wp.select_wallet_pair
    draws = EnumeratingRng(list(draw))

    async def select(config, pool, client, *, has_key, fanout=False):
        return await real_select(config, pool, client, has_key=has_key, rng=draws, clock=lambda: 7.0, fanout=fanout)

    real_route = fanout_cycle_module.select_fanout_route
    monkeypatch.setattr(fanout_cycle_module, 'select_fanout_route',
                        lambda source, receivers: real_route(source, receivers, FixedRng(direction)))
    monkeypatch.setattr(wp, 'select_wallet_pair', select)
    monkeypatch.setattr('builtins.input', lambda _prompt: '')
    monkeypatch.setattr(cli_module, '_validate_simple_sdk', lambda: None)
    monkeypatch.setattr(cli_module, 'LighterSdkClient', FakeSdkClient)
    monkeypatch.setattr(cli_module, '_keychain_provider', provider)
    monkeypatch.setattr(cli_module, '_prime_keychain', prime)
    monkeypatch.setattr(recovery, 'resolve_prior', resolve)
    monkeypatch.setattr(cli_module, 'run_random_cycle', run)
    args = cli_module._parser().parse_args(['simple', '--fanout', '--keychain', '--no-progress',
                                            '--config', str(config_path), '--market-evidence', str(evidence_path),
                                            *admission])
    return cli_module, args, operator_dir, calls


@pytest.mark.parametrize("draw,direction,cycle_rng,expected", [
    ((1, 2, 2, 0), 0, (40, 20, 5), (22, (44, 11), "LONG")),
    ((3, 3, 0, 0, 0), 1, (40, 20, 3, 7), (44, (11, 22, 33), "SHORT")),
])
async def test_simple_fanout_launch_binds_every_receiver_and_runs_flat(tmp_path, monkeypatch, capsys,
                                                                       draw, direction, cycle_rng, expected):
    cli_module, args, operator_dir, calls = fanout_launcher(tmp_path, monkeypatch, [11, 22, 33, 44], draw=draw,
                                                            direction=direction, rng=FixedRng(*cycle_rng))
    assert await cli_module._run(args) == 0
    source, receivers, side = expected
    slot = operator_dir / "cycle-001"
    launch = json.loads((slot / "launch.json").read_text())
    assert launch["random_route"] == {"source_account_index": source, "receiver_account_index": receivers[0],
                                      "direction": side}
    assert launch["fanout_receivers"] == list(receivers[1:])
    assert launch["wallet_selection"]["mode"] == "fanout"
    assert launch["wallet_selection"]["fanout"]["source"] == source
    assert launch["wallet_selection"]["fanout"]["receivers"] == list(receivers)
    [config] = calls["configs"]
    assert config.extra_receiver_account_indices == receivers[1:]
    # The selection reader signs nothing; the cycle client may sign for exactly the drawn accounts.
    assert calls["clients"][0][3] == "_NoSigningSecrets"
    assert calls["clients"][-1] == (source, receivers[0], receivers[1:], "KeychainSecretProvider")
    assert calls["primed"] == [(source, *receivers)]
    venue = calls["venues"][-1]
    assert all(value == 0 for value in venue.positions.values())
    markets = sorted(send[1] for send in venue.sends if send[0] == "MARKET")
    assert markets == sorted(list(receivers) * 2)
    cycle = journal_events(slot / "cycle.jsonl")
    started = cycle[0]["payload"]["binding"]
    assert started["extra_receiver_account_indices"] == list(receivers[1:])
    complete = next(row for row in cycle if row["event"] == "CYCLE_COMPLETE")["payload"]
    assert complete["outcome"] == "SUCCESS"
    output = capsys.readouterr().out
    assert f"Кошельки: LIMIT — {source}, MARKET — {', '.join(map(str, receivers))}" in output
    from risex_spread_shadow.hood_handoff.telegram_cards import wallet_steps
    [(_, _, icon, line)] = wallet_steps(slot)
    assert (icon, line) == ("👛", f"кошельки: LIMIT — {source}, MARKET — {', '.join(map(str, receivers))} "
                                  "из 4 готовых (всего активных 4)")


async def test_simple_fanout_without_three_ready_wallets_sends_nothing(tmp_path, monkeypatch, capsys):
    from risex_spread_shadow.hood_handoff.operator_view import read_launch_failure
    states = {11: ('1000', '0', True), 22: ('1000', '0', True), 33: ('1000', '0.3', True)}
    cli_module, args, operator_dir, calls = fanout_launcher(tmp_path, monkeypatch, [11, 22, 33], states=states)
    assert await cli_module._run(args) == 2
    slot = operator_dir / "cycle-001"
    assert read_launch_failure(slot) == "WALLETS_UNAVAILABLE"
    launch = json.loads((slot / "launch.json").read_text())
    assert "random_route" not in launch and "fanout_receivers" not in launch
    assert launch["wallet_selection"]["fanout"]["source"] is None
    assert calls["configs"] == [] and calls["primed"] == [] and len(calls["clients"]) == 1
    assert "для режима «несколько MARKET»" in capsys.readouterr().out
    from risex_spread_shadow.hood_handoff.telegram_cards import wallet_steps
    [(_, _, icon, line)] = wallet_steps(slot)
    assert icon == "⛔" and line.startswith("кошельки: для режима «несколько MARKET» готовых 2 из 3")
    assert line.endswith("цикл не начат; пропущены: 33 — есть позиция")


async def test_simple_fanout_requires_the_wallet_pool_file(tmp_path, monkeypatch):
    from risex_spread_shadow.hood_handoff.operator_view import read_launch_failure
    cli_module, args, operator_dir, calls = fanout_launcher(tmp_path, monkeypatch, None)
    assert await cli_module._run(args) == 2
    assert read_launch_failure(operator_dir / "cycle-001") == "WALLET_POOL"
    assert calls == {"primed": [], "clients": [], "configs": [], "venues": []}


async def test_simple_fanout_refuses_strict_admission_before_claiming_a_slot(tmp_path, monkeypatch):
    cli_module, args, operator_dir, calls = fanout_launcher(tmp_path, monkeypatch, [11, 22, 33], admission=())
    with pytest.raises(SystemExit, match="только с приёмом ACK или WS"):
        await cli_module._run(args)
    assert not list(operator_dir.glob("cycle-*")) and calls["clients"] == []


def test_fanout_flag_belongs_to_simple_only(tmp_path):
    import asyncio
    from risex_spread_shadow.hood_handoff import cli as cli_module
    args = cli_module._parser().parse_args(["close-positions", "--fanout"])
    with pytest.raises(SystemExit, match="--fanout is supported only by simple"):
        asyncio.run(cli_module._run(args))


def test_fanout_route_draws_the_direction_uniformly():
    from risex_spread_shadow.hood_handoff.fanout_cycle import select_fanout_route
    assert select_fanout_route(11, (22, 33), FixedRng(0)) == (
        {"source_account_index": 11, "receiver_account_index": 22, "direction": "LONG"}, [33])
    assert select_fanout_route(11, (22, 33, 44), FixedRng(1)) == (
        {"source_account_index": 11, "receiver_account_index": 22, "direction": "SHORT"}, [33, 44])
    with pytest.raises(Exception):
        select_fanout_route(11, (22,), FixedRng(0))


# ----- Telegram views ---------------------------------------------------------------------

async def test_live_steps_and_card_name_every_receiver_and_own_quantities(tmp_path):
    from risex_spread_shadow.hood_handoff import load_saved_cycle_report
    from risex_spread_shadow.hood_handoff.telegram_cards import cycle_card, cycle_steps, phase_notices
    from test_hood_telegram_messages import valid
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    slot = tmp_path / "cycle"
    await run_random_cycle(fanout_config(slot), venue, clock=clock, rng=FixedRng(40, 20, 5))
    steps = {text for _, _, _, text in cycle_steps(slot)}
    # LONG: the source sells 0.40 at ask - 1 tick; 5 of the 20 extra ticks go to 22 (0.15), the rest to 33.
    assert "выбрано: LIMIT 11 SELL 0.4 BTC → 2 MARKET BUY: 22 0.15, 33 0.25 · удержание 20 с" in steps
    notices = {key: (icon, text) for key, _, icon, text in phase_notices(slot)}
    assert notices["opening-1-accepted"][1] == "открываю: LIMIT 11 SELL 0.4 BTC по 100.1 → MARKET 22 0.15, 33 0.25"
    assert notices["opening-1-execution"][0] == "🟢"
    assert notices["opening-1-execution"][1].startswith("открыто: 🤝 свои счета 0.4 BTC")
    assert notices["closing-1-execution"][1].startswith("закрыто: 🤝 свои счета 0.4 BTC")
    report = load_saved_cycle_report(slot)
    card = valid(cycle_card({"name": "cycle-001", "index": 1, "total": 1, "safe": True}, report, slot))
    lines = card.splitlines()
    assert lines[0].startswith("✅ Цикл · cycle-001")
    assert "LIMIT 11 SELL → MARKET 22 0.15 + 33 0.25 BUY" in lines
    assert "Открытие: 🤝 свои счета" in card and "Закрытие: 🤝 свои счета" in card
    assert "чужие" not in card


async def test_partial_fanout_card_names_the_external_fill_not_our_receivers(tmp_path):
    from risex_spread_shadow.hood_handoff import load_saved_cycle_report
    from risex_spread_shadow.hood_handoff.telegram_cards import cycle_card, phase_notices
    from test_hood_telegram_messages import valid
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    venue.inject_maker = ("SELL", Decimal("100.1"), Decimal("0.05"))
    slot = tmp_path / "cycle"
    await run_random_cycle(fanout_config(slot), venue, clock=clock, rng=FixedRng(30, 20, 5))
    notices = {key: (icon, text) for key, _, icon, text in phase_notices(slot)}
    icon, text = notices["opening-1-execution"]
    assert icon == "⚠️" and "MARKET исполнился о чужие 0.05 (счёт 777)" in text
    assert "22" not in text.split("чужие", 1)[1] and "33" not in text.split("чужие", 1)[1]
    card = valid(cycle_card({"name": "cycle-001", "index": 1, "total": 1, "safe": True},
                            load_saved_cycle_report(slot), slot))
    assert card.startswith("🟡 Цикл · cycle-001 · позиции закрыты, пара неполная")
    assert "MARKET исполнился о чужие 0.05 (счёт 777)" in card


def test_series_totals_count_every_fanout_account_once():
    from test_hood_telegram_series_report import fill, render, report
    fanout = report()
    fanout["binding"]["extra_receiver_account_indices"] = [33]
    fanout["confirmed_fills"] = [
        fill(11, "own-open", "0.0005", "50000", "SELL"),
        fill(22, "own-open", "0.0002", "50000"),
        fill(33, "own-open", "0.0003", "50000"),
    ]
    pair = report()
    pair["confirmed_fills"] = [fill(11, "pair-open", "0.001", "50000"), fill(22, "pair-open", "0.001", "50000", "SELL")]
    # 25 + 10 + 15 for the fan-out, 50 + 50 for the pair.
    result = render({"cycle-001": fanout, "cycle-002": pair})
    assert "Оборот всех счетов: 150.00 $" in result
    stranger = report()
    stranger["confirmed_fills"] = [fill(33, "foreign", "0.0003", "50000")]  # 33 is not in this 1 -> 1 cycle
    assert "Оборот обоих счетов: неизвестен" in render({"cycle-001": stranger})


def test_leverage_step_lists_every_fanout_account(tmp_path):
    from risex_spread_shadow.hood_handoff.journal import DurableJournal
    from risex_spread_shadow.hood_handoff.telegram_cards import cycle_steps
    tmp_path.chmod(0o700)
    journal = DurableJournal(tmp_path / "cycle.jsonl", clock=lambda: 1000.0)
    journal.acquire_attempt()
    journal.append("CYCLE_STARTED", {"binding": {"source_account_index": 11, "receiver_account_index": 22,
                                                 "extra_receiver_account_indices": [33, 44]}}, run_id="r")
    journal.append("LEVERAGE_PLAN", {"target_fraction_bps": {"11": 2500, "22": 5000, "33": 5000, "44": 10000},
                                     "observed_fraction_bps": {"11": 2500, "22": 2500, "33": 5000, "44": 10000}},
                   run_id="r")
    journal.release_attempt()
    steps = [text for _, _, _, text in cycle_steps(tmp_path)]
    assert steps[-1] == "ставлю плечо: 11 4.00x, 22 2.00x, 33 2.00x, 44 1.00x"


# ----- SDK and read stream ----------------------------------------------------------------

async def test_sdk_client_signs_only_for_the_cycle_source_and_its_receivers(monkeypatch):
    import time
    from risex_spread_shadow.hood_handoff import ContractError, StaticSecretProvider
    from risex_spread_shadow.hood_handoff.sdk import LighterSdkClient
    from test_hood_handoff_sdk_interface import FakeHttp, FakeModule, FakeSigner, _sdk_config

    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))

    def make(extras):
        client = LighterSdkClient(
            _sdk_config("fanout-sdk"), source_account_index=11, receiver_account_index=22,
            secrets=StaticSecretProvider({11: "s", 22: "r", 33: "r2", 44: "r3"}), market_evidence={},
            extra_receiver_account_indices=extras, signer_factory=lambda **kwargs: FakeSigner(**kwargs),
            http_factory=FakeHttp)
        client._lighter = lambda: FakeModule
        return client

    for bad in ((22,), (11,), (33, 33), (True,), (-1,)):
        with pytest.raises(ContractError, match="must all differ"):
            make(bad)
    pair, fanout = make(()), make((33,))
    assert pair._mutation_accounts == {11, 22} and fanout._mutation_accounts == {11, 22, 33}
    deadline = time.monotonic() + 1
    token = await fanout.reserve_order_nonce(33, deadline=deadline)
    await fanout.invalidate_reserved_nonce(token)
    for client, account in ((pair, 33), (fanout, 44)):
        with pytest.raises(ContractError, match="nonce reservation account"):
            await client.reserve_order_nonce(account, deadline=deadline)
        with pytest.raises(ContractError, match="leverage setting identity"):
            await client.update_leverage_fraction(account, 7, 5000, 0)
        with pytest.raises(ContractError):
            await client.prepare_cancel_order(account, 7, "99")
    cancel = await fanout.prepare_cancel_order(33, 7, "99")
    assert fanout._signers[33].cancel_calls
    await fanout.invalidate_prepared_order(cancel)
    assert fanout._http.calls == [] and pair._http.calls == []


def test_read_stream_covers_every_fanout_account_and_receiver_role(tmp_path):
    from risex_spread_shadow.hood_handoff.stream_evidence import StreamEvidenceJournal
    from risex_spread_shadow.hood_handoff.stream_measurement import StreamIdentity
    from risex_spread_shadow.hood_handoff.stream_state import ReadStreamState
    base = {"market_id": 1, "market_symbol": "BTC", "source_account_index": 27331,
            "receiver_account_index": 27337, "api_key_index": 4, "environment": "robinhood",
            "api_base_url": "https://api.rh.lighter.xyz", "chain_id": 466324}
    assert StreamIdentity.from_config(base).accounts == (27331, 27337)
    identity = StreamIdentity.from_config({**base, "extra_receiver_account_indices": [34019, 34020]})
    assert identity.accounts == (27331, 27337, 34019, 34020)
    for bad in ([27337], [34019, 34019], [True], [-5]):
        with pytest.raises(ValueError):
            StreamIdentity.from_config({**base, "extra_receiver_account_indices": bad})
    state = ReadStreamState(identity)
    for account, role in ((27331, "source"), (27337, "receiver"), (34019, "receiver_2"), (34020, "receiver_3")):
        state.bind_order(account, 1, 7, run_id="r", phase="PAIRED_OPENING", role=role, attempt_index=1)
        assert state.order_contexts[(account, 7)]["role"] == role
    for account, role in ((34020, "receiver_17"), (34020, "receiver_1"), (34020, "market"), (40000, "receiver")):
        with pytest.raises(ValueError):
            state.bind_order(account, 1, 8, run_id="r", phase="PAIRED_OPENING", role=role, attempt_index=1)
    tmp_path.chmod(0o700)
    journal = StreamEvidenceJournal(tmp_path / "events.jsonl", market_id=1, accounts=identity.accounts)
    assert journal.accounts == identity.accounts
    for bad in ((27331,), (27331, 27331, 27337), tuple(range(1, 19))):
        with pytest.raises(ValueError):
            StreamEvidenceJournal(tmp_path / "bad.jsonl", market_id=1, accounts=bad)


# ----- dispatch revalidation ---------------------------------------------------------------

def revalidation_context(tmp_path, balances, *, minimum_quote="10", parts=(15, 25)):
    from risex_spread_shadow.hood_handoff.fanout_cycle import FanoutCycleEngine, FanoutSelection
    from risex_spread_shadow.hood_handoff.journal import DurableJournal
    from risex_spread_shadow.hood_handoff.random_cycle import OpeningMarginReserve
    from test_hood_handoff_random_cycle import book
    from test_hood_margin_reserve import _margin_account
    clock = AdvancingClock()
    initial_market = replace(metadata(), mark_price=Decimal("100"), minimum_initial_margin_fraction=2500,
                             market_margin_mode=0)
    fresh_market = replace(initial_market, minimum_quote_amount=Decimal(minimum_quote))
    initial = {index: _margin_account(index, "20") for index in (11, 22, 33)}

    class Client:
        async def market_metadata(self, market_id):
            return replace(fresh_market, observed_at=clock.now())

        async def order_book(self, market_id):
            return replace(book(), observed_at=clock.now())

        async def account_snapshot(self, index, market_id):
            return replace(_margin_account(index, balances.get(index, "20")), observed_at=clock.now())

    engine = FanoutCycleEngine(Client(), clock=clock, rng=FixedRng())
    engine._leverage_fractions = {11: 2500, 22: 2500, 33: 2500}
    config = fanout_config(tmp_path / "cycle", margin_reserve=OpeningMarginReserve(Decimal(".10"), Decimal(".02")))
    engine._config = config
    bounds = compute_fanout_bounds(initial_market, initial[11], (initial[22], initial[33]), Decimal("100.1"),
                                   receiver_bound=Decimal("100.1"), direction=Direction.LONG)
    total = sum(parts)
    selection = FanoutSelection(
        quantity=total * Decimal("0.01"), quantity_tick=total, hold_seconds=29,
        opening_source_price=Decimal("100.1"), opening_receiver_bound=Decimal("100.1"), bounds=bounds.quantity,
        metadata_observed_at=1000, book_observed_at=1000, receiver_accounts=(22, 33), receiver_part_ticks=parts,
        part_minimum_tick=bounds.part_minimum_tick, receiver_cap_ticks=bounds.receiver_cap_ticks)
    journal = DurableJournal(config.journal_path, clock=clock.now)
    return engine, config, journal, selection, initial_market, initial


async def test_dispatch_resize_keeps_the_ten_percent_floor(tmp_path):
    from risex_spread_shadow.hood_handoff.random_cycle import _OpeningQuantityRefresh
    from test_hood_handoff_random_cycle import book
    # Independent receiver unit at 4x, mark 100, BUY bound 100.1: 25 + taker 100.1 x 0.00035 + adverse 0.1.
    unit = Decimal("100") * Decimal("0.25") + Decimal("100.1") * Decimal("0.00035") + Decimal("0.1")
    assert unit == Decimal("25.135035")
    balance = Decimal("2.80")
    planning = int((balance - Decimal(".10")) / unit / Decimal("0.01"))   # 10 ticks each: 20 < floor 22
    dispatch = int((balance - Decimal(".02")) / unit / Decimal("0.01"))   # 11 ticks each: 22 = floor
    assert (planning, dispatch) == (10, 11) and fanout_quantity_floor(2, 10) == 22
    engine, config, journal, selection, market, initial = revalidation_context(
        tmp_path, {22: str(balance), 33: str(balance)})
    with pytest.raises(_OpeningQuantityRefresh) as err:
        await engine._revalidate_fanout(config, selection, journal, market, book(), initial[11],
                                        (initial[22], initial[33]))
    resized = err.value.selection
    assert resized.receiver_part_ticks == (11, 11) and resized.quantity == Decimal("0.22")


async def test_dispatch_revalidation_refuses_a_limit_below_the_fresh_floor(tmp_path):
    from test_hood_handoff_random_cycle import book
    # A quote minimum of 11 at 100.1 makes one part 11 ticks: the floor becomes ceil(2 x 11 x 1.1) = 25.
    assert fanout_quantity_floor(2, 11) == 25
    engine, config, journal, selection, market, initial = revalidation_context(
        tmp_path, {}, minimum_quote="11", parts=(12, 12))
    with pytest.raises(PreflightBlocked, match="minimum plus 10 %"):
        await engine._revalidate_fanout(config, selection, journal, market, book(), initial[11],
                                        (initial[22], initial[33]))
