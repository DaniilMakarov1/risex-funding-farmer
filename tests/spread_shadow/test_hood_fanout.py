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
