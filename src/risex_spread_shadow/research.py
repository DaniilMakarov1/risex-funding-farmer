"""Offline RP2 recording -> accepted S1b; no network or execution clients."""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Iterator

from risex_farmer.models import Venue
from .book_chain import BookRevisionReconstructor
from .causal import CausalEvent
from .cycle import CyclePolicy, CycleScenario, CycleKernelState, CycleTerminalState
from .models import QuotePolicy
from .recording import build_recording_readback, RecordingReadbackError, _Availability
from .s1b import (Scv1S1bKernel, CycleFillModel, _d1_current_books,
                  _d1_recomputed_version, _d1_result_row, _d1_summary)
from .s3_cycle import _market_from_dict, _trade_from_dict, _gap_from_dict
from .store import iter_records

SECOND = 1_000_000_000


def _identity(path: Path) -> dict:
    digest = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
            size += len(chunk)
    return {'path': str(path.resolve()), 'bytes': size, 'sha256': digest.hexdigest()}


def _metadata(path: Path) -> tuple[dict, QuotePolicy, int, int]:
    metadata = None
    start_record = None
    terminal = None
    for record in iter_records(path):
        if record['kind'] == 'RUN_METADATA':
            metadata = record['metadata']
        elif record['kind'] == 'RUN_START':
            if start_record is not None:
                raise RecordingReadbackError('DUPLICATE_RUN_START')
            start_record = record
        elif record['kind'] in {'RUN_STOP', 'RUN_FAILED'}:
            terminal = record
    if metadata is None or start_record is None or terminal is None:
        raise RecordingReadbackError('MISSING_RESEARCH_METADATA')
    pairs = start_record.get('market_metadata')
    if not isinstance(pairs, list) or len(pairs) != 1:
        raise RecordingReadbackError('RESEARCH_REQUIRES_ONE_BTC_PAIR')
    markets = []
    for name, venue in [('risex', Venue.RISEX), ('lighter', Venue.LIGHTER)]:
        raw = pairs[0].get(name)
        if not isinstance(raw, dict) or any(type(raw.get(k)) is not bool for k in ('is_active', 'is_rfq', 'is_off_hours')):
            raise RecordingReadbackError('INVALID_MARKET_SAFETY_FIELDS')
        market = _market_from_dict(raw, context='research ' + name)
        if market is None or market.venue != venue or market.canonical_asset != 'BTC':
            raise RecordingReadbackError('RESEARCH_MARKET_MISMATCH')
        markets.append(market)
    frozen = CyclePolicy()
    names = ('canonical_market', 'direction', 'target_notional_usd', 'target_margin_bps',
             'risex_maker_fee_rate', 'lighter_taker_fee_rate', 'risex_fee_source', 'lighter_fee_source')
    policy = QuotePolicy(**{name: getattr(frozen, name) for name in names},
                         risex_market=markets[0], lighter_market=markets[1])
    planned = start_record.get('duration_seconds')
    if planned is not None and (type(planned) is not int or not 1 <= planned <= 900):
        raise RecordingReadbackError('INVALID_PLANNED_DURATION')
    if metadata.get('evidence_mode') == 'OBSERVATIONAL' and planned not in (60, 900):
        raise RecordingReadbackError('PUBLIC_RECORDING_DURATION_NOT_FROZEN')
    metadata = dict(metadata, planned_duration_seconds=planned)
    start = start_record.get('observed_monotonic_ns') or metadata['started_monotonic_ns']
    end = terminal['observed_monotonic_ns']
    if end < start or end - start > 960 * SECOND:
        raise RecordingReadbackError('RESEARCH_WINDOW_OUT_OF_BOUNDS')
    return metadata, policy, start, end


def _events(path: Path, start: int, end: int) -> Iterator[tuple[int, int, CausalEvent]]:
    """Physical processing order. BOOK is usable only after its CHANGED outcome.

    Derived decision readiness includes the observed processing result; original
    receipt/normalization timestamps and book identity remain unchanged.
    """
    chain = BookRevisionReconstructor()
    pending = {}
    receipts = {}
    cursor = start
    for record in iter_records(path):
        kind = record['kind']
        payload = None
        ready = None
        if kind == 'PUBLIC_RECEIPT':
            receipts[record['receipt_id']] = record
        elif kind == 'BOOK':
            book = chain.append(record)
            pending[book.book_revision_id] = book
        elif kind == 'PUBLIC_RECEIPT_OUTCOME' and record['processing_outcome'] == 'CHANGED':
            book = pending.pop(record['linked_book_revision_id'])
            ready = max(record['processing_ready_monotonic_ns'], book.received_monotonic_ns,
                        book.normalized_ready_monotonic_ns, book.decision_ready_monotonic_ns or 0)
            payload = replace(book, decision_ready_monotonic_ns=max(cursor, ready))
        elif kind == 'RISEX_TRADE':
            if type(record.get('admissible')) is not bool:
                raise RecordingReadbackError('TRADE_ADMISSIBILITY_MISSING')
            trade = _trade_from_dict(record)
            receipt = receipts.get(record.get('receipt_id'))
            if receipt is None or any(receipt.get(k) != record.get(k) for k in
                    ('venue', 'stream_session_id', 'recovery_generation', 'received_monotonic_ns')):
                raise RecordingReadbackError('TRADE_RECEIPT_LINK_INVALID')
            if trade.ingress_received_monotonic_ns is None or trade.normalized_ready_monotonic_ns is None:
                raise RecordingReadbackError('TRADE_TIMING_MISSING')
            if trade.venue != Venue.RISEX or trade.canonical_market != 'BTC':
                raise RecordingReadbackError('TRADE_MARKET_MISMATCH')
            # Old records without an application processing timestamp cannot
            # prove when queued trades became available to the model.
            processing = record.get('processing_ready_monotonic_ns')
            if type(processing) is not int or processing < trade.received_monotonic_ns:
                raise RecordingReadbackError('TRADE_PROCESSING_TIME_UNPROVEN')
            ready = max(processing, trade.normalized_ready_monotonic_ns or processing)
            if record['admissible']:
                payload = replace(trade, decision_ready_monotonic_ns=max(cursor, ready))
        elif kind == 'DATA_GAP':
            gap = _gap_from_dict({**record, 'source_venue': record['venue']})
            if gap.canonical_market != 'UNKNOWN':
                chain.mark_gap(venue=gap.source_venue, market=gap.canonical_market,
                               session=gap.stream_session_id, recovery=gap.recovery_generation)
            ready = max(cursor, gap.gap_start_monotonic_ns, record.get('processing_ready_monotonic_ns', 0))
            # The collector subscribes to one BTC pair; an unclassified
            # venue-level loss invalidates that venue's BTC evidence too.
            payload = replace(gap, canonical_market='BTC') if gap.canonical_market == 'UNKNOWN' else gap
        if ready is not None:
            cursor = max(cursor, ready)
            if cursor > end:
                raise RecordingReadbackError('EVENT_AFTER_TERMINAL')
        if payload is not None:
            yield cursor, record['record_index'], CausalEvent(payload)
    if pending:
        raise RecordingReadbackError('BOOK_PROCESSING_RESULT_MISSING')


class _ResearchAvailability(_Availability):
    def __init__(self, start: int, policy: QuotePolicy):
        super().__init__(start)
        self.policy = policy

    def reason(self) -> str:
        _, reasons = _d1_current_books(
            {venue: (CausalEvent(book), book) for venue, book in self.books.items()},
            self.at, self.policy, CyclePolicy())
        return reasons[0] if reasons else 'VALID'


def _run_lane(path: Path, policy: QuotePolicy, start: int, end: int,
              cutoff: int, model: CycleFillModel, scenario: CycleScenario) -> dict:
    began = time.perf_counter_ns()
    kernel = Scv1S1bKernel(fill_model=model)
    latest = {}
    availability = _ResearchAvailability(start, policy)
    decision = start + SECOND
    attempts = opportunities = admissions = 0
    reasons = Counter()
    decisions = []
    blocked_at = None
    blocked_reasons = []
    first_admission = None

    def observe_block() -> None:
        nonlocal blocked_at, blocked_reasons
        for result in kernel.retained_results(scenario):
            if result.status is CycleTerminalState.UNRESOLVED or result.policy_blocked:
                at = result.terminal_monotonic_ns
                if at is not None and (blocked_at is None or at < blocked_at):
                    blocked_at = at
                    blocked_reasons = list(result.reason_codes)

    def deliver(item) -> None:
        last = kernel.last_result(scenario)
        is_clock = isinstance(item, int)
        if (kernel.state(scenario) is CycleKernelState.PENDING
                or (last is not None and (not is_clock or last.policy_blocked))):
            kernel.advance(item, scenario=scenario)
            observe_block()

    def tick(at: int) -> None:
        nonlocal attempts, opportunities, admissions, first_admission, blocked_at, blocked_reasons
        deliver(at)
        if at >= cutoff:
            return
        attempts += 1
        books, why = _d1_current_books(latest, at, policy, CyclePolicy())
        row = {'decision_monotonic_ns': at, 'reasons': list(why), 'accepted': False}
        if books is not None:
            version, quote, reason = _d1_recomputed_version(policy, books, at, attempts,
                                                           datetime(1970, 1, 1, tzinfo=timezone.utc))
            row['source_book_revision_ids'] = [book.book_revision_id for book in books]
            if version is not None:
                opportunities += 1
                version = replace(version, version_id=f'RP3-{attempts:08d}')
                result = kernel.admit(version, scenario=scenario, source_books=books)
                admissions += int(result.accepted)
                row.update(accepted=result.accepted, reasons=[] if result.accepted else [result.reason])
                if result.accepted and first_admission is None:
                    first_admission = at
                if result.reason == 'TERMINAL_RETENTION_EXHAUSTED' and blocked_at is None:
                    blocked_at, blocked_reasons = at, [result.reason]
                observe_block()
            else:
                row['reasons'] = [reason or 'QUOTE_UNAVAILABLE']
        reasons.update(row['reasons'])
        decisions.append(row)  # <= 900 bounded model decisions

    for at, index, event in _events(path, start, end):
        while decision < at and decision <= end:
            tick(decision)
            decision += SECOND
        availability.advance(at)
        if event.book is not None:
            latest[event.venue] = (event, event.book)
            availability.books[event.venue] = event.book
        elif event.gap is not None:
            latest.pop(event.venue, None)
            availability.books.pop(event.venue, None)
        deliver(event)
    while decision <= end:
        tick(decision)
        decision += SECOND
    availability.advance(end)
    deliver(end)
    if kernel.last_result(scenario) is not None:
        kernel.finish(scenario=scenario, end_monotonic_ns=end)
    observe_block()
    results = kernel.retained_results(scenario)
    summary = _d1_summary(results)
    summary['forced_fill_count'] = sum('forced' in fill.action_id.lower() for result in results for fill in result.fills)
    summary.update(decision_attempts=attempts, opportunities=opportunities, accepted_quotes=admissions,
                   admission_block_reasons=dict(sorted(reasons.items())),
                   first_admission_monotonic_ns=first_admission, blocked_at_monotonic_ns=blocked_at,
                   lane_available_before_block_duration_ns=(blocked_at or end) - start,
                   lane_active_until_block_duration_ns=0 if first_admission is None else max(0, (blocked_at or end) - first_admission),
                   lane_blocked_duration_ns=0 if blocked_at is None else end - blocked_at,
                   lane_block_reasons=blocked_reasons,
                   data_valid_duration_ns=availability.durations.get('VALID', 0),
                   data_eligibility_duration_ns=dict(sorted(availability.durations.items())))
    episodes = []
    for result in results:
        row = _d1_result_row(result)
        row['terminal_monotonic_ns'] = result.terminal_monotonic_ns
        row['fills'] = [asdict(fill) for fill in result.fills]
        episodes.append(row)
    return {'fill_model': model.value, 'scenario': scenario.value, 'summary': summary,
            'episodes': episodes, 'decisions': decisions,
            'offline_compute_duration_ns': time.perf_counter_ns() - began}


def build_research_report(path: str | Path) -> dict:
    path = Path(path)
    began = time.perf_counter_ns()
    identity = _identity(path)
    readback = build_recording_readback(path)
    metadata, policy, start, end = _metadata(path)
    # Frozen 900-second campaign admits/requotes only before 12:45. Short
    # technical/fixture files use their whole duration; no fictitious tail.
    planned = metadata.get('planned_duration_seconds')
    pilot = planned == 900 or end - start >= 900 * SECOND
    cutoff = min(end, start + 765 * SECOND) if pilot else end
    lanes = [_run_lane(path, policy, start, end, cutoff, model, scenario)
             for model in CycleFillModel for scenario in CycleScenario]
    if _identity(path) != identity:
        raise RecordingReadbackError('SOURCE_CHANGED_DURING_REPORT')
    valid = lanes[0]['summary']['data_valid_duration_ns']
    technical = readback['recording_status']
    if any('TERMINAL_RETENTION_EXHAUSTED' in lane['summary']['lane_block_reasons'] for lane in lanes):
        technical = 'INCOMPLETE'
    return {'report_kind': 'RP3_SCV1_S1B_RESEARCH', 'source': identity,
            'evidence_mode': metadata.get('evidence_mode', 'UNKNOWN'),
            'implementation_sha256': {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                for name in ('research.py', 's1b.py', 'cycle.py', 'causal.py', 'models.py',
                             'economics.py', 'recording.py', 'book_chain.py')},
            'recording_source_commit': metadata.get('source_commit', 'UNKNOWN'),
            'policy': asdict(CyclePolicy()), 'market_metadata': [asdict(policy.risex_market), asdict(policy.lighter_market)],
            'technical_status': technical, 'readback': readback,
            'start_monotonic_ns': start, 'end_monotonic_ns': end,
            'entry_requote_cutoff_monotonic_ns': cutoff,
            'collector_duration_ns': end - start, 'data_valid_duration_ns': valid,
            'data_ineligible_duration_ns': end - start - valid,
            'economic_conclusion': ('Данная модель пока не проверяется этим потоком' if valid * 2 < end - start
                                    else 'Условная офлайн-модель; вывод ограничен данным образцом'),
            'alternatives': lanes, 'funding': 'UNKNOWN', 'points_usd': '0',
            'limitations': ['Public-only paper model; no queue-position or actual execution proof',
                            'Unchanged messages do not refresh economic book timestamps',
                            'Processing chronology is distinct from offline compute duration',
                            'Upstream silence/network cause UNKNOWN unless observed'],
            'offline_compute_duration_ns': time.perf_counter_ns() - began}


def render_research_report(report: dict, *, format: str = 'table') -> str:
    if format == 'json':
        return json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if format != 'table':
        raise ValueError('unknown report format')
    seconds = lambda ns: f'{ns / SECOND:.3f}'
    lines = [f"{report['evidence_mode']} | Recording: {report['technical_status']} | SHA256: {report['source']['sha256']}",
             f"Collector {seconds(report['collector_duration_ns'])} s; eligible {seconds(report['data_valid_duration_ns'])} s; ineligible {seconds(report['data_ineligible_duration_ns'])} s", 
             '| Model / delay | Opportunities / attempts / admitted | Fills / closed / forced fills | Blocked s | Active until block s | Closed PnL $ | Fees $ | Stress $ | Open RISEx / Lighter | Offline s |',
             '|---|---:|---:|---:|---:|---:|---:|---:|---|---:|']
    for lane in report['alternatives']:
        s = lane['summary']
        residuals = [f"{e['positions']['risex_signed_quantity']} / {e['positions']['lighter_signed_quantity']}" for e in lane['episodes'] if not e['closed'] and (e['positions']['risex_signed_quantity'] != '0' or e['positions']['lighter_signed_quantity'] != '0')]
        lines.append(f"| {lane['fill_model']} / {lane['scenario']} | {s['opportunities']} / {s['decision_attempts']} / {s['accepted_quotes']} | {s['fills']} / {s['closed_count']} / {s['forced_fill_count']} | {seconds(s['lane_blocked_duration_ns'])} | {seconds(s['lane_active_until_block_duration_ns'])} | {s['closed_execution_pnl_usd']} | {s['fees_usd']} | {s['stress_cost_usd']} | {'; '.join(residuals) or '0 / 0'} | {seconds(lane['offline_compute_duration_ns'])} |")
    for lane in report['alternatives']:
        if lane['summary']['lane_block_reasons']:
            lines.append(f"{lane['fill_model']}/{lane['scenario']}: " + ', '.join(lane['summary']['lane_block_reasons']))
    lines += [report['economic_conclusion'], 'Funding UNKNOWN; points $0. Open inventory and marks are separate from closed PnL.']
    return '\n'.join(lines)
