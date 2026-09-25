"""History-scan bounds: the shared recovery journal outgrew the per-slot 32 MiB
limit on 2026-09-25 (35,110,061 bytes) and every READY check then refused
/run with a generic PREFLIGHT_REFUSED.  These checks pin the corrected
behaviour: bounded streaming of the shared journal, compact persisted proofs,
explicit HISTORY_LIMIT diagnostics and unchanged trading-journal strictness."""
import hashlib
import json
import os
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import operator_recovery as r
from risex_spread_shadow.hood_handoff.cli import _persist_prejournal_launch_failure
from risex_spread_shadow.hood_handoff.contracts import PreflightBlocked
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.operator_view import read_launch_failure
from risex_spread_shadow.hood_handoff.telegram_control import admission_failure_category
from risex_spread_shadow.hood_handoff.telegram_messages import admission_refusal_message, launch_failure_message
from test_hood_handoff_random_cycle import AdvancingClock, cycle_config
from test_hood_operator_recovery import RecoveryClient
from test_hood_shared_recovery import Client, seed

OLD_LIMIT = 32 * 1024 * 1024


def _pad_shared_journal(path, target_bytes):
    """Append well-formed CURRENT_STATE_VERIFIED rows (each < 2 MiB) directly."""
    rows = path.read_text().splitlines() if path.exists() else []
    last = json.loads(rows[-1]) if rows else {'sequence': 0, 'at': 1000.0}
    sequence, at = last['sequence'], last['at']
    blob = 'x' * (1900 * 1024)
    with path.open('a', encoding='utf-8') as stream:
        while path.stat().st_size <= target_bytes:
            sequence += 1
            row = {'sequence': sequence, 'run_id': f'pad-{sequence}', 'event': 'CURRENT_STATE_VERIFIED',
                   'at': at, 'payload': {'status': 'CLOSE_READY', 'padding': blob}}
            stream.write(json.dumps(row, separators=(',', ':')) + '\n')
            stream.flush()


@pytest.mark.asyncio
async def test_ready_survives_shared_journal_beyond_old_32mib_limit(tmp_path):
    cfg = seed(tmp_path)
    clock = AdvancingClock()
    client = Client(clock)
    # First READY writes the exact leverage checkpoint into the shared journal.
    first = await r.inspect_current(cfg, client, tmp_path, clock=clock, require_flat=True)
    assert first[2]['status'] == 'READY' and client.tx_reads == 1
    path = tmp_path / 'recovery-checks.jsonl'
    _pad_shared_journal(path, OLD_LIMIT)
    assert path.stat().st_size > OLD_LIMIT  # the production size class at cycle-231
    again = await r.inspect_current(cfg, client, tmp_path, clock=clock, require_flat=True)
    assert again[2]['status'] == 'READY'
    assert client.tx_reads == 1  # the checkpoint was read, not re-proved or replayed
    assert not client.submissions


def test_shared_journal_bound_is_finite_and_explicit(tmp_path, monkeypatch):
    path = tmp_path / 'recovery-checks.jsonl'
    _pad_shared_journal(path, 3 * 1024 * 1024)
    monkeypatch.setattr(r, 'SHARED_RECOVERY_JOURNAL_MAX_BYTES', 2 * 1024 * 1024)
    with pytest.raises(r.HistoryBoundExceeded, match='too large') as err:
        r._leverage_checkpoints(tmp_path)
    assert isinstance(err.value, PreflightBlocked)
    assert admission_failure_category(err.value) == 'HISTORY_LIMIT'


def test_trading_journal_keeps_its_strict_bound(tmp_path, monkeypatch):
    path = tmp_path / 'cycle.jsonl'
    j = DurableJournal(path, clock=lambda: 1000)
    j.acquire_attempt()
    try:
        j.append('CYCLE_STARTED', {'padding': 'x' * 4096})
    finally:
        j.release_attempt()
    monkeypatch.setattr(r, 'HISTORICAL_JOURNAL_MAX_BYTES', 1024)
    with pytest.raises(r.HistoryBoundExceeded):
        list(r.journal_rows(path))
    # A trading journal is never read with the shared-journal allowance.
    monkeypatch.setattr(r, 'SHARED_RECOVERY_JOURNAL_MAX_BYTES', 10 ** 9)
    with pytest.raises(r.HistoryBoundExceeded):
        list(r.journal_rows(path))


def test_symlinked_history_is_still_unsafe_not_a_bound(tmp_path):
    real = tmp_path / 'real.jsonl'
    real.write_text('')
    link = tmp_path / 'cycle.jsonl'
    os.symlink(real, link)
    with pytest.raises(PreflightBlocked, match='unsafe') as err:
        list(r.journal_rows(link))
    assert not isinstance(err.value, r.HistoryBoundExceeded)


def _tiny_journals(operator, count):
    slot = operator / 'cycle-001'
    slot.mkdir(mode=0o700)
    for index in range(count):
        path = slot / f'extra-{index:05d}.jsonl'
        row = {'sequence': 1, 'run_id': f'run-{index}', 'event': 'CYCLE_STARTED', 'at': 1000.0, 'payload': {}}
        path.write_text(json.dumps(row) + '\n')


def test_more_than_old_2000_journals_are_scanned(tmp_path):
    _tiny_journals(tmp_path, 2001)
    cfg = cycle_config(tmp_path / 'cycle-999')
    intents, files, pending = r.prior_intents(tmp_path, cfg)
    assert len(files) == 2001 and intents == [] and pending == []


def test_file_count_bound_is_explicit(tmp_path, monkeypatch):
    _tiny_journals(tmp_path, 12)
    monkeypatch.setattr(r, 'HISTORICAL_JOURNAL_MAX_FILES', 10)
    with pytest.raises(r.HistoryBoundExceeded, match='count'):
        r.prior_intents(tmp_path, cycle_config(tmp_path / 'cycle-999'))


def test_persisted_proof_is_compact_exact_and_does_not_mutate_caller():
    inputs = [{'path': f'/h/cycle-{i:03d}/cycle.jsonl', 'sha256': hashlib.sha256(i.to_bytes(2, 'big')).hexdigest()}
              for i in reversed(range(300))]
    proof = {'status': 'READY', 'at': 1.0, 'previous_intents': 3, 'inputs': inputs}
    stored = r.persisted_proof(proof)
    assert proof['inputs'] is inputs and 'inputs' not in stored
    assert stored['inputs_count'] == 300
    # Independent digest: canonical JSON of the path-sorted list.
    ordered = sorted(inputs, key=lambda item: item['path'])
    expected = hashlib.sha256(json.dumps(ordered, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert stored['inputs_sha256'] == expected
    assert len(json.dumps(stored)) < 400
    # Order of discovery does not change the digest; a changed hash does.
    assert r.persisted_proof({**proof, 'inputs': ordered})['inputs_sha256'] == expected
    changed = [dict(item) for item in inputs]
    changed[0]['sha256'] = '0' * 64
    assert r.persisted_proof({**proof, 'inputs': changed})['inputs_sha256'] != expected


@pytest.mark.asyncio
async def test_check_recovery_journals_compact_rows_and_returns_full_proof(tmp_path, monkeypatch):
    cfg = seed(tmp_path)
    clock = AdvancingClock()
    # Real READY proof from the production inspector, then routed through the
    # production check_recovery persistence path (credentials/transport faked).
    _, _, real = await r.inspect_current(cfg, Client(clock), tmp_path, clock=clock, require_flat=True)
    assert real['status'] == 'READY' and len(real['inputs']) == 1

    async def inspect(*args, **kwargs):
        return None, None, real

    class Secrets:
        def close(self):
            pass

    class Closable:
        async def aclose(self):
            pass

    monkeypatch.setattr(r, 'inspect_current', inspect)
    returned = await r.check_recovery(cfg, tmp_path, require_flat=True,
                                      client_factory=lambda *a, **k: Closable(),
                                      secret_factory=lambda *a, **k: Secrets())
    assert returned is real and len(returned['inputs']) == 1
    rows = [json.loads(line) for line in (tmp_path / 'recovery-checks.jsonl').read_text().splitlines()]
    verified = [row for row in rows if row['event'] == 'CURRENT_STATE_VERIFIED']
    assert len(verified) == 1
    payload = verified[0]['payload']
    assert 'inputs' not in payload and payload['inputs_count'] == 1
    digest = hashlib.sha256(json.dumps(real['inputs'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert payload['inputs_sha256'] == digest
    assert payload['status'] == 'READY' and payload['source']['signed_position'] == '0'
    # The compact row is still accepted by the strict shared reader.
    assert r._leverage_checkpoints(tmp_path)


@pytest.mark.asyncio
async def test_close_baseline_is_compact_and_close_still_flat(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock, '.2', '-.2')
    slot = r.allocate_close_slot(tmp_path)
    result = await r.close_positions(cycle_config(slot), client, tmp_path, slot, clock=clock)
    assert result['status'] == 'CONFIRMED_FLAT'
    rows = [json.loads(line) for line in (slot / 'close.jsonl').read_text().splitlines()]
    baseline = [row['payload'] for row in rows if row['event'] == 'CLOSE_BASELINE'][0]
    assert 'inputs' not in baseline and baseline['inputs_count'] >= 1
    assert [p.quantity for p in client.submissions] == [Decimal('.2'), Decimal('.2')]


def test_history_limit_launch_code_and_messages(tmp_path):
    slot = tmp_path / 'cycle-231'
    slot.mkdir(mode=0o700)
    code = _persist_prejournal_launch_failure(slot, r.HistoryBoundExceeded('historical journal is too large'))
    assert code == 'HISTORY_LIMIT'
    saved = read_launch_failure(slot)
    assert saved == 'HISTORY_LIMIT'
    assert 'предел проверки' in launch_failure_message('cycle-231', 'HISTORY_LIMIT')
    text = admission_refusal_message({'status': 'REFUSED', 'update_id': 1, 'stage': 'READY',
                                      'category': 'HISTORY_LIMIT', 'at': 1.0})
    assert 'предел проверки' in text and 'HISTORY_LIMIT' in text
    # Other preflight refusals keep their existing generic category.
    assert admission_failure_category(PreflightBlocked('SECRET')) == 'PREFLIGHT_REFUSED'
    other = tmp_path / 'cycle-232'
    other.mkdir(mode=0o700)
    assert _persist_prejournal_launch_failure(other, PreflightBlocked('x')) == 'PREFLIGHT_REFUSED'
