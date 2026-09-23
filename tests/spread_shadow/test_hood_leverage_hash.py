"""Offline native-shaped leverage identity across durable and read boundaries."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff.contracts import ContractError, LeverageNotSent, PreflightBlocked
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff import operator_recovery
from risex_spread_shadow.hood_handoff.operator_recovery import RecoveryReadClient, prior_intents
from risex_spread_shadow.hood_handoff.sdk import _leverage_tx_hash
from test_hood_handoff_random_cycle import cycle_config
from test_hood_handoff_sdk_interface import (
    AmbiguousConstantHttp, ConstantNonceSigner, FakeModule, _constant_nonce_client,
)


NATIVE_SHAPE = 'aB' * 40  # Synthetic, exact observed native shape; never a generated hash.


def _history(operator: Path, tx_hash: str):
    slot = operator / 'cycle-001'
    slot.mkdir(mode=0o700, parents=True)
    journal = DurableJournal(slot / 'cycle.jsonl', clock=lambda: 1000)
    journal.acquire_attempt()
    try:
        intent = {'account_index': 11, 'market_id': 7, 'fraction_bps': 4166,
                  'margin_mode': 0, 'source_identity': 'cycle-account-11'}
        journal.append('LEVERAGE_UPDATE_INTENT', intent)
        journal.append('LEVERAGE_TX_PREPARED', {
            **intent, 'api_key_index': 4, 'tx_type': 20,
            'nonce': 41, 'tx_hash': tx_hash,
        })
    finally:
        journal.release_attempt()
    return slot


@pytest.mark.asyncio
async def test_native_shaped_hash_round_trips_prepared_journal_history_and_both_readers(tmp_path, monkeypatch):
    class Signer(ConstantNonceSigner):
        async def sign_update_leverage(self, **kwargs):
            return 20, 'synthetic-signed-body-not-durable', NATIVE_SHAPE, None

    client = _constant_nonce_client(prefix='native-hash-roundtrip', signer=Signer(),
                                    http_factory=AmbiguousConstantHttp)
    slot = tmp_path / 'cycle-001'
    slot.mkdir(mode=0o700)
    journal = DurableJournal(slot / 'cycle.jsonl', clock=lambda: 1000)
    journal.acquire_attempt()
    try:
        journal.append('LEVERAGE_UPDATE_INTENT', {
            'account_index': 11, 'market_id': 7, 'fraction_bps': 4166,
            'margin_mode': 0, 'source_identity': 'cycle-account-11',
        })
        with pytest.raises(TimeoutError):
            await client.update_leverage_fraction(
                11, 7, 4166, 0,
                prepared_intent=lambda identity: journal.append('LEVERAGE_TX_PREPARED', identity),
            )
    finally:
        journal.release_attempt()
    assert len(client._http.calls) == 1
    assert 'synthetic-signed-body-not-durable' not in (slot / 'cycle.jsonl').read_text()
    _, _, pending = prior_intents(tmp_path, cycle_config(slot, api_key_index=4))
    assert len(pending) == 1
    assert pending[0]['prepared_identity'] == {
        'hash': NATIVE_SHAPE, 'nonce': 41, 'account_index': 11, 'api_key_index': 4,
    }

    class TxApi:
        def __init__(self, api_client):
            pass

        async def tx(self, *, by, value, _request_timeout):
            assert (by, value) == ('hash', NATIVE_SHAPE)
            return {'code': 200, 'hash': NATIVE_SHAPE, 'type': 20, 'status': 2,
                    'account_index': 11, 'api_key_index': 4, 'nonce': 41,
                    'executed_at': 1000, 'committed_at': 1001, 'verified_at': 1002,
                    'info': 'private-field-must-not-return'}

        async def next_nonce(self, *, account_index, api_key_index, _request_timeout):
            assert (account_index, api_key_index) == (11, 4)
            return {'code': 200, 'nonce': 42}

    class Module(FakeModule):
        TransactionApi = TxApi

    client._lighter = lambda: Module
    sdk_read = await client.read_leverage_transaction(NATIVE_SHAPE)
    assert await client.read_leverage_next_nonce(11, 4) == 42
    assert sdk_read['hash'] == NATIVE_SHAPE
    assert 'info' not in sdk_read
    await client.aclose()

    calls = []

    class Http:
        def __init__(self, *args, **kwargs):
            pass

        async def get(self, path, *, params, authorization):
            calls.append((path, params, authorization))
            if path == 'api/v1/nextNonce':
                return {'code': 200, 'nonce': 42}
            return {'code': 200, **sdk_read, 'info': 'private-field-must-not-return'}

        async def aclose(self):
            pass

    monkeypatch.setattr(operator_recovery, 'PlainAioHttp', Http)
    reader = RecoveryReadClient.__new__(RecoveryReadClient)
    reader.config = SimpleNamespace(api_base_url='http://127.0.0.1:1', request_timeout_seconds=1,
                                    api_key_index=4)
    reader.source_account_index = 11
    reader.receiver_account_index = 22
    reader._clock = lambda: 2000

    async def authorization(index):
        assert index == 11
        return 'synthetic-authorization'

    reader._authorization = authorization
    assert await reader.read_leverage_transaction(NATIVE_SHAPE) == sdk_read
    assert await reader.read_leverage_next_nonce(11, 4) == 42
    assert calls == [('api/v1/tx', {'by': 'hash', 'value': NATIVE_SHAPE},
                      'synthetic-authorization'),
                     ('api/v1/nextNonce', {'account_index': 11, 'api_key_index': 4},
                      'synthetic-authorization')]


BAD_HASHES = [
    '0x' + 'a' * 64, 'a' * 79, 'a' * 81, 'g' * 80,
    'a' * 79 + ' ', 'a' * 80 + '\n', '{"signed":"payload"}', None,
]


@pytest.mark.parametrize('bad_hash', BAD_HASHES)
@pytest.mark.asyncio
async def test_malformed_hash_blocks_all_four_boundaries_before_any_transport(tmp_path, bad_hash):
    assert _leverage_tx_hash(bad_hash) is None

    class Signer(ConstantNonceSigner):
        async def sign_update_leverage(self, **kwargs):
            return 20, 'synthetic-signed-body', bad_hash, None

    client = _constant_nonce_client(prefix='invalid-native-hash', signer=Signer(),
                                    http_factory=AmbiguousConstantHttp)
    identities = []
    try:
        with pytest.raises(LeverageNotSent, match='before transport') as error:
            await client.update_leverage_fraction(11, 7, 4166, 0, prepared_intent=identities.append)
        assert isinstance(error.value.__cause__, ContractError)
        assert not identities and not client._http.calls
        with pytest.raises(ContractError, match='hash is invalid'):
            await client.read_leverage_transaction(bad_hash)
    finally:
        await client.aclose()

    reader = RecoveryReadClient.__new__(RecoveryReadClient)
    with pytest.raises(ContractError, match='hash is invalid'):
        await reader.read_leverage_transaction(bad_hash)

    slot = _history(tmp_path, bad_hash)
    with pytest.raises(PreflightBlocked, match='prepared leverage transaction conflicts'):
        prior_intents(tmp_path, cycle_config(slot, api_key_index=4))
