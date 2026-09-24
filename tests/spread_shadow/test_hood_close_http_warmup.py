import asyncio
import json
from types import SimpleNamespace

import pytest
from aiohttp import web

from risex_spread_shadow.hood_handoff import Outcome, run_random_cycle
from risex_spread_shadow.hood_handoff.random_cycle import RandomCycleEngine
from risex_spread_shadow.hood_handoff.sdk import LighterSdkClient, PlainAioHttp
from test_hood_handoff_random_cycle import AdvancingClock, CycleClient, FixedRng, cycle_config


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ok", "error", "slow"])
async def test_cycle_warmup_is_close_only_optional_and_drained_before_quote(tmp_path, mode):
    class Client(CycleClient):
        calls = 0
        finished = False

        async def warm_mutation_http(self):
            self.calls += 1
            assert len(self.submissions) == 2
            assert self.source_position != 0 and self.receiver_position != 0
            try:
                if mode == "error":
                    raise RuntimeError("PRIVATE_ERROR")
                if mode == "slow":
                    await asyncio.Event().wait()
            finally:
                self.finished = True

        async def order_book(self, market_id):
            if self.calls:
                assert self.finished
            return await super().order_book(market_id)

    clock = AdvancingClock(); client = Client(clock)
    cfg = cycle_config(tmp_path / "cycle")
    result = await asyncio.wait_for(run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20, 20)), 2)
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == "CONFIRMED_FLAT"
    assert client.calls == 1 and client.finished and len(client.submissions) == 4
    rows = [json.loads(line) for line in cfg.journal_path.read_text().splitlines()]
    plan, = [r for r in rows if r["event"] == "CLOSING_PLAN_READY"]
    assert plan["payload"]["http_warmup"]["status"] == {
        "ok": "completed", "error": "failed", "slow": "unfinished_read_cancelled"}[mode]
    assert "PRIVATE_ERROR" not in cfg.journal_path.read_text()


@pytest.mark.asyncio
async def test_required_read_cancellation_drains_warmup_and_sends_nothing(tmp_path):
    started = asyncio.Event(); drained = asyncio.Event()
    class Client(CycleClient):
        async def warm_mutation_http(self):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                drained.set()
        async def market_metadata(self, market_id):
            await asyncio.Event().wait()
    clock = AdvancingClock(); client = Client(clock)
    engine = RandomCycleEngine(client, clock=clock)
    task = asyncio.create_task(engine._parallel_revalidation_context(cycle_config(tmp_path / "cycle"),
        market_read_label="closing market read", warm_closing_transport=True))
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert drained.is_set() and not client.submissions


@pytest.mark.asyncio
@pytest.mark.parametrize("peer_closes", [False, True])
async def test_public_warmup_uses_send_pool_and_peer_close_never_replays(peer_closes):
    requests = []
    async def handle(request):
        requests.append((request.method, request.path, request.transport,
                         request.headers.get("Authorization"), dict(request.query)))
        reply = web.json_response({"code": 200})
        if request.method == "GET" and peer_closes:
            reply.force_close()
        return reply
    app = web.Application(); app.router.add_get('/api/v1/orderBookDetails', handle)
    app.router.add_post('/api/v1/sendTx', handle)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0); await site.start()
    port = site._server.sockets[0].getsockname()[1]
    http = PlainAioHttp(f'http://127.0.0.1:{port}', timeout_seconds=1)
    client = SimpleNamespace(_http=http, _warmed_ws_sender=None, config=SimpleNamespace(market_id=1))
    try:
        await LighterSdkClient.warm_mutation_http(client)
        assert len(requests) == 1 and requests[0][:2] == ('GET', '/api/v1/orderBookDetails')
        assert requests[0][3] == '' and requests[0][4] == {'market_id': '1'}
        result = await http.post_form('api/v1/sendTx', form={'synthetic': 'not_a_signed_transaction'})
        assert len(requests) == 2 and requests[1][0] == 'POST'
        if not peer_closes:
            assert requests[0][2] is requests[1][2]
            assert result['_transport_timing']['http_connection_reused'] == 1
        else:
            assert requests[0][2] is not requests[1][2]
    finally:
        await http.aclose(); await runner.cleanup()


@pytest.mark.asyncio
async def test_selected_ws_transport_does_not_warm_unused_http():
    client = SimpleNamespace(_warmed_ws_sender=object())
    await LighterSdkClient.warm_mutation_http(client)
