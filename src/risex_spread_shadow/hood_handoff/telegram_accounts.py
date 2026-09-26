"""On-demand account inspection; no execution adapter or credential provisioning."""
import asyncio
import time

from .contracts import AccountSnapshot
from .keychain import KeychainSecretProvider
from .readiness import ReadOnlyLighterSdkClient


def missing_key(_prompt):
    raise RuntimeError("stored account credential unavailable")


# Concurrent account reads for a wallet pool (display only).
READ_CONCURRENCY = 4


async def read_accounts(config, *, operator=None, client_factory=ReadOnlyLighterSdkClient,
                        secret_factory=KeychainSecretProvider.from_config, clock=time.time):
    pool = None
    if operator is not None:
        from .wallet_pool import load_wallet_pool
        pool = load_wallet_pool(operator, config)
        if not pool.from_file:
            pool = None
    indices = tuple(pool.all) if pool is not None else (config.source_account_index, config.receiver_account_index)
    secrets = secret_factory(config, indices, prompt=missing_key)
    client = None
    try:
        client = client_factory(config, source_account_index=config.source_account_index,
                                receiver_account_index=config.receiver_account_index, secrets=secrets)
        semaphore = asyncio.Semaphore(READ_CONCURRENCY)

        async def read(index):
            try:
                async with semaphore:
                    snapshot = await asyncio.wait_for(
                        client.account_snapshot(index, config.market_id),
                        timeout=config.request_timeout_seconds)
                if (not isinstance(snapshot, AccountSnapshot)
                    or snapshot.account_index != index or snapshot.market_id != config.market_id
                    or not snapshot.authorized):
                    return None
                return snapshot
            except Exception:
                return None  # Never expose SDK/credential-bearing exceptions.

        snapshots = await asyncio.gather(*(read(index) for index in indices))
        now = clock()
        result = {'symbol': config.market_symbol, 'accounts': [
            {'index': index, 'snapshot': snapshot,
             'stale': snapshot is not None and not 0 <= now - snapshot.observed_at <= config.freshness_seconds}
            for index, snapshot in zip(indices, snapshots)]}
        if pool is not None:
            result['wallets'] = {'active': list(pool.active), 'paused': list(pool.paused)}
        return result
    finally:
        try:
            if client is not None:
                await client.aclose()
        finally:
            secrets.close()
