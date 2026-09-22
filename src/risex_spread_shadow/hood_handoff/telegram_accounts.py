"""On-demand account inspection; no execution adapter or credential provisioning."""
import asyncio
import time

from .contracts import AccountSnapshot
from .keychain import KeychainSecretProvider
from .readiness import ReadOnlyLighterSdkClient


def missing_key(_prompt):
    raise RuntimeError("stored account credential unavailable")


async def read_accounts(config, *, client_factory=ReadOnlyLighterSdkClient,
                        secret_factory=KeychainSecretProvider.from_config, clock=time.time):
    indices = (config.source_account_index, config.receiver_account_index)
    secrets = secret_factory(config, indices, prompt=missing_key)
    client = None
    try:
        client = client_factory(config, source_account_index=indices[0],
                                receiver_account_index=indices[1], secrets=secrets)

        async def read(index):
            try:
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
        return {'symbol': config.market_symbol, 'accounts': [
            {'index': index, 'snapshot': snapshot,
             'stale': snapshot is not None and not 0 <= now - snapshot.observed_at <= config.freshness_seconds}
            for index, snapshot in zip(indices, snapshots)]}
    finally:
        try:
            if client is not None:
                await client.aclose()
        finally:
            secrets.close()
