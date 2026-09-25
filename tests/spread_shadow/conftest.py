"""Close best-effort Telegram display tasks when a test's controller ends."""
import asyncio

import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def close_telegram_display_tasks():
    yield
    tasks = [task for task in asyncio.all_tasks()
             if getattr(task.get_coro(), '__qualname__', '') == 'Controller._deliver_notices']
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
