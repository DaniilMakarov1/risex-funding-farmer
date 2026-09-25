"""Credential-free classification of authoritative read-only HTTP rate limits."""
import math
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime


class ReadRateLimited(RuntimeError):
    def __init__(self, retry_after=0.0):
        super().__init__('read temporarily rate limited (HTTP 429)')
        self.retry_after = retry_after


def retry_after_delay(headers):
    delay = 0.0
    if isinstance(headers, Mapping):
        raw = next((v for k, v in headers.items() if str(k).lower() == 'retry-after'), None)
        if isinstance(raw, str):
            try:
                delay = float(raw)
            except ValueError:
                try:
                    delay = parsedate_to_datetime(raw).timestamp() - time.time()
                except (TypeError, ValueError, OverflowError):
                    delay = 0.0
    return delay if math.isfinite(delay) and delay >= 0 else 0.0


def read_rate_limit_delay(exc):
    if isinstance(exc, ReadRateLimited):
        return exc.retry_after
    try:
        from lighter.exceptions import ApiException
    except ImportError:
        return None
    if isinstance(exc, ApiException) and type(exc.status) is int and exc.status == 429:
        return retry_after_delay(exc.headers)
    return None
