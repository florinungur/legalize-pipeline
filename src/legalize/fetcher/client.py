"""Backwards-compatibility shim — use legalize.fetcher.es.client instead."""

from legalize.fetcher.es.client import *  # noqa: F401,F403
from legalize.fetcher.es.client import BOEClient, RateLimiter  # noqa: F811

__all__ = ["BOEClient", "RateLimiter"]
