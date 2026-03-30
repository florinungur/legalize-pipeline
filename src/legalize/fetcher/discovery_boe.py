"""Backwards-compatibility shim — use legalize.fetcher.es.discovery instead."""

from legalize.fetcher.es.discovery import *  # noqa: F401,F403
from legalize.fetcher.es.discovery import BOEDiscovery  # noqa: F811

__all__ = ["BOEDiscovery"]
