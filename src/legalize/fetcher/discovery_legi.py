"""Backwards-compatibility shim — use legalize.fetcher.fr.discovery instead."""

from legalize.fetcher.fr.discovery import *  # noqa: F401,F403
from legalize.fetcher.fr.discovery import LEGIDiscovery  # noqa: F811

__all__ = ["LEGIDiscovery"]
