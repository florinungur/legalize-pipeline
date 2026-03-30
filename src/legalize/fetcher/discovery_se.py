"""Backwards-compatibility shim — use legalize.fetcher.se.discovery instead."""

from legalize.fetcher.se.discovery import *  # noqa: F401,F403
from legalize.fetcher.se.discovery import SwedishDiscovery  # noqa: F811

__all__ = ["SwedishDiscovery"]
