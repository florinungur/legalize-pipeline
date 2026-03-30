"""Backwards-compatibility shim — use legalize.fetcher.se.client instead."""

from legalize.fetcher.se.client import *  # noqa: F401,F403
from legalize.fetcher.se.client import SwedishClient  # noqa: F811

__all__ = ["SwedishClient"]
