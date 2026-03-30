"""Backwards-compatibility shim — use legalize.fetcher.se.parser instead."""

from legalize.fetcher.se.parser import *  # noqa: F401,F403
from legalize.fetcher.se.parser import SwedishMetadataParser, SwedishTextParser  # noqa: F811

__all__ = ["SwedishTextParser", "SwedishMetadataParser"]
