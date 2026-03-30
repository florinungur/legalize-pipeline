"""Backwards-compatibility shim — use legalize.fetcher.es.parser instead."""

from legalize.fetcher.es.parser import *  # noqa: F401,F403
from legalize.fetcher.es.parser import BOEMetadataParser, BOETextParser  # noqa: F811

__all__ = ["BOETextParser", "BOEMetadataParser"]
