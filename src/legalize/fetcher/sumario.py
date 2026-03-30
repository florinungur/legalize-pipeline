"""Backwards-compatibility shim — use legalize.fetcher.es.sumario instead."""

from legalize.fetcher.es.sumario import *  # noqa: F401,F403
from legalize.fetcher.es.sumario import parse_sumario  # noqa: F811

__all__ = ["parse_sumario"]
