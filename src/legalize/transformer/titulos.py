"""Backwards-compatibility shim — use legalize.fetcher.es.titulos instead."""

from legalize.fetcher.es.titulos import *  # noqa: F401,F403
from legalize.fetcher.es.titulos import TITULOS_CORTOS, get_titulo_corto  # noqa: F811

__all__ = ["TITULOS_CORTOS", "get_titulo_corto"]
