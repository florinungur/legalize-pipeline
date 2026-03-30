"""Backwards-compatibility shim — use legalize.fetcher.es.metadata instead."""

from legalize.fetcher.es.metadata import *  # noqa: F401,F403
from legalize.fetcher.es.metadata import (  # noqa: F811
    _DEPT_TO_JURISDICCION,
    _RANGO_CODE_MAP,
    _RANGO_MAP,
    parse_metadatos,
)

__all__ = ["parse_metadatos", "_RANGO_MAP", "_RANGO_CODE_MAP", "_DEPT_TO_JURISDICCION"]
