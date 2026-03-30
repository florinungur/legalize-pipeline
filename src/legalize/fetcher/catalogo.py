"""Backwards-compatibility shim — use legalize.fetcher.es.catalogo instead."""

from legalize.fetcher.es.catalogo import *  # noqa: F401,F403
from legalize.fetcher.es.catalogo import iter_normas_from_sumarios  # noqa: F811

__all__ = ["iter_normas_from_sumarios"]
