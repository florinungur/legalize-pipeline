"""Norm discovery for Argentine legislation via the InfoLEG catalog.

There is no per-norm search API. Discovery downloads the monthly catalog ZIP
from datos.jus.gob.ar (via :class:`InfoLEGClient`), filters to the V1 scope
(Tier 1: Leyes + Decretos + DNUs with consolidated text), and yields the
``id_norma`` of each.

Daily updates piggyback on the same monthly catalog: between releases there
is no incremental feed, so ``discover_daily`` filters by ``fecha_boletin``
matching the requested date. The cron schedule is monthly, not daily.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date
from typing import TYPE_CHECKING

from legalize.fetcher.base import LegislativeClient, NormDiscovery

if TYPE_CHECKING:
    from legalize.fetcher.ar.client import InfoLEGClient

logger = logging.getLogger(__name__)


# V1 scope: only norms with consolidated text (Tier 1).
# These are the types where InfoLEG actively maintains a vigent text.
V1_TIPO_NORMA = frozenset(
    {
        "Ley",
        "Decreto",
        "Decreto/Ley",  # decretos-ley from de facto governments (note the slash)
    }
)

# Whitelist of id_norma to include even if Tier 2 (no texto_actualizado).
# These are headline norms that users always expect to find.
V1_TIER2_WHITELIST = frozenset(
    {
        "804",  # Ley 24.430 — Constitución Nacional (consolidated text via norma.htm only)
    }
)


class InfoLEGDiscovery(NormDiscovery):
    """Discover Argentine norms from the InfoLEG catalog CSV.

    The catalog is downloaded once per session via :meth:`InfoLEGClient.ensure_catalog`.
    Subsequent discovery iterations reuse the in-memory index.
    """

    def __init__(
        self,
        *,
        allowed_types: frozenset[str] = V1_TIPO_NORMA,
        whitelist: frozenset[str] = V1_TIER2_WHITELIST,
    ) -> None:
        self._allowed_types = allowed_types
        self._whitelist = whitelist

    @classmethod
    def create(cls, source: dict) -> InfoLEGDiscovery:
        # Optional config override of the type filter
        types = source.get("tipos_norma")
        allowed = frozenset(types) if types else V1_TIPO_NORMA
        whitelist = frozenset(source.get("tier2_whitelist", V1_TIER2_WHITELIST))
        return cls(allowed_types=allowed, whitelist=whitelist)

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield every Tier 1 ``id_norma`` plus the Tier 2 whitelist."""
        ic: InfoLEGClient = client  # type: ignore[assignment]
        catalog = ic.ensure_catalog()
        n = 0
        for row in catalog.filter_tier1(self._allowed_types, self._whitelist):
            n += 1
            yield row.id_norma
        logger.info(
            "Discovered %d Argentine norms (allowed types=%s, whitelist=%d)",
            n,
            sorted(self._allowed_types),
            len(self._whitelist),
        )

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield norms whose ``fecha_boletin`` equals ``target_date``.

        Note: the catalog refreshes monthly. Calling this with a date for
        which the catalog has not yet been updated will return an empty
        iterator. The pipeline orchestration is expected to schedule daily
        runs only after the monthly catalog refresh.
        """
        ic: InfoLEGClient = client  # type: ignore[assignment]
        catalog = ic.ensure_catalog()
        n = 0
        for row in catalog.by_id.values():
            if row.fecha_boletin != target_date:
                continue
            if row.id_norma in self._whitelist:
                n += 1
                yield row.id_norma
                continue
            if not row.has_consolidated_text and not row.has_original_text:
                continue
            if row.tipo_norma not in self._allowed_types:
                continue
            n += 1
            yield row.id_norma
        logger.info("Discovered %d norms for %s", n, target_date.isoformat())
