"""Norm discovery for Czech Republic via e-Sbírka search API."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date
from typing import TYPE_CHECKING

from legalize.fetcher.base import LegislativeClient, NormDiscovery

if TYPE_CHECKING:
    from legalize.fetcher.cz.client import ESbirkaClient

logger = logging.getLogger(__name__)

# Act types to include in discovery (laws and constitutional laws).
# Others (ordinances, decrees, international acts) are excluded for now.
_LAW_ACT_TYPES = ("ZAKON", "ZAKONUST")

# Search page size. The API allows up to 1000 per page.
_PAGE_SIZE = 100


class ESbirkaDiscovery(NormDiscovery):
    """Discover Czech laws via the e-Sbírka search API.

    discover_all: paginated search filtered to laws + constitutional laws.
    discover_daily: search filtered by publication date.
    """

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield staleUrls for all laws and constitutional laws.

        Paginates through the full catalog. The facet filter restricts
        results to ZAKON (law) and ZAKONUST (constitutional law) types.
        """
        esbirka: ESbirkaClient = client  # type: ignore[assignment]
        start = 0
        total = None

        while total is None or start < total:
            result = esbirka.search(
                start=start,
                count=_PAGE_SIZE,
                facet_filter={
                    "typPravnihoAktu": list(_LAW_ACT_TYPES),
                },
            )
            total = result.get("pocetCelkem", 0)
            items = result.get("seznam", [])

            if not items:
                break

            for item in items:
                stale_url = item.get("staleUrl")
                if stale_url:
                    yield stale_url

            start += len(items)
            if start % 500 == 0:
                logger.info("Discovery progress: %d / %d", start, total)

        logger.info("Discovery complete: %d laws found", start)

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield staleUrls for laws published on a specific date.

        Uses the publication year facet filter to narrow results, then
        filters by exact date from the datum field in each result.
        """
        esbirka: ESbirkaClient = client  # type: ignore[assignment]
        target_str = target_date.isoformat()
        year_str = str(target_date.year)
        start = 0

        while True:
            result = esbirka.search(
                start=start,
                count=_PAGE_SIZE,
                facet_filter={
                    "typPravnihoAktu": list(_LAW_ACT_TYPES),
                    "vyhlaseni": [year_str],
                },
            )
            items = result.get("seznam", [])
            if not items:
                break

            for item in items:
                datum = item.get("datum", "")
                if datum == target_str:
                    stale_url = item.get("staleUrl")
                    if stale_url:
                        yield stale_url

            start += len(items)
            total = result.get("pocetCelkem", 0)
            if start >= total:
                break
