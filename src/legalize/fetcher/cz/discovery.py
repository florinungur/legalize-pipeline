"""Norm discovery for Czech Republic via e-Sbírka API.

The e-Sbírka search API does not support pagination beyond the first
page (returns 400 on start > 0) and ignores facet filters. Both
discovery methods use sequential probing of law numbers instead:

- discover_all: probes /sb/{year}/{n} for every year from present back
  to 1945. Stops each year after 5 consecutive 404s.
- discover_daily: probes /sb/{year}/{n} for the target year, filtering
  by publication date from metadata.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import date
from typing import TYPE_CHECKING

import requests

from legalize.fetcher.base import LegislativeClient, NormDiscovery

if TYPE_CHECKING:
    from legalize.fetcher.cz.client import ESbirkaClient

logger = logging.getLogger(__name__)

# Maximum law number to probe per year before giving up.
# Czech Republic publishes ~400-700 acts per year.
_MAX_LAW_NUMBER = 800

# Oldest year to scan in discover_all. e-Sbírka has laws from 1945.
_MIN_YEAR = 1945


class ESbirkaDiscovery(NormDiscovery):
    """Discover Czech laws via the e-Sbírka API."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield staleUrls for all acts in the Sbírka zákonů.

        The search API does not support pagination beyond the first page
        (returns 400 on start > 0). Instead, we probe sequential law
        numbers /sb/{year}/{n} for every year from 1945 to present.

        Czech laws are numbered sequentially per year within the
        collection. A run of 5 consecutive 400/404 responses signals
        the end of that year's sequence.
        """
        esbirka: ESbirkaClient = client  # type: ignore[assignment]
        current_year = date.today().year
        total = 0

        for year in range(current_year, _MIN_YEAR - 1, -1):
            consecutive_misses = 0
            year_count = 0

            for n in range(1, _MAX_LAW_NUMBER + 1):
                stale_url = f"/sb/{year}/{n}"
                try:
                    esbirka.get_metadata(stale_url)
                    consecutive_misses = 0
                    year_count += 1
                    total += 1
                    yield stale_url
                except requests.HTTPError:
                    consecutive_misses += 1
                    if consecutive_misses >= 5:
                        break

            if year_count > 0:
                logger.info(
                    "Discovery %d: %d laws (total so far: %d)",
                    year,
                    year_count,
                    total,
                )

        logger.info(
            "Discovery complete: %d laws found across %d-%d", total, _MIN_YEAR, current_year
        )

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield staleUrls for laws published on a specific date.

        Czech laws are numbered sequentially per year. This method
        probes /sb/{year}/{n} for n=1..N, fetches metadata for each
        existing law, and yields those whose publication date matches.

        Stops after hitting 5 consecutive 400/404 responses (end of
        the sequence for the year so far).
        """
        esbirka: ESbirkaClient = client  # type: ignore[assignment]
        year = target_date.year
        target_str = target_date.isoformat()
        consecutive_misses = 0

        for n in range(1, _MAX_LAW_NUMBER + 1):
            stale_url = f"/sb/{year}/{n}"
            try:
                meta_bytes = esbirka.get_metadata(stale_url)
                consecutive_misses = 0
            except requests.HTTPError:
                consecutive_misses += 1
                if consecutive_misses >= 5:
                    logger.debug(
                        "Stopping at /sb/%d/%d after 5 consecutive misses",
                        year,
                        n,
                    )
                    break
                continue

            try:
                meta = json.loads(meta_bytes)
            except (json.JSONDecodeError, TypeError):
                continue

            pub_date = meta.get("datumCasVyhlaseni", "")[:10]
            if pub_date == target_str:
                yield stale_url
