"""Discovery for Norwegian legislation via Lovdata public data dump.

discover_all: yields all norm IDs from the local nl/ directory.
discover_daily: re-downloads the archive and yields IDs of changed files.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import NormDiscovery
from legalize.fetcher.no.client import LovdataClient

logger = logging.getLogger(__name__)


class LovdataDiscovery(NormDiscovery):
    """Discover Norwegian laws from the local data dump."""

    def discover_all(self, client: LovdataClient, **kwargs) -> Iterator[str]:
        """Yield all norm IDs (nl-YYYYMMDD-NNN) from the local dump."""
        ids = client.indexed_ids
        logger.info("Discovered %d Norwegian laws", len(ids))
        yield from ids

    def discover_daily(self, client: LovdataClient, target_date: date, **kwargs) -> Iterator[str]:
        """Yield norm IDs that changed since last sync.

        The Lovdata public dump does not expose per-file change dates.
        For daily updates, the pipeline re-downloads the full archive
        and the state layer detects which files changed via content hash.

        This method yields all IDs — the state layer handles dedup.
        """
        logger.info(
            "Daily discovery for %s: yielding all IDs (state layer handles dedup)",
            target_date,
        )
        yield from self.discover_all(client, **kwargs)
