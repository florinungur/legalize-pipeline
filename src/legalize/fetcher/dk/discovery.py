"""Discovery of Danish legislation via Retsinformation sitemap + harvest API.

Bootstrap: downloads all 21 sitemap pages, extracts ``eli/lta/`` URLs, and
yields accession numbers after fetching the XML header for each document.

Daily updates: calls the harvest API (``/v1/Documents``) which returns
documents changed since a given date.

Norm IDs use accession number format (e.g. ``A20240006229``) which is
extracted from the XML ``<AccessionNumber>`` element during discovery.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import date

from lxml import etree

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.dk.client import RetsinformationClient

logger = logging.getLogger(__name__)

_SITEMAP_PAGES = 21
_SITEMAP_URL = "https://www.retsinformation.dk/sitemap.xml"

# Extract ELI path from sitemap URLs.
# Example: https://retsinformation.dk/eli/lta/2024/62 → lta/2024/62
_ELI_LTA_PATTERN = re.compile(r"retsinformation\.dk/eli/(lta/\d{4}/\d+)")

# Document types we want (from DocumentType XML field).
# LBK H = consolidated law, LOV H = original law, BEK H = executive order.
_WANTED_TYPES = {"LBK H", "LOV H", "BEK H", "Lov"}


class RetsinformationDiscovery(NormDiscovery):
    """Discovers Danish legislation via sitemap (bootstrap) and harvest API (daily).

    Bootstrap: ~21 sitemap pages → ~63K ELI URLs → filter lta/ → fetch XML
    headers to get accession numbers and document types.
    """

    @classmethod
    def create(cls, source: dict) -> RetsinformationDiscovery:
        return cls()

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield ELI paths (``lta/{year}/{number}``) for all lta documents.

        The caller (generic_fetch_all) caches these to ``discovery_ids.txt``.
        Filtering by document type happens at fetch time (in the parser),
        since checking each document's type during discovery would require
        fetching all 63K XMLs during discovery alone.
        """
        assert isinstance(client, RetsinformationClient)

        total = 0
        seen: set[str] = set()

        for page in range(2, _SITEMAP_PAGES + 1):
            url = f"{_SITEMAP_URL}?page={page}"
            try:
                data = client._get(url)
            except Exception:
                logger.warning("Failed to fetch sitemap page %d", page)
                continue

            # Parse sitemap XML — strip namespace for simpler xpath
            try:
                root = etree.fromstring(data)
            except etree.XMLSyntaxError:
                logger.warning("Invalid XML in sitemap page %d", page)
                continue

            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall("sm:url/sm:loc", ns):
                text = loc.text or ""
                match = _ELI_LTA_PATTERN.search(text)
                if match:
                    eli_path = match.group(1)
                    if eli_path not in seen:
                        seen.add(eli_path)
                        total += 1
                        yield eli_path

            if page % 5 == 0:
                logger.info(
                    "Sitemap progress: page %d/%d, %d lta documents so far",
                    page,
                    _SITEMAP_PAGES,
                    total,
                )

        logger.info(
            "Discovery complete: %d unique lta documents across %d pages", total, _SITEMAP_PAGES
        )

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield ELI paths of documents changed on *target_date*.

        Uses the harvest API which returns changes from the last 24 hours
        relative to the given date.
        """
        assert isinstance(client, RetsinformationClient)

        date_str = target_date.isoformat()
        try:
            changes = client.get_daily_changes(date_str)
        except Exception:
            logger.error("Harvest API failed for %s", date_str, exc_info=True)
            return

        total = 0
        for doc in changes:
            reason = doc.get("reasonForChange", "")
            if reason == "RemovedDocument":
                continue

            # The href field contains the ELI XML URL
            href = doc.get("href", "")
            match = _ELI_LTA_PATTERN.search(href)
            if match:
                total += 1
                yield match.group(1)
            else:
                # Fallback: use accession number directly
                accn = doc.get("accessionsnummer", "")
                if accn:
                    total += 1
                    yield accn

        logger.info("Daily discovery for %s: %d documents", target_date, total)
