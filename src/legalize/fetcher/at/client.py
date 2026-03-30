"""Austria RIS (Rechtsinformationssystem) HTTP client.

Data source: https://data.bka.gv.at/ris/api/v2.6/
License: CC BY 4.0 (OGD Austria — https://www.data.gv.at)
"""

from __future__ import annotations

import json
import logging
import time

import requests

from legalize.fetcher.base import LegislativeClient

logger = logging.getLogger(__name__)

API_BASE = "https://data.bka.gv.at/ris/api/v2.6"
DOC_BASE = "https://www.ris.bka.gv.at/Dokumente/Bundesnormen"
RATE_LIMIT_DELAY = 0.5  # seconds between requests


class RISClient(LegislativeClient):
    """HTTP client for the Austrian RIS open data API (Bundesrecht konsolidiert).

    Austria's API returns one XML per NOR (paragraph/article). To get a full law,
    we first fetch metadata (by Gesetzesnummer) to find all NOR IDs, then fetch
    each NOR XML and combine them into a single document.
    """

    @classmethod
    def create(cls, country_config):
        """Create RISClient from CountryConfig."""
        return cls()

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "legalize-bot/1.0"})

    def get_text(self, gesetzesnummer: str) -> bytes:
        """Fetch all NOR XMLs for a Gesetzesnummer and combine them.

        1. Fetches metadata to find all NOR IDs for this law
        2. Downloads each NOR XML
        3. Wraps them in a combined <combined_nor_documents> element

        Args:
            gesetzesnummer: Stable law identifier, e.g. '10002333'

        Returns:
            Combined XML bytes with all NOR documents.
        """
        meta_data = self.get_metadata(gesetzesnummer)
        nor_ids = self._extract_nor_ids(meta_data)

        if not nor_ids:
            raise ValueError(f"No NOR documents found for Gesetzesnummer {gesetzesnummer}")

        logger.info("Fetching %d NOR documents for %s", len(nor_ids), gesetzesnummer)

        parts = ['<?xml version="1.0" encoding="UTF-8"?>']
        parts.append(f'<combined_nor_documents gesetzesnummer="{gesetzesnummer}">')

        for nor_id in nor_ids:
            try:
                xml = self._fetch_nor_xml(nor_id)
                # Strip XML declaration from individual docs before combining
                content = xml.decode("utf-8", errors="replace")
                content = content.replace('<?xml version="1.0" encoding="UTF-8"?>', "").strip()
                parts.append(content)
            except Exception:
                logger.warning("Could not fetch NOR %s, skipping", nor_id)

        parts.append("</combined_nor_documents>")
        return "\n".join(parts).encode("utf-8")

    def get_metadata(self, gesetzesnummer: str) -> bytes:
        """Fetch JSON metadata for all NOR entries of a Gesetzesnummer.

        Returns raw JSON bytes of the full API response (OgdSearchResult).
        gesetzesnummer is the stable law identifier, e.g. '10002333'.
        """
        params = {
            "Applikation": "BrKons",
            "Gesetzesnummer": gesetzesnummer,
            "Seitennummer": 1,
            "Dokumentnummer": 100,
        }
        r = self._session.get(f"{API_BASE}/Bundesrecht", params=params, timeout=30)
        r.raise_for_status()
        return r.content

    def get_page(self, page: int = 1, page_size: int = 100, **filters: str) -> bytes:
        """Generic paginated search against the Bundesrecht endpoint."""
        params: dict[str, str | int] = {
            "Applikation": "BrKons",
            "Seitennummer": page,
            "Dokumentnummer": page_size,
            **filters,
        }
        r = self._session.get(f"{API_BASE}/Bundesrecht", params=params, timeout=30)
        r.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)
        return r.content

    def close(self) -> None:
        self._session.close()

    # ── Internal helpers ──

    def _fetch_nor_xml(self, nor_id: str) -> bytes:
        """Fetch the XML of one NOR document."""
        url = f"{DOC_BASE}/{nor_id}/{nor_id}.xml"
        r = self._session.get(url, timeout=30)
        r.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)
        return r.content

    @staticmethod
    def _extract_nor_ids(meta_data: bytes) -> list[str]:
        """Extract all NOR IDs from a metadata API response."""
        data = json.loads(meta_data)
        docs = (
            data.get("OgdSearchResult", {})
            .get("OgdDocumentResults", {})
            .get("OgdDocumentReference", [])
        )
        return [
            d["Data"]["Metadaten"]["Technisch"]["ID"]
            for d in docs
            if "Data" in d
            and "Metadaten" in d["Data"]
            and "Technisch" in d["Data"]["Metadaten"]
            and "ID" in d["Data"]["Metadaten"]["Technisch"]
        ]
