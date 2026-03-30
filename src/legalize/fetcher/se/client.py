"""Client for the Riksdagen Open Data API (Swedish legislation).

Fetches Swedish statutes (SFS) from:
  https://data.riksdagen.se/dokumentlista/ — document search
  https://data.riksdagen.se/dokument/{dok_id}.json — full document
  https://rkrattsbaser.gov.se/sfsr?bet={SFS} — amendment register (SFSR)

Rate limited to 100ms between requests with retry on 429/503.
"""

from __future__ import annotations

import logging
import threading
import time
from urllib.parse import quote

import requests

from legalize.fetcher.base import LegislativeClient

logger = logging.getLogger(__name__)

_RIKSDAGEN_LIST_URL = "https://data.riksdagen.se/dokumentlista"
_RIKSDAGEN_DOC_URL = "https://data.riksdagen.se/dokument"
_SFSR_URL = "https://rkrattsbaser.gov.se/sfsr"

_USER_AGENT = "legalize-bot/1.0"
_RATE_LIMIT_MS = 100  # Riksdagen has no strict rate limit (tested: 10 burst OK)
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0


class SwedishClient(LegislativeClient):
    """Fetches Swedish legislation from the Riksdagen Open Data API.

    Two-step fetch for text:
    1. Search /dokumentlista/?sok={SFS}&doktyp=sfs to find dok_id
    2. Fetch /dokument/{dok_id}.json for full document with text

    Amendment register is fetched from rkrattsbaser.gov.se (SFSR HTML).
    """

    @classmethod
    def create(cls, country_config):
        """Create SwedishClient from CountryConfig."""
        return cls()

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": _USER_AGENT,
            "Accept": "application/json",
        })
        self._last_request_time: float = 0.0
        self._rate_lock = threading.Lock()

    def get_texto(self, norm_id: str) -> bytes:
        """Fetch the full document JSON for a Swedish statute.

        Searches Riksdagen API by SFS number, extracts dok_id,
        then fetches the full document JSON.

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            Full document JSON as bytes.
        """
        dok_id = self._find_dok_id(norm_id)
        url = f"{_RIKSDAGEN_DOC_URL}/{dok_id}.json"
        logger.info("Fetching document text: %s", url)
        return self._get(url)

    def get_metadatos(self, norm_id: str) -> bytes:
        """Fetch metadata for a Swedish statute.

        Uses the same endpoint as get_texto — metadata is embedded
        in the full document JSON (dokumentstatus.dokuppgift).

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            Full document JSON as bytes (same as get_texto).
        """
        return self.get_texto(norm_id)

    def get_amendment_register(self, norm_id: str) -> bytes:
        """Fetch the SFSR amendment register for a statute.

        Fetches HTML from rkrattsbaser.gov.se/sfsr?bet={SFS}.
        Contains amendment history with affected sections.

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            SFSR HTML page as bytes.
        """
        url = f"{_SFSR_URL}?bet={quote(norm_id)}"
        logger.info("Fetching SFSR amendment register: %s", url)
        return self._get(url, accept="text/html")

    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()

    # ── Internal helpers ──

    def _find_dok_id(self, norm_id: str) -> str:
        """Search Riksdagen API by SFS number to find the dok_id.

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            The dok_id string for the matching document.

        Raises:
            ValueError: If no document is found for the SFS number.
        """
        url = (
            f"{_RIKSDAGEN_LIST_URL}/"
            f"?sok={quote(norm_id)}&doktyp=sfs&format=json&utformat=json"
        )
        logger.debug("Searching Riksdagen for SFS %s", norm_id)
        data = self._get(url)

        import json
        result = json.loads(data)
        documents = (
            result.get("dokumentlista", {}).get("dokument") or []
        )

        if not documents:
            raise ValueError(f"No Riksdagen document found for SFS {norm_id}")

        # Prefer exact match on beteckning
        for doc in documents:
            if doc.get("beteckning") == norm_id:
                dok_id = doc["dok_id"]
                logger.debug("Found dok_id %s for SFS %s", dok_id, norm_id)
                return dok_id

        # Fallback: first result
        dok_id = documents[0]["dok_id"]
        logger.warning(
            "No exact match for SFS %s, using first result: %s",
            norm_id, dok_id,
        )
        return dok_id

    def _get(self, url: str, accept: str | None = None) -> bytes:
        """HTTP GET with rate limiting and retry on 429/503.

        Args:
            url: The URL to fetch.
            accept: Optional Accept header override.

        Returns:
            Response body as bytes.

        Raises:
            requests.HTTPError: On non-retryable HTTP errors.
        """
        self._rate_limit()

        headers = {}
        if accept:
            headers["Accept"] = accept

        for attempt in range(_MAX_RETRIES):
            response = self._session.get(url, headers=headers, timeout=30)

            if response.status_code in (429, 503):
                wait = _BACKOFF_BASE ** (attempt + 1)
                logger.warning(
                    "HTTP %d from %s, retrying in %.1fs (attempt %d/%d)",
                    response.status_code, url, wait, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(wait)
                self._rate_limit()
                continue

            response.raise_for_status()
            return response.content

        # Final attempt failed
        response.raise_for_status()
        return response.content  # unreachable, but satisfies type checker

    def _rate_limit(self) -> None:
        """Enforce minimum delay between requests."""
        with self._rate_lock:
            now = time.monotonic()
            elapsed_ms = (now - self._last_request_time) * 1000
            if elapsed_ms < _RATE_LIMIT_MS:
                sleep_s = (_RATE_LIMIT_MS - elapsed_ms) / 1000
                time.sleep(sleep_s)
            self._last_request_time = time.monotonic()
