"""HTTP client for the Estonian Riigi Teataja.

Riigi Teataja exposes individual laws as XML at:

    https://www.riigiteataja.ee/akt/{globaalID}.xml

There is no JSON REST API. Bulk discovery happens via the annual zip dumps
(handled in discovery.py); this client is for per-law fetches.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig


_DEFAULT_BASE_URL = "https://www.riigiteataja.ee"


class RTClient(HttpClient):
    """Client for the Estonian Riigi Teataja."""

    @classmethod
    def create(cls, country_config: CountryConfig) -> RTClient:
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", _DEFAULT_BASE_URL),
            request_timeout=int(source.get("request_timeout", 60)),
            max_retries=int(source.get("max_retries", 5)),
            requests_per_second=float(source.get("requests_per_second", 2.0)),
        )

    def get_text(self, norm_id: str) -> bytes:
        """Fetch the consolidated XML of a law by its ``globaalID``."""
        url = f"{self._base_url}/akt/{norm_id}.xml"
        return self._get(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Riigi Teataja embeds metadata inside the same XML as the text."""
        return self.get_text(norm_id)

    def get_html(self, norm_id: str) -> bytes:
        """Fetch the HTML page of an act.

        Used by the Eelmine chain crawler — the HTML contains explicit
        ``<a class="drop-label" href="{prev_gid}">Eelmine</a>`` and
        ``Järgmine`` links that let us walk the full version timeline of
        a law even for legacy pre-2010 entries.
        """
        url = f"{self._base_url}/akt/{norm_id}"
        return self._get(url)
