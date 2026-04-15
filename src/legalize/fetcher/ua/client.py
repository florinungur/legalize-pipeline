"""Ukraine legislative client — data.rada.gov.ua.

Primary endpoints:
- /laws/card/{nreg}.json — structured metadata + edition list (1 req per law)
- /laws/show/{nreg}/ed{YYYYMMDD}.txt — historical version text at a specific date
- /laws/show/{nreg}.txt — current consolidated text (fallback)

Discovery:
- zakon.rada.gov.ua/laws/main/t{N}.txt — type lists (laws, codes, etc.)
- data.rada.gov.ua/ogd/zak/laws/data/csv/perv1.txt — curated primary acts
- /laws/main/r/page{N}.json — recently updated documents (daily)

Rate limit: 60 req/min, 100K req/day. Token from /api/token required for
JSON endpoints (sent as User-Agent header).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING
from urllib.parse import quote

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

_OPEN_DATA_UA = "OpenData"


class RadaClient(HttpClient):
    """HTTP client for the Verkhovna Rada legislative portal."""

    def __init__(
        self,
        *,
        base_url: str,
        zakon_base_url: str,
        request_timeout: int = 30,
        max_retries: int = 5,
        requests_per_second: float = 1.0,
    ) -> None:
        super().__init__(
            base_url=base_url,
            user_agent=_OPEN_DATA_UA,
            request_timeout=request_timeout,
            max_retries=max_retries,
            requests_per_second=requests_per_second,
        )
        self._zakon_base_url = zakon_base_url.rstrip("/")
        self._token: str | None = None

    @classmethod
    def create(cls, country_config: CountryConfig) -> RadaClient:
        src = country_config.source
        return cls(
            base_url=src.get("base_url", "https://data.rada.gov.ua"),
            zakon_base_url=src.get("zakon_base_url", "https://zakon.rada.gov.ua"),
            request_timeout=src.get("request_timeout", 30),
            max_retries=src.get("max_retries", 5),
            requests_per_second=src.get("requests_per_second", 1.0),
        )

    @staticmethod
    def _encode_nreg(nreg: str) -> str:
        """URL-encode nreg, keeping ``/`` literal."""
        return quote(nreg, safe="/")

    def _ensure_token(self) -> None:
        """Fetch a daily API token for JSON endpoints."""
        if self._token is not None:
            return
        try:
            raw = self._get(f"{self._base_url}/api/token")
            data = json.loads(raw)
            self._token = data["token"]
            logger.info("Obtained Rada API token (expires in %ss)", data.get("expire"))
        except Exception:
            logger.warning("Failed to obtain API token, JSON endpoints may fail")
            self._token = _OPEN_DATA_UA

    def _get_json(self, url: str) -> bytes:
        """GET a JSON endpoint using the API token as User-Agent."""
        self._ensure_token()
        old_ua = self._session.headers.get("User-Agent")
        try:
            self._session.headers["User-Agent"] = self._token or _OPEN_DATA_UA
            return self._get(url)
        finally:
            self._session.headers["User-Agent"] = old_ua or _OPEN_DATA_UA

    # ── Card endpoint (metadata + edition list) ──

    def get_card(self, norm_id: str) -> dict:
        """Fetch structured metadata via /laws/card/{nreg}.json.

        Returns a dict with keys including:
        - nazva: title
        - nreg: registration number
        - orgdat: original date (YYYYMMDD int)
        - status: status code (5 = in force)
        - edcnt: number of editions
        - eds[]: array of editions with datred, pidstava, size
        - hist[]: history entries with poddat, pidstava, podid
        - organs, n_vlas, typ, publics, etc.
        """
        url = f"{self._base_url}/laws/card/{self._encode_nreg(norm_id)}.json"
        raw = self._get_json(url)
        return json.loads(raw)

    # ── Text endpoints ──

    def get_text(self, norm_id: str) -> bytes:
        """Fetch current consolidated text via /laws/show/{nreg}.txt."""
        url = f"{self._base_url}/laws/show/{self._encode_nreg(norm_id)}.txt"
        return self._get(url)

    def get_text_at_edition(self, norm_id: str, edition_date: int) -> bytes:
        """Fetch historical text at a specific edition date.

        edition_date is YYYYMMDD as int (e.g. 20200101).
        Uses /laws/show/{nreg}/ed{YYYYMMDD}.txt.
        """
        url = f"{self._base_url}/laws/show/{self._encode_nreg(norm_id)}/ed{edition_date}.txt"
        return self._get(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata via /laws/show/{nreg}.xml (HTML with <meta> tags).

        Used as fallback when card endpoint is unavailable.
        """
        url = f"{self._base_url}/laws/show/{self._encode_nreg(norm_id)}.xml"
        return self._get(url)

    # ── Discovery endpoints ──

    def get_discovery_list(self, list_name: str) -> bytes:
        """Fetch a discovery list (perv0/1/2.txt). Returns CP1251-encoded bytes."""
        url = f"{self._base_url}/ogd/zak/laws/data/csv/{list_name}"
        return self._get(url)

    def get_type_list(self, type_id: str) -> bytes:
        """Fetch a type list from zakon.rada.gov.ua (e.g. t1.txt for laws)."""
        url = f"{self._zakon_base_url}/laws/main/{type_id}.txt"
        return self._get(url)

    def get_recent_page(self, page: int) -> bytes:
        """Fetch recently updated documents (JSON). Requires API token."""
        url = f"{self._base_url}/laws/main/r/page{page}.json"
        return self._get_json(url)
