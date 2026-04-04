"""Discovery of Austrian Bundesrecht norms via the RIS OGD API."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.at.client import RISClient

logger = logging.getLogger(__name__)

# Cache file for discovered Gesetzesnummern (discovery takes ~73 min)
_CACHE_FILENAME = "gesetzesnummern.json"
_CACHE_MAX_AGE_DAYS = 7


class RISDiscovery(NormDiscovery):
    """Discovers all Gesetze (grouped by Gesetzesnummer) in the RIS catalog."""

    def __init__(self, cache_dir: str | None = None, **kwargs) -> None:
        self._cache_dir = cache_dir

    @classmethod
    def create(cls, source: dict) -> RISDiscovery:
        """Create with cache_dir from source config."""
        return cls(cache_dir=source.get("cache_dir"))

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield all unique Gesetzesnummern in BrKons (Bundesrecht konsolidiert).

        Uses a cached list if available and recent (< 7 days old).
        Otherwise paginates through the full catalog (~437k NOR entries).
        """
        # Try cache first
        cached = self._load_cache()
        if cached is not None:
            logger.info("Using cached discovery: %d Gesetzesnummern", len(cached))
            yield from cached
            return

        assert isinstance(client, RISClient)
        seen: set[str] = set()
        result: list[str] = []
        page = 1
        page_size = 100

        while True:
            raw = client.get_page(page=page, page_size=page_size)
            data = json.loads(raw)
            results = data["OgdSearchResult"]["OgdDocumentResults"]
            total = int(results["Hits"]["#text"])

            refs = results.get("OgdDocumentReference", [])
            if isinstance(refs, dict):
                refs = [refs]

            for ref in refs:
                br = ref["Data"]["Metadaten"]["Bundesrecht"]["BrKons"]
                gesnr = br.get("Gesetzesnummer", "")
                if gesnr and gesnr not in seen:
                    seen.add(gesnr)
                    result.append(gesnr)
                    yield gesnr

            fetched_so_far = (page - 1) * page_size + len(refs)
            if page % 500 == 0:
                logger.info(
                    "Discovery page %d: %d/%d NOR entries, %d unique laws",
                    page,
                    fetched_so_far,
                    total,
                    len(result),
                )
            if fetched_so_far >= total or not refs:
                break
            page += 1

        # Save cache for next run
        self._save_cache(result)

    def _cache_path(self) -> Path | None:
        if not self._cache_dir:
            return None
        return Path(self._cache_dir) / _CACHE_FILENAME

    def _load_cache(self) -> list[str] | None:
        path = self._cache_path()
        if path is None or not path.exists():
            return None
        import time

        age_days = (time.time() - path.stat().st_mtime) / 86400
        if age_days > _CACHE_MAX_AGE_DAYS:
            logger.info("Discovery cache expired (%.0f days old)", age_days)
            return None
        try:
            data = json.loads(path.read_text())
            return data.get("gesetzesnummern", None)
        except (json.JSONDecodeError, OSError):
            return None

    def _save_cache(self, gesnrs: list[str]) -> None:
        path = self._cache_path()
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"gesetzesnummern": gesnrs}, indent=2))
        logger.info("Saved discovery cache: %d Gesetzesnummern → %s", len(gesnrs), path)

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield Gesetzesnummern updated on target_date.

        The RIS API ignores the Geaendert query parameter, so we use
        ImRisSeit=EinerWoche to get recent changes and filter client-side
        by the Allgemein.Geaendert field matching the target date.
        """
        assert isinstance(client, RISClient)
        seen: set[str] = set()
        date_str = target_date.strftime("%Y-%m-%d")
        page = 1
        page_size = 100

        while True:
            raw = client.get_page(page=page, page_size=page_size, ImRisSeit="EinerWoche")
            data = json.loads(raw)
            results = data["OgdSearchResult"].get("OgdDocumentResults")
            if not results:
                break

            total = int(results["Hits"]["#text"])
            refs = results.get("OgdDocumentReference", [])
            if isinstance(refs, dict):
                refs = [refs]

            for ref in refs:
                geaendert = ref["Data"]["Metadaten"].get("Allgemein", {}).get("Geaendert", "")
                if geaendert != date_str:
                    continue
                br = ref["Data"]["Metadaten"]["Bundesrecht"]["BrKons"]
                gesnr = br.get("Gesetzesnummer", "")
                if gesnr and gesnr not in seen:
                    seen.add(gesnr)
                    yield gesnr

            fetched_so_far = (page - 1) * page_size + len(refs)
            if fetched_so_far >= total or not refs:
                break
            page += 1
