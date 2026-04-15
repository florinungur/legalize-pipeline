"""Client for the Lovdata public data dump (Norway).

Downloads consolidated legislation from the Lovdata public API and reads
XML-HTML files from the local data directory.  No authentication required.

Public data endpoint:
  https://api.lovdata.no/v1/publicData/get/gjeldende-lover.tar.bz2

The archive extracts to a flat ``nl/`` directory with one XML file per law.
File naming: ``nl-{YYYYMMDD}-{NNN}.xml`` (e.g., ``nl-20050520-028.xml``).
"""

from __future__ import annotations

import io
import logging
import tarfile
from pathlib import Path

import requests

from legalize.fetcher.base import LegislativeClient

logger = logging.getLogger(__name__)

_PUBLIC_API = "https://api.lovdata.no/v1/publicData/get"
_LAWS_ARCHIVE = "gjeldende-lover.tar.bz2"
_USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize)"


class LovdataClient(LegislativeClient):
    """Read Norwegian law XML from a local data directory.

    On first use, downloads the public tar.bz2 archive if the data
    directory does not yet contain an ``nl/`` subdirectory.
    """

    @classmethod
    def create(cls, country_config) -> LovdataClient:
        data_dir = Path(country_config.data_dir)
        return cls(data_dir)

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._nl_dir = data_dir / "nl"
        self._index: dict[str, Path] = {}

        if not self._nl_dir.exists():
            self._download_and_extract()

        self._build_index()

    def _download_and_extract(self) -> None:
        """Download gjeldende-lover.tar.bz2 and extract to data_dir."""
        url = f"{_PUBLIC_API}/{_LAWS_ARCHIVE}"
        logger.info("Downloading %s …", url)
        resp = requests.get(url, timeout=120, headers={"User-Agent": _USER_AGENT})
        resp.raise_for_status()
        logger.info("Downloaded %.1f MB, extracting …", len(resp.content) / 1024 / 1024)

        self._data_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:bz2") as tar:
            tar.extractall(self._data_dir, filter="data")

        logger.info("Extracted to %s", self._nl_dir)

    def _build_index(self) -> None:
        """Build norm_id → file path index from the nl/ directory."""
        for path in sorted(self._nl_dir.glob("nl-*.xml")):
            # Skip Nynorsk variants (only Constitution has one)
            if "-nn.xml" in path.name:
                continue
            norm_id = path.stem  # e.g. "nl-20050520-028"
            self._index[norm_id] = path

        logger.info("Indexed %d Norwegian laws", len(self._index))

    @property
    def indexed_ids(self) -> list[str]:
        """All norm IDs available in the local dump."""
        return sorted(self._index)

    def get_text(self, norm_id: str) -> bytes:
        """Return raw XML-HTML bytes for a law."""
        path = self._index.get(norm_id)
        if path is None:
            raise FileNotFoundError(f"No file for norm_id={norm_id!r}")
        return path.read_bytes()

    def get_metadata(self, norm_id: str) -> bytes:
        """Return raw XML-HTML bytes (metadata is embedded in the same file)."""
        return self.get_text(norm_id)

    def close(self) -> None:
        pass  # No HTTP session to close
