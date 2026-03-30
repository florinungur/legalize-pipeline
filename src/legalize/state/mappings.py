"""Bidirectional BOE-ID to filepath mapping.

Persists in id-to-filename.json to know which file corresponds
to each norm without having to recalculate it.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class IdToFilename:
    """BOE-ID to file path mapping in the repo."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._map: dict[str, str] = {}
        self._reverse: dict[str, str] = {}

    def load(self) -> None:
        """Loads the mapping from disk."""
        if not self._path.exists():
            return

        with open(self._path, encoding="utf-8") as f:
            self._map = json.load(f)
        self._reverse = {v: k for k, v in self._map.items()}

    def save(self) -> None:
        """Persists the mapping to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)

        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._map, f, indent=2, ensure_ascii=False)

        logger.debug("Mappings saved to %s", self._path)

    def get(self, boe_id: str) -> Optional[str]:
        """BOE-A-1978-31229 → 'constitucion/constitucion-espanola.md'."""
        return self._map.get(boe_id)

    def set(self, boe_id: str, filepath: str) -> None:
        """Registers a BOE-ID to filepath mapping."""
        self._map[boe_id] = filepath
        self._reverse[filepath] = boe_id

    def get_by_filepath(self, filepath: str) -> Optional[str]:
        """Reverse lookup: filepath to BOE-ID. O(1) via reverse index."""
        return self._reverse.get(filepath)

    def __len__(self) -> int:
        return len(self._map)

    def __contains__(self, boe_id: str) -> bool:
        return boe_id in self._map
