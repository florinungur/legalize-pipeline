"""State Store — pipeline state tracking.

Persists in state.json: last summary date and run history.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RunRecord:
    """Record of a pipeline run."""

    timestamp: str  # ISO datetime
    summaries_reviewed: list[str] = field(default_factory=list)
    commits_created: int = 0
    errors: list[str] = field(default_factory=list)


class StateStore:
    """Manages the pipeline's state.json file."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._last_summary: Optional[str] = None
        self._runs: list[RunRecord] = []

    def load(self) -> None:
        """Load state from disk."""
        if not self._path.exists():
            return

        with open(self._path, encoding="utf-8") as f:
            data = json.load(f)

        self._last_summary = data.get("last_summary")

        for r in data.get("runs", []):
            self._runs.append(
                RunRecord(
                    timestamp=r["timestamp"],
                    summaries_reviewed=r.get("summaries_reviewed", []),
                    commits_created=r.get("commits_created", 0),
                    errors=r.get("errors", []),
                )
            )

    def save(self) -> None:
        """Persist state to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "last_summary": self._last_summary,
            "runs": [asdict(r) for r in self._runs],
        }

        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.debug("State saved to %s", self._path)

    @property
    def last_summary_date(self) -> Optional[date]:
        """Date of the last processed summary."""
        if self._last_summary:
            return date.fromisoformat(self._last_summary)
        return None

    @last_summary_date.setter
    def last_summary_date(self, value: date) -> None:
        self._last_summary = value.isoformat()

    def record_run(
        self,
        summaries: list[str] | None = None,
        commits: int = 0,
        errors: list[str] | None = None,
    ) -> None:
        """Record a pipeline run."""
        self._runs.append(
            RunRecord(
                timestamp=datetime.now().isoformat(),
                summaries_reviewed=summaries or [],
                commits_created=commits,
                errors=errors or [],
            )
        )
