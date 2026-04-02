"""YAML frontmatter generation for norm Markdown files.

Only factual data from the official source. 7 fixed fields, same order,
all countries. Everything else goes to extra (JSON only, not frontmatter).

  ---
  title: "Lei 50/2024"
  identifier: "DRE-L-50-2024"
  country: "pt"
  rank: "lei"
  publication_date: "2024-07-29"
  last_updated: "2024-07-29"
  status: "in_force"
  source: "https://data.dre.pt/eli/..."
  ---
"""

from __future__ import annotations

from datetime import date

from legalize.models import NormMetadata, NormStatus


def render_frontmatter(metadata: NormMetadata, version_date: date) -> str:
    """Generates the YAML frontmatter block for a norm at a given date.

    Only core fields — no department, no summary, no extra.
    Those go in the JSON for the DB.
    """
    clean_title = _clean_title(metadata.title)
    status = metadata.status.value if isinstance(metadata.status, NormStatus) else metadata.status

    lines = [
        "---",
        f'title: "{_escape_yaml(clean_title)}"',
        f'identifier: "{metadata.identifier}"',
        f'country: "{metadata.country}"',
        f'rank: "{metadata.rank}"',
        f'publication_date: "{metadata.publication_date.isoformat()}"',
        f'last_updated: "{version_date.isoformat()}"',
        f'status: "{status}"',
        f'source: "{metadata.source}"',
        "---",
        "",
    ]

    return "\n".join(lines)


def _escape_yaml(text: str) -> str:
    """Escapes double quotes in YAML values."""
    return text.replace('"', '\\"')


def _clean_title(raw_title: str) -> str:
    """Cleans the title: remove trailing period, normalize spaces."""
    return raw_title.rstrip(". ").strip()
