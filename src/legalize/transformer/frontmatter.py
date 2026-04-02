"""YAML frontmatter generation for norm Markdown files.

Generic multi-country format:
  ---
  title: "Constitución Española"
  identifier: "BOE-A-1978-31229"
  country: "es"
  rank: "constitucion"
  publication_date: "1978-12-29"
  last_updated: "2024-02-17"
  status: "vigente"
  source: "https://www.boe.es/eli/es/c/1978/12/27/(1)"
  ---
"""

from __future__ import annotations

from datetime import date

from legalize.models import NormMetadata, NormStatus


def render_frontmatter(metadata: NormMetadata, version_date: date) -> str:
    """Generates the YAML frontmatter block for a norm at a given date."""
    clean_title = _clean_title(metadata.title)

    lines = [
        "---",
        f'title: "{_escape_yaml(clean_title)}"',
        f'identifier: "{metadata.identifier}"',
        f'country: "{metadata.country}"',
    ]

    if metadata.jurisdiction:
        lines.append(f'jurisdiction: "{metadata.jurisdiction}"')

    lines.extend(
        [
            f'rank: "{metadata.rank}"',
            f'publication_date: "{metadata.publication_date.isoformat()}"',
            f'last_updated: "{version_date.isoformat()}"',
            f'status: "{metadata.status.value if isinstance(metadata.status, NormStatus) else metadata.status}"',
            f'source: "{metadata.source}"',
        ]
    )

    if metadata.pdf_url:
        lines.append(f'pdf_url: "{metadata.pdf_url}"')

    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def _escape_yaml(text: str) -> str:
    """Escapes double quotes in YAML values."""
    return text.replace('"', '\\"')


def _clean_title(raw_title: str) -> str:
    """Cleans the title: remove trailing period, normalize spaces."""
    return raw_title.rstrip(". ").strip()
