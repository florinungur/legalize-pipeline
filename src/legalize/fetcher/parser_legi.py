"""Backwards-compatibility shim — use legalize.fetcher.fr.parser instead."""

from legalize.fetcher.fr.parser import *  # noqa: F401,F403
from legalize.fetcher.fr.parser import (  # noqa: F811
    LEGIMetadataParser,
    LEGITextParser,
    _extract_text_legi,
    _parse_date_legi,
    _parse_legi_combined,
    _titulo_corto_fr,
)

__all__ = [
    "LEGITextParser",
    "LEGIMetadataParser",
    "_extract_text_legi",
    "_parse_date_legi",
    "_parse_legi_combined",
    "_titulo_corto_fr",
]
