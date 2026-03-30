"""France (FR) — LEGI legislative fetcher components."""

from legalize.fetcher.fr.client import LEGIClient
from legalize.fetcher.fr.discovery import LEGIDiscovery
from legalize.fetcher.fr.parser import LEGIMetadataParser, LEGITextParser

__all__ = [
    "LEGIClient",
    "LEGIDiscovery",
    "LEGITextParser",
    "LEGIMetadataParser",
]
