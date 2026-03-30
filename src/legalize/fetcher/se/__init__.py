"""Sweden (SE) — Riksdagen legislative fetcher components."""

from legalize.fetcher.se.client import SwedishClient
from legalize.fetcher.se.discovery import SwedishDiscovery
from legalize.fetcher.se.parser import SwedishMetadataParser, SwedishTextParser

__all__ = [
    "SwedishClient",
    "SwedishDiscovery",
    "SwedishTextParser",
    "SwedishMetadataParser",
]
