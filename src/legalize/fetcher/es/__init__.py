"""Spain (ES) — BOE legislative fetcher components."""

from legalize.fetcher.es.client import BOEClient, RateLimiter
from legalize.fetcher.es.discovery import BOEDiscovery
from legalize.fetcher.es.parser import BOEMetadataParser, BOETextParser

__all__ = [
    "BOEClient",
    "RateLimiter",
    "BOEDiscovery",
    "BOETextParser",
    "BOEMetadataParser",
]
