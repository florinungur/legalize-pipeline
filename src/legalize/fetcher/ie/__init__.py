"""Ireland (IE) -- legislative fetcher components.

Source: Irish Statute Book (ISB) for Act text (XML),
        Oireachtas API for discovery and metadata (JSON).
"""

from legalize.fetcher.ie.client import ISBClient
from legalize.fetcher.ie.discovery import ISBDiscovery
from legalize.fetcher.ie.parser import ISBMetadataParser, ISBTextParser

__all__ = [
    "ISBClient",
    "ISBDiscovery",
    "ISBTextParser",
    "ISBMetadataParser",
]
