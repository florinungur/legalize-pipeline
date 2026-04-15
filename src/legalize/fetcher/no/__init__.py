"""Norway (NO) — legislative fetcher components.

Source: Lovdata public data API (api.lovdata.no).
License: NLOD 2.0. No authentication required for public data.
"""

from legalize.fetcher.no.client import LovdataClient
from legalize.fetcher.no.discovery import LovdataDiscovery
from legalize.fetcher.no.parser import LovdataMetadataParser, LovdataTextParser

__all__ = [
    "LovdataClient",
    "LovdataDiscovery",
    "LovdataTextParser",
    "LovdataMetadataParser",
]
