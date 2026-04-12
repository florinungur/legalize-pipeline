"""Argentina (AR) -- legislative fetcher for InfoLEG (Ministerio de Justicia / SAIJ).

Discovery: monthly CSV catalog downloaded from datos.jus.gob.ar.
Per-norm text: legacy HTML host servicios.infoleg.gob.ar (windows-1252).
Reform reconstruction: per-modificatoria text parsed for "Sustitúyese..." patterns.

See ../../../../RESEARCH-AR.md for the full source inventory and the version
reconstruction strategy validated in the POC on 2026-04-11.
"""

from legalize.fetcher.ar.client import InfoLEGClient
from legalize.fetcher.ar.discovery import InfoLEGDiscovery
from legalize.fetcher.ar.parser import InfoLEGMetadataParser, InfoLEGTextParser

__all__ = [
    "InfoLEGClient",
    "InfoLEGDiscovery",
    "InfoLEGTextParser",
    "InfoLEGMetadataParser",
]
