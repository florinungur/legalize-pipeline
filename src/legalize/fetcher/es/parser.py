"""BOE-specific text and metadata parsers.

Wraps the existing xml_parser.py and metadata.py modules
behind the abstract TextParser and MetadataParser interfaces.
"""

from __future__ import annotations

from typing import Any

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import NormaMetadata


class BOETextParser(TextParser):
    """Parse BOE consolidated text XML into Bloque objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        from legalize.transformer.xml_parser import parse_text_xml

        return parse_text_xml(data)

    def extract_reforms(self, data: bytes) -> list[Any]:
        from legalize.transformer.xml_parser import extract_reforms

        return extract_reforms(data)


class BOEMetadataParser(MetadataParser):
    """Parse BOE metadata XML into NormaMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormaMetadata:
        from legalize.fetcher.es.metadata import parse_metadata

        return parse_metadata(data, norm_id)
