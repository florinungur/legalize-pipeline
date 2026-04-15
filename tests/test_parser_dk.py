"""Tests for the Danish LexDania 2.1 parser.

Fixtures are real XML documents from retsinformation.dk:
  - 2024-434.xml: LBK H (Straffeloven — Criminal Code, 395 §§, tables, notes)
  - 2024-62.xml: LBK H (Lov om hold af dyr — consolidated law)
  - 2024-1709.xml: BEK H (Sygedagpenge — executive order)
  - 2023-1547.xml: LOV Æ (Amendment law with amendment instructions)
  - 2020-1061.xml: LOV Æ (Property tax amendment with tables)
"""

from pathlib import Path

import pytest

from legalize.fetcher.dk.parser import DanishMetadataParser, DanishTextParser
from legalize.models import NormStatus

FIXTURES = Path(__file__).parent / "fixtures" / "dk"


@pytest.fixture
def text_parser():
    return DanishTextParser()


@pytest.fixture
def meta_parser():
    return DanishMetadataParser()


# ─── Straffeloven (Criminal Code) — LBK H, 395 §§, tables, notes ───


class TestStraffeloven:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.data = (FIXTURES / "2024-434.xml").read_bytes()

    def test_parse_produces_blocks(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        assert len(blocks) > 100, f"Expected >100 blocks, got {len(blocks)}"

    def test_chapters_present(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        chapters = [b for b in blocks if b.block_type == "chapter"]
        assert len(chapters) >= 20, f"Expected >=20 chapters, got {len(chapters)}"

    def test_articles_present(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        articles = [b for b in blocks if b.block_type == "article"]
        assert len(articles) >= 300, f"Expected >=300 articles, got {len(articles)}"

    def test_paragraf_1_text(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        # Find § 1 — title may contain non-breaking space (\xa0)
        par1 = next(
            (b for b in blocks if "§" in b.title and "1." in b.title),
            None,
        )
        assert par1 is not None, "§ 1 not found"
        text = "\n".join(p.text for p in par1.versions[0].paragraphs)
        assert "§" in text
        assert "straffelov" in text.lower() or "straf" in text.lower()

    def test_table_rendered(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        table_blocks = [
            b
            for b in blocks
            if any(p.css_class == "table_row" and "|" in p.text for p in b.versions[0].paragraphs)
        ]
        assert len(table_blocks) >= 1, "Expected at least 1 table"
        # Verify table has pipe table format
        table_text = next(
            p.text
            for b in table_blocks
            for p in b.versions[0].paragraphs
            if p.css_class == "table_row"
        )
        assert "| ---" in table_text, "Table should have markdown separator"

    def test_list_items_present(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        all_paragraphs = [p for b in blocks for p in b.versions[0].paragraphs]
        list_items = [p for p in all_paragraphs if p.css_class == "list_item"]
        assert len(list_items) >= 50, f"Expected >=50 list items, got {len(list_items)}"

    def test_metadata(self, meta_parser):
        meta = meta_parser.parse(self.data, "lta/2024/434")
        assert meta.title == "Bekendtgørelse af straffeloven"
        assert meta.identifier == "A20240043429"
        assert meta.country == "dk"
        assert meta.rank == "lovbekendtgoerelse"
        assert meta.status == NormStatus.REPEALED  # This is a Historic document
        assert meta.publication_date.year == 2024
        assert meta.department == "Justitsministeriet"
        assert "retsinformation.dk" in meta.source

    def test_reforms_extracted(self, text_parser):
        reforms = text_parser.extract_reforms(self.data)
        # Straffeloven has multiple Change elements
        assert len(reforms) >= 1, "Expected at least 1 reform"
        assert all(r.date is not None for r in reforms)


# ─── Lov om hold af dyr — LBK H, consolidated law ───


class TestHoldAfDyr:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.data = (FIXTURES / "2024-62.xml").read_bytes()

    def test_parse_produces_blocks(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        assert len(blocks) > 30

    def test_introduction_present(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        preamble = [b for b in blocks if b.block_type == "preamble"]
        assert len(preamble) >= 1, "Expected introduction/preamble block"
        intro_text = preamble[0].versions[0].paragraphs[0].text
        assert "bekendtgøres" in intro_text.lower()

    def test_metadata_valid_status(self, meta_parser):
        meta = meta_parser.parse(self.data, "lta/2024/62")
        assert meta.status == NormStatus.IN_FORCE
        assert meta.identifier == "A20240006229"
        assert "hold af dyr" in meta.title.lower()
        # Check extra fields
        extra_dict = dict(meta.extra)
        assert extra_dict.get("document_type") == "LBK H"
        assert extra_dict.get("number") == "62"

    def test_chapters_and_articles(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        chapters = [b for b in blocks if b.block_type == "chapter"]
        articles = [b for b in blocks if b.block_type == "article"]
        assert len(chapters) >= 5
        assert len(articles) >= 20


# ─── BEK H — Executive order ───


class TestBekendtgoerelse:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.data = (FIXTURES / "2024-1709.xml").read_bytes()

    def test_parse_produces_blocks(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        assert len(blocks) > 10

    def test_metadata_rank_bek(self, meta_parser):
        meta = meta_parser.parse(self.data, "lta/2024/1709")
        assert meta.rank == "bekendtgoerelse"
        assert meta.identifier == "B20240170905"
        assert meta.country == "dk"


# ─── LOV Æ — Amendment law ───


class TestAmendmentLaw:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.data = (FIXTURES / "2023-1547.xml").read_bytes()

    def test_parse_produces_blocks(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        assert len(blocks) > 0

    def test_metadata_rank_amendment(self, meta_parser):
        meta = meta_parser.parse(self.data, "lta/2023/1547")
        assert meta.rank == "aendringslov"
        assert "ændring" in meta.title.lower()


# ─── LOV Æ with tables — Property tax amendment ───


class TestAmendmentWithTable:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.data = (FIXTURES / "2020-1061.xml").read_bytes()

    def test_parse_produces_blocks(self, text_parser):
        blocks = text_parser.parse_text(self.data)
        assert len(blocks) > 5

    def test_metadata(self, meta_parser):
        meta = meta_parser.parse(self.data, "lta/2020/1061")
        assert meta.identifier == "A20200106130"
        assert meta.department == "Skatteministeriet"


# ─── Edge cases ───


class TestEdgeCases:
    def test_empty_data(self, text_parser):
        assert text_parser.parse_text(b"") == []

    def test_invalid_xml(self, text_parser):
        assert text_parser.parse_text(b"<not valid") == []

    def test_metadata_only_xml(self, text_parser, meta_parser):
        """XML with Meta but no DokumentIndhold should produce empty blocks."""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
        <Dokument>
          <Meta>
            <DocumentType>Lov</DocumentType>
            <AccessionNumber>A19530016930</AccessionNumber>
            <DocumentTitle>Test law</DocumentTitle>
            <DiesSigni>1953-06-05</DiesSigni>
            <Status>Valid</Status>
            <Year>1953</Year>
            <Number>169</Number>
          </Meta>
        </Dokument>"""
        blocks = text_parser.parse_text(xml)
        assert blocks == []

        meta = meta_parser.parse(xml, "lta/1953/169")
        assert meta.title == "Test law"
        assert meta.identifier == "A19530016930"
