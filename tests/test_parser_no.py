"""Tests for the Norwegian Lovdata parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path


from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.no.parser import (
    LovdataMetadataParser,
    LovdataTextParser,
    _clean_text,
    _parse_date_no,
)
from legalize.models import NormStatus, Rank

FIXTURES = Path(__file__).parent / "fixtures" / "no"

CONSTITUTION = FIXTURES / "sample-constitution.xml"
PENAL_CODE = FIXTURES / "sample-penal-code.xml"
INSURANCE_ACT = FIXTURES / "sample-insurance-act.xml"
WITH_TABLES = FIXTURES / "sample-with-tables.xml"
RECENT_ACT = FIXTURES / "sample-recent-act.xml"


# ─────────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────────


class TestUtilities:
    def test_clean_text_strips_nbsp(self):
        assert _clean_text("hello\xa0world") == "hello world"

    def test_clean_text_strips_control_chars(self):
        assert _clean_text("hello\x00world") == "helloworld"

    def test_clean_text_normalizes_whitespace(self):
        assert _clean_text("  hello   world  ") == "hello world"

    def test_parse_date_iso(self):
        assert _parse_date_no("2024-05-21") == date(2024, 5, 21)

    def test_parse_date_with_time(self):
        assert _parse_date_no("2023-06-09 12:00") == date(2023, 6, 9)

    def test_parse_date_empty(self):
        assert _parse_date_no("") is None


# ─────────────────────────────────────────────
# Text parser
# ─────────────────────────────────────────────


class TestLovdataTextParser:
    def setup_method(self):
        self.parser = LovdataTextParser()

    def test_parse_constitution(self):
        blocks = self.parser.parse_text(CONSTITUTION.read_bytes())
        assert len(blocks) > 0

    def test_constitution_has_articles(self):
        blocks = self.parser.parse_text(CONSTITUTION.read_bytes())
        articles = [b for b in blocks if b.block_type == "article"]
        # Constitution has ~121 articles (§§)
        assert len(articles) >= 100

    def test_constitution_has_sections(self):
        blocks = self.parser.parse_text(CONSTITUTION.read_bytes())
        sections = [b for b in blocks if b.block_type == "section"]
        # Constitution has parts: A, B, C, D, E, F
        assert len(sections) >= 5

    def test_penal_code_has_many_articles(self):
        blocks = self.parser.parse_text(PENAL_CODE.read_bytes())
        articles = [b for b in blocks if b.block_type == "article"]
        # Straffeloven has ~400 articles
        assert len(articles) >= 200

    def test_penal_code_has_chapters(self):
        blocks = self.parser.parse_text(PENAL_CODE.read_bytes())
        sections = [b for b in blocks if b.block_type == "section"]
        # 3 parts + ~30 chapters
        assert len(sections) >= 20

    def test_article_has_paragraphs(self):
        blocks = self.parser.parse_text(RECENT_ACT.read_bytes())
        articles = [b for b in blocks if b.block_type == "article"]
        assert len(articles) >= 5
        # First article should have articulo heading + body paragraphs
        first = articles[0]
        assert len(first.versions) == 1
        assert len(first.versions[0].paragraphs) >= 2

    def test_article_heading_has_section_number(self):
        blocks = self.parser.parse_text(RECENT_ACT.read_bytes())
        articles = [b for b in blocks if b.block_type == "article"]
        first_para = articles[0].versions[0].paragraphs[0]
        assert first_para.css_class == "articulo"
        assert "§" in first_para.text

    def test_strips_amendment_history(self):
        """changesToParent elements should NOT appear in output text."""
        blocks = self.parser.parse_text(PENAL_CODE.read_bytes())
        all_text = " ".join(p.text for b in blocks for v in b.versions for p in v.paragraphs)
        # "Endret ved" is the amendment history prefix — should be stripped
        # NOTE: some articles may reference amendments in their actual text,
        # but the bulk of "Endret ved lov(er)" occurrences are in changesToParent
        endret_count = all_text.count("Endret ved lov")
        # The penal code has 370 changesToParent entries, but very few
        # legitimate uses of "Endret ved" in article text
        assert endret_count < 10

    def test_tables_parsed(self):
        blocks = self.parser.parse_text(WITH_TABLES.read_bytes())
        table_blocks = [
            b
            for b in blocks
            for v in b.versions
            for p in v.paragraphs
            if p.css_class == "table_row"
        ]
        assert len(table_blocks) >= 1

    def test_cross_references_as_text(self):
        """<a href='lov/...'> should be plain text, not broken markup."""
        blocks = self.parser.parse_text(RECENT_ACT.read_bytes())
        all_text = " ".join(p.text for b in blocks for v in b.versions for p in v.paragraphs)
        # Should contain reference text without HTML artifacts
        assert "<a " not in all_text
        assert "&lt;" not in all_text

    def test_no_reforms_from_public_data(self):
        """Public data has no historical versions — reforms should be empty."""
        reforms = self.parser.extract_reforms(RECENT_ACT.read_bytes())
        assert reforms == []

    def test_lists_in_articles(self):
        """Articles containing <ul>/<ol> should have list_item paragraphs."""
        blocks = self.parser.parse_text(PENAL_CODE.read_bytes())
        list_paras = [
            p
            for b in blocks
            for v in b.versions
            for p in v.paragraphs
            if p.css_class == "list_item"
        ]
        assert len(list_paras) > 0


# ─────────────────────────────────────────────
# Metadata parser
# ─────────────────────────────────────────────


class TestLovdataMetadataParser:
    def setup_method(self):
        self.parser = LovdataMetadataParser()

    def test_constitution_metadata(self):
        meta = self.parser.parse(CONSTITUTION.read_bytes(), "nl-18140517-000")
        assert meta.identifier == "LOV-1814-05-17"
        assert meta.country == "no"
        assert meta.rank == Rank("grunnlov")
        assert "Grunnlov" in meta.title
        assert meta.status == NormStatus.IN_FORCE

    def test_penal_code_metadata(self):
        meta = self.parser.parse(PENAL_CODE.read_bytes(), "nl-20050520-028")
        assert meta.identifier == "LOV-2005-05-20-28"
        assert meta.rank == Rank("lov")
        assert "straff" in meta.title.lower()
        assert meta.publication_date == date(2005, 5, 20)
        assert meta.department != ""

    def test_recent_act_metadata(self):
        meta = self.parser.parse(RECENT_ACT.read_bytes(), "nl-20230609-026")
        assert meta.identifier == "LOV-2023-06-09-26"
        assert meta.publication_date == date(2023, 6, 9)

    def test_short_title(self):
        meta = self.parser.parse(PENAL_CODE.read_bytes(), "nl-20050520-028")
        # Short title includes abbreviation: "Straffeloven – strl."
        assert meta.short_title != ""
        assert len(meta.short_title) < len(meta.title)

    def test_subjects_extracted(self):
        meta = self.parser.parse(PENAL_CODE.read_bytes(), "nl-20050520-028")
        assert len(meta.subjects) > 0

    def test_extra_fields(self):
        meta = self.parser.parse(PENAL_CODE.read_bytes(), "nl-20050520-028")
        extra_keys = [k for k, v in meta.extra]
        assert "dokid" in extra_keys
        assert "refid" in extra_keys

    def test_source_url(self):
        meta = self.parser.parse(PENAL_CODE.read_bytes(), "nl-20050520-028")
        assert meta.source.startswith("https://lovdata.no/")

    def test_last_modified(self):
        meta = self.parser.parse(PENAL_CODE.read_bytes(), "nl-20050520-028")
        assert meta.last_modified is not None
        assert meta.last_modified > date(2020, 1, 1)

    def test_insurance_act_eea_references(self):
        meta = self.parser.parse(INSURANCE_ACT.read_bytes(), "nl-19970228-019")
        extra_keys = [k for k, v in meta.extra]
        assert "eea_references" in extra_keys


# ─────────────────────────────────────────────
# Registry integration
# ─────────────────────────────────────────────


class TestRegistry:
    def test_text_parser_registered(self):
        parser = get_text_parser("no")
        assert isinstance(parser, LovdataTextParser)

    def test_metadata_parser_registered(self):
        parser = get_metadata_parser("no")
        assert isinstance(parser, LovdataMetadataParser)
