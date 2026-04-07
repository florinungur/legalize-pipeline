"""Tests for the Lithuanian TAR parser (data.gov.lt Spinta API)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.lt.parser import TARMetadataParser, TARTextParser
from legalize.models import NormMetadata, NormStatus, Reform
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures"


class TestTARTextParser:
    def setup_method(self):
        self.parser = TARTextParser()

    def test_parse_json_returns_blocks(self):
        data = (FIXTURES / "tar-text-sample.json").read_bytes()
        blocks = self.parser.parse_text(data)
        assert len(blocks) > 0

    def test_articles_detected(self):
        data = (FIXTURES / "tar-text-sample.json").read_bytes()
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        assert len(article_blocks) == 3

    def test_article_ids(self):
        data = (FIXTURES / "tar-text-sample.json").read_bytes()
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        ids = [b.id for b in article_blocks]
        assert ids == ["str1", "str2", "str3"]

    def test_article_has_paragraphs(self):
        data = (FIXTURES / "tar-text-sample.json").read_bytes()
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        # First article should have the heading + 2 body paragraphs
        assert len(article_blocks[0].versions[0].paragraphs) >= 2

    def test_article_title(self):
        data = (FIXTURES / "tar-text-sample.json").read_bytes()
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        assert "1 straipsnis" in article_blocks[0].title

    def test_extract_reforms_returns_list(self):
        data = (FIXTURES / "tar-text-sample.json").read_bytes()
        reforms = self.parser.extract_reforms(data)
        assert isinstance(reforms, list)

    def test_empty_data_returns_empty(self):
        blocks = self.parser.parse_text(b'{"_data": [{"tekstas_lt": ""}]}')
        assert blocks == []

    def test_no_items_returns_empty(self):
        blocks = self.parser.parse_text(b'{"_data": []}')
        assert blocks == []

    def test_structural_headings_detected(self):
        data = (FIXTURES / "tar-text-sample.json").read_bytes()
        blocks = self.parser.parse_text(data)
        # First block (before first article) should contain structural headings
        first_block = blocks[0]
        para_classes = [p.css_class for p in first_block.versions[0].paragraphs]
        assert "chapter_heading" in para_classes or "part_heading" in para_classes

    def test_plain_text_fallback(self):
        """Parser handles plain text (non-JSON) gracefully."""
        text = b"1 straipsnis. Test\n\nSome paragraph text."
        blocks = self.parser.parse_text(text)
        assert len(blocks) >= 1


class TestTARMetadataParser:
    def setup_method(self):
        self.parser = TARMetadataParser()

    def test_parse_civil_code(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        assert isinstance(meta, NormMetadata)
        assert meta.country == "lt"
        assert meta.identifier == "TAR.47BB952431DA"

    def test_title(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        assert "civilinis kodeksas" in meta.title.lower()

    def test_short_title(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        assert meta.short_title == "Civilinis kodeksas"

    def test_rank_istatymas(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        # Kodeksas maps to istatymas
        assert str(meta.rank) == "istatymas"

    def test_rank_konstitucija(self):
        json_data = (FIXTURES / "tar-metadata-TAR-1992-00001.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA-K")
        assert str(meta.rank) == "konstitucija"

    def test_publication_date(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        assert meta.publication_date == date(2000, 7, 18)

    def test_in_force_status(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        assert meta.status == NormStatus.IN_FORCE

    def test_repealed_status(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2020-99999.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2020-99999")
        assert meta.status == NormStatus.REPEALED

    def test_department(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        assert "Seimas" in meta.department

    def test_source_url(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        assert "e-tar.lt" in meta.source

    def test_last_modified_from_pakeista(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        # pakeista has "2004-11-09, 2011-06-21, 2023-05-23" → last_modified = max
        assert meta.last_modified == date(2023, 5, 23)

    def test_last_modified_falls_back_to_isigalioja(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2020-99999.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2020-99999")
        # pakeista is null → falls back to isigalioja (2020-07-01)
        assert meta.last_modified == date(2020, 7, 1)

    def test_short_title_falls_back_to_title(self):
        json_data = (FIXTURES / "tar-metadata-TAR-1992-00001.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA-K")
        # alt_pavadinimas is null, should fall back to pavadinimas
        assert meta.short_title == meta.title

    def test_extra_fields_tar_kodas(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR.47BB952431DA")
        extra_dict = dict(meta.extra)
        assert "tar_code" in extra_dict

    def test_empty_data_raises(self):
        import pytest

        empty = b'{"_data": []}'
        with pytest.raises(ValueError, match="No metadata found"):
            self.parser.parse(empty, "TAR-0000-00000")


class TestSuvestine:
    """Tests for parse_suvestine() — historical version parsing."""

    def setup_method(self):
        self.parser = TARTextParser()
        self.data = (FIXTURES / "tar-suvestine-sample.json").read_bytes()

    def test_returns_blocks_and_reforms(self):
        blocks, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        assert len(blocks) > 0
        assert len(reforms) > 0

    def test_three_reforms_from_three_versions(self):
        _, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        assert len(reforms) == 3

    def test_reform_dates(self):
        _, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        assert reforms[0].date == date(2000, 7, 18)
        assert reforms[1].date == date(2005, 3, 15)
        assert reforms[2].date == date(2010, 1, 1)

    def test_reform_norm_ids(self):
        _, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        assert reforms[0].norm_id == "TAR.TEST:V001"
        assert reforms[1].norm_id == "TAR.TEST:V002"

    def test_first_reform_is_bootstrap(self):
        _, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        # First reform should have all blocks as affected
        assert len(reforms[0].affected_blocks) > 0

    def test_blocks_have_multiple_versions(self):
        blocks, _ = self.parser.parse_suvestine(self.data, "TAR.TEST")
        # Article 1 (str1) should have 3 versions (changed in V2)
        art1 = [b for b in blocks if b.id == "str1"]
        assert len(art1) == 1
        assert len(art1[0].versions) == 3

    def test_article_detection(self):
        blocks, _ = self.parser.parse_suvestine(self.data, "TAR.TEST")
        article_blocks = [b for b in blocks if b.block_type == "article"]
        assert len(article_blocks) == 3  # 3 straipsnis in the fixture

    def test_second_reform_detects_changed_blocks(self):
        _, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        # V2 changed article 1 text (added "ir su jais susijusius...")
        assert "str1" in reforms[1].affected_blocks

    def test_unchanged_block_not_in_affected(self):
        _, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        # Article 3 didn't change between V1 and V2
        assert "str3" not in reforms[1].affected_blocks

    def test_empty_data_returns_empty(self):
        blocks, reforms = self.parser.parse_suvestine(b'{"_data": []}', "TAR.X")
        assert blocks == []
        assert reforms == []

    def test_reform_is_reform_type(self):
        _, reforms = self.parser.parse_suvestine(self.data, "TAR.TEST")
        assert all(isinstance(r, Reform) for r in reforms)


class TestCountriesDispatchLT:
    def test_get_text_parser_lt(self):
        parser = get_text_parser("lt")
        assert isinstance(parser, TARTextParser)

    def test_get_metadata_parser_lt(self):
        parser = get_metadata_parser("lt")
        assert isinstance(parser, TARMetadataParser)


class TestSlugLithuania:
    def test_norm_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="TAR.47BB952431DA",
            country="lt",
            rank="istatymas",
            publication_date=date(2000, 7, 18),
            status=NormStatus.IN_FORCE,
            department="Seimas",
            source="https://e-tar.lt/portal/lt/legalAct/TAR.47BB952431DA",
        )
        assert norm_to_filepath(meta) == "lt/TAR.47BB952431DA.md"
