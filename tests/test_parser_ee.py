"""Tests for the Estonian Riigi Teataja parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from legalize.fetcher.ee.parser import RTMetadataParser, RTTextParser
from legalize.models import NormStatus
from legalize.transformer.markdown import render_norm_at_date


FIXTURES = Path(__file__).parent / "fixtures" / "ee"


def _read(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


# ─────────────────────────────────────────────
# Metadata
# ─────────────────────────────────────────────


class TestMetadataConstitution:
    """Estonian Constitution (terviktekst, current version 2015-08-13)."""

    @pytest.fixture
    def metadata(self):
        return RTMetadataParser().parse(_read("constitution_115052015002.xml"), "115052015002")

    def test_identifier_is_global_id(self, metadata):
        assert metadata.identifier == "115052015002"

    def test_country(self, metadata):
        assert metadata.country == "ee"

    def test_title_is_estonian(self, metadata):
        assert metadata.title == "Eesti Vabariigi põhiseadus"

    def test_short_title_is_lyhend(self, metadata):
        assert metadata.short_title == "PS"

    def test_rank(self, metadata):
        assert metadata.rank == "seadus"

    def test_publication_date(self, metadata):
        assert metadata.publication_date == date(2015, 5, 15)

    def test_status_repealed_because_kehtivus_lopp_in_past(self, metadata):
        # This version's kehtivuseLopp is 2025-07-08, before today
        assert metadata.status == NormStatus.REPEALED

    def test_department_is_issuer(self, metadata):
        assert "Rahvahääletusel" in metadata.department

    def test_source_url(self, metadata):
        assert metadata.source == "https://www.riigiteataja.ee/akt/115052015002"

    def test_subjects_includes_pohiseadus(self, metadata):
        assert "põhiseadus" in metadata.subjects

    def test_extra_has_group_id(self, metadata):
        extra = dict(metadata.extra)
        assert extra.get("group_id") == "151381"

    def test_extra_has_text_type(self, metadata):
        extra = dict(metadata.extra)
        assert extra.get("text_type") == "terviktekst"

    def test_extra_has_adoption_date(self, metadata):
        extra = dict(metadata.extra)
        assert extra.get("adoption_date") == "1992-06-28"

    def test_extra_has_effective_dates(self, metadata):
        extra = dict(metadata.extra)
        assert extra.get("effective_from") == "2015-08-13"
        assert extra.get("effective_until") == "2025-07-08"

    def test_extra_has_rt_section(self, metadata):
        extra = dict(metadata.extra)
        assert extra.get("rt_section") == "RT I"


# ─────────────────────────────────────────────
# Text structure
# ─────────────────────────────────────────────


class TestTextConstitution:
    """Parsing the structural body of the Constitution."""

    @pytest.fixture
    def blocks(self):
        return RTTextParser().parse_text(_read("constitution_115052015002.xml"))

    def test_has_blocks(self, blocks):
        assert len(blocks) > 50  # Constitution has 168 paragrahvs + 15 chapters

    def test_first_block_is_preamble(self, blocks):
        assert blocks[0].block_type == "preamble"

    def test_preamble_mentions_kindlustada(self, blocks):
        # The Estonian Constitution preamble starts with "Kõikumatus usus..."
        preamble = blocks[0]
        text = " ".join(p.text for p in preamble.versions[0].paragraphs)
        assert "kindlustada" in text.lower()

    def test_has_chapter_blocks(self, blocks):
        chapters = [b for b in blocks if b.block_type == "ptk"]
        assert len(chapters) >= 15  # The Constitution has 15 chapters

    def test_first_chapter_is_uldsatted(self, blocks):
        chapters = [b for b in blocks if b.block_type == "ptk"]
        assert "ÜLDSÄTTED" in chapters[0].title.upper()

    def test_has_article_blocks(self, blocks):
        articles = [b for b in blocks if b.block_type == "article"]
        assert len(articles) > 100

    def test_paragrahv_1_text(self, blocks):
        # § 1 of the Constitution
        articles = [b for b in blocks if b.block_type == "article"]
        para1 = next(b for b in articles if "§ 1." in b.title)
        text = " ".join(p.text for p in para1.versions[0].paragraphs)
        assert "Eesti on iseseisev ja sõltumatu demokraatlik vabariik" in text

    def test_paragrahv_1_loiged_have_numbers(self, blocks):
        articles = [b for b in blocks if b.block_type == "article"]
        para1 = next(b for b in articles if "§ 1." in b.title)
        body_paragraphs = [p for p in para1.versions[0].paragraphs if p.css_class == "parrafo"]
        # Both loiged should be prefixed with (1) and (2)
        joined = " ".join(p.text for p in body_paragraphs)
        assert "(1)" in joined
        assert "(2)" in joined


# ─────────────────────────────────────────────
# Markdown rendering
# ─────────────────────────────────────────────


class TestMarkdownConstitution:
    """End-to-end: parse Constitution → render markdown."""

    @pytest.fixture
    def rendered(self):
        data = _read("constitution_115052015002.xml")
        meta = RTMetadataParser().parse(data, "115052015002")
        blocks = RTTextParser().parse_text(data)
        return render_norm_at_date(meta, blocks, date(2025, 1, 1), include_all=True)

    def test_has_frontmatter(self, rendered):
        assert rendered.startswith("---\n")
        assert 'country: "ee"' in rendered

    def test_has_title_h1(self, rendered):
        assert "# Eesti Vabariigi põhiseadus" in rendered

    def test_has_chapter_h3(self, rendered):
        # peatykk → ### in our mapping
        assert "### " in rendered
        assert "ÜLDSÄTTED" in rendered

    def test_has_paragrahv_h5(self, rendered):
        # paragrahv → ##### in our mapping
        assert "##### § 1." in rendered

    def test_paragrahv_1_text_present(self, rendered):
        assert "Eesti on iseseisev ja sõltumatu demokraatlik vabariik" in rendered

    def test_paragrahv_1_has_numbered_loiged(self, rendered):
        # Looking for "(1) Eesti on iseseisev"
        assert "(1) Eesti on iseseisev" in rendered

    def test_no_xml_tags_leaked(self, rendered):
        assert "<tavatekst" not in rendered
        assert "<paragrahv" not in rendered


# ─────────────────────────────────────────────
# Other fixtures (smoke tests)
# ─────────────────────────────────────────────


class TestPenalCode:
    """Karistusseadustik — biggest stress test."""

    @pytest.fixture
    def parsed(self):
        data = _read("penal_code_KarS_122122025002.xml")
        meta = RTMetadataParser().parse(data, "122122025002")
        blocks = RTTextParser().parse_text(data)
        return meta, blocks

    def test_metadata_short_title_is_kars(self, parsed):
        meta, _ = parsed
        assert meta.short_title == "KarS"

    def test_has_osa_blocks(self, parsed):
        _, blocks = parsed
        osa_blocks = [b for b in blocks if b.block_type == "osa"]
        # KarS is split into 2 main parts: Üldosa (general) + Eriosa (special)
        assert len(osa_blocks) == 2

    def test_has_many_articles(self, parsed):
        _, blocks = parsed
        articles = [b for b in blocks if b.block_type == "article"]
        assert len(articles) >= 500


class TestAmendmentHTMLKonteiner:
    """Amendment with HTMLKonteiner CDATA HTML."""

    def test_parses_without_error(self):
        data = _read("amendment_103012025003.xml")
        meta = RTMetadataParser().parse(data, "103012025003")
        blocks = RTTextParser().parse_text(data)
        rendered = render_norm_at_date(meta, blocks, date(2025, 1, 6), include_all=True)
        # Bold from <b>1)</b> in HTMLKonteiner should appear
        assert "**1)**" in rendered or "1)" in rendered
        # Should have some text body
        assert "kodakondsus" in rendered

    def test_metadata_text_type_is_algtekst(self):
        data = _read("amendment_103012025003.xml")
        meta = RTMetadataParser().parse(data, "103012025003")
        extra = dict(meta.extra)
        assert extra.get("text_type") == "algtekst"


class TestConstitutionVersions:
    """Validate that all 3 historical versions share the same group_id."""

    def test_three_versions_same_group(self):
        files = [
            "constitution_OLDEST_12846827.xml",
            "constitution_PREV_127042011002.xml",
            "constitution_115052015002.xml",
        ]
        group_ids = []
        for f in files:
            meta = RTMetadataParser().parse(_read(f), f)
            extra = dict(meta.extra)
            group_ids.append(extra.get("group_id"))
        assert len(set(group_ids)) == 1
        assert group_ids[0] == "151381"
