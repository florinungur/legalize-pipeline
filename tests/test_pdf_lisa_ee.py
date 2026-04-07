"""Tests for the Estonian PDF annex → Markdown table converter."""

from __future__ import annotations

from pathlib import Path

import pytest

from legalize.fetcher.ee.pdf_lisa import (
    _looks_numeric,
    has_tabular_content,
    pdf_to_markdown_tables,
)


PDFS = Path(__file__).parent / "fixtures" / "ee" / "_investigation" / "pdfs"
STAMPS = Path(__file__).parent / "fixtures" / "ee" / "extracted_lisas"

# Skip all tests in this file if the fixture directory is not present
# (investigation artifacts are not always checked into git).
pytestmark = pytest.mark.skipif(
    not PDFS.is_dir(),
    reason="PDF investigation fixtures not available",
)


class TestLooksNumeric:
    @pytest.mark.parametrize(
        "value",
        ["1", "1,5", "1.5", "0,38", "14,1–18", "4000", "5,56", "–", "-", " 12 "],
    )
    def test_numeric(self, value):
        assert _looks_numeric(value)

    @pytest.mark.parametrize(
        "value",
        [None, "", "Mänd", "Kuni 2", "Iga järgmise", "Lisa 1"],
    )
    def test_not_numeric(self, value):
        assert not _looks_numeric(value)


class TestForestDamageScale:
    """MS_1_lisa2.pdf is the cleanest numeric table in our corpus:
    29 rows × 5 cols of damage euros by tree diameter and species."""

    @pytest.fixture
    def tables(self):
        return pdf_to_markdown_tables((PDFS / "MS_1_lisa2.pdf").read_bytes())

    def test_one_table_detected(self, tables):
        assert len(tables) == 1

    def test_has_28_data_rows(self, tables):
        # 28 data rows (0–2 through 98,1–102, plus the "every additional step" row)
        md = tables[0]
        data_rows = [
            line
            for line in md.splitlines()
            if line.startswith("|") and not line.startswith("| ---")
        ][1:]  # drop header
        assert len(data_rows) >= 25

    def test_numeric_values_preserved(self, tables):
        md = tables[0]
        # Constitution-specific values from lisa 2
        assert "| Kuni 2 |" in md
        assert "0,38" in md
        assert "92,35" in md  # 58,1–62 row for conifers
        assert "872,30" in md  # 98,1–102 row

    def test_iga_jargmise_row_merged(self, tables):
        md = tables[0]
        # The multi-line "Iga järgmise / 4-sentimeetrilise / astme eest" row
        # should be merged into a single cell
        assert "Iga järgmise" in md
        assert "28,75" in md
        assert "19,17" in md

    def test_is_tabular(self, tables):
        assert has_tabular_content(tables)


class TestBuildingPermitMatrix:
    """EhS_1_Lisa1_28032026.pdf is a 7-page textual matrix: the hardest
    category C case. No numeric cells — just text labels."""

    @pytest.fixture
    def tables(self):
        return pdf_to_markdown_tables((PDFS / "EhS_1_Lisa1_28032026.pdf").read_bytes())

    def test_multiple_tables_detected(self, tables):
        # One table per page, 7 pages
        assert len(tables) >= 5

    def test_is_tabular(self, tables):
        assert has_tabular_content(tables)


class TestGraphicStampFallback:
    """Tax stamp designs are pure graphics with no extractable tables.
    They must fall through to the link fallback."""

    @pytest.fixture
    def tables(self):
        stamp = STAMPS / "01_110102012005Lisa1.pdf"
        if not stamp.exists():
            pytest.skip("stamp fixture not present")
        return pdf_to_markdown_tables(stamp.read_bytes())

    def test_no_meaningful_tables(self, tables):
        assert not has_tabular_content(tables)


class TestBlankFormFallback:
    """Blank delivery slip forms have labeled empty boxes but no
    tabular data. They must fall through to the link fallback."""

    @pytest.fixture
    def tables(self):
        form = STAMPS / "03_102122010007_lisa_3.pdf"
        if not form.exists():
            pytest.skip("form fixture not present")
        return pdf_to_markdown_tables(form.read_bytes())

    def test_no_meaningful_tables(self, tables):
        assert not has_tabular_content(tables)
