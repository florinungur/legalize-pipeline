"""Tests for the Estonian Riigi Teataja bulk-dump discovery."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from legalize.fetcher.ee.discovery import (
    RTDiscovery,
    _parse_header,
)


FIXTURES = Path(__file__).parent / "fixtures" / "ee"


def _make_discovery(legi_dir: Path, **kwargs) -> RTDiscovery:
    defaults = dict(
        bulk_url="https://example.invalid",
        legi_dir=legi_dir,
        bulk_dir=legi_dir.parent / "bulk",
        document_types=("seadus", "määrus"),
        text_types=("terviktekst", "algtekst-terviktekst"),
        start_year=2010,
        end_year=None,
    )
    defaults.update(kwargs)
    return RTDiscovery(**defaults)


class TestHeaderParser:
    """Streaming header parser on the real fixture XMLs."""

    def test_parse_constitution_header(self):
        info = _parse_header(FIXTURES / "constitution_115052015002.xml")
        assert info is not None
        assert info.global_id == "115052015002"
        assert info.group_id == "151381"
        assert info.doc_type == "seadus"
        assert info.text_type == "terviktekst"
        assert info.effective_from == date(2015, 8, 13)

    def test_parse_penal_code_header(self):
        info = _parse_header(FIXTURES / "penal_code_KarS_122122025002.xml")
        assert info is not None
        assert info.global_id == "122122025002"
        assert info.doc_type == "seadus"
        assert info.text_type == "terviktekst"

    def test_parse_amendment_header(self):
        info = _parse_header(FIXTURES / "amendment_103012025003.xml")
        assert info is not None
        assert info.doc_type == "määrus"
        assert info.text_type == "algtekst"

    def test_parse_older_constitution_header(self):
        info = _parse_header(FIXTURES / "constitution_OLDEST_12846827.xml")
        assert info is not None
        # Legacy 8-digit ID format
        assert info.global_id == "12846827"
        assert info.group_id == "151381"


class TestDiscoverAllWithFixtures:
    """Run the header walker against a tiny directory of fixtures."""

    @pytest.fixture
    def legi_dir(self, tmp_path: Path) -> Path:
        """Copy a curated set of fixtures into a tmp legi_dir."""
        legi = tmp_path / "legi"
        legi.mkdir()
        for name in (
            "constitution_115052015002.xml",
            "constitution_PREV_127042011002.xml",
            "constitution_OLDEST_12846827.xml",
            "penal_code_KarS_122122025002.xml",
            "income_tax_TuMS_118122025017.xml",
            "amendment_103012025003.xml",  # algtekst — should be filtered out
        ):
            src = FIXTURES / name
            # lxml iterparse requires the filename to stay "something.xml"
            dst = legi / f"{_parse_header(src).global_id}.xml"
            dst.write_bytes(src.read_bytes())
        return legi

    def test_discover_all_groups_constitution_versions(self, legi_dir):
        """All three Constitution versions should collapse to 1 canonical ID
        (the most-recent-effective one: 115052015002)."""
        d = _make_discovery(legi_dir)
        ids = list(d.discover_all(client=None))
        # Constitution (3 versions → 1) + penal code + income tax = 3
        assert len(ids) == 3
        # Amendment (algtekst) is filtered out
        assert "103012025003" not in ids
        # Constitution canonical is the latest effective version
        assert "115052015002" in ids
        assert "127042011002" not in ids
        assert "12846827" not in ids

    def test_discover_all_respects_doc_type_filter(self, legi_dir):
        d = _make_discovery(legi_dir, document_types=("seadus",))
        ids = set(d.discover_all(client=None))
        # Only seadus: Constitution + KarS + TuMS = 3
        assert len(ids) == 3

    def test_discover_all_respects_text_type_filter(self, legi_dir):
        d = _make_discovery(legi_dir, text_types=("algtekst",))
        ids = list(d.discover_all(client=None))
        # Only the amendment passes this filter
        assert ids == ["103012025003"]

    def test_discover_daily_matches_effective_date(self, legi_dir):
        d = _make_discovery(legi_dir)
        ids = list(d.discover_daily(client=None, target_date=date(2015, 8, 13)))
        assert "115052015002" in ids

    def test_discover_daily_empty_for_random_date(self, legi_dir):
        d = _make_discovery(legi_dir)
        ids = list(d.discover_daily(client=None, target_date=date(1990, 1, 1)))
        assert ids == []


class TestPathResolution:
    """Legacy globaalIDs need padding to locate the zip entry."""

    def test_find_xml_path_with_padded_legacy_id(self, tmp_path: Path):
        legi = tmp_path / "legi"
        legi.mkdir()
        # Simulate the zip layout: legacy ID stored padded to 12 chars
        padded = legi / "000000076913.xml"
        padded.write_bytes(b"<?xml version='1.0'?><root/>")

        d = _make_discovery(legi)
        # The <globaalID> inside the XML would say "76913" (no padding)
        assert d.find_xml_path("76913") == padded
        # Direct match also works
        assert d.find_xml_path("000000076913") == padded

    def test_find_xml_path_with_modern_12_digit(self, tmp_path: Path):
        legi = tmp_path / "legi"
        legi.mkdir()
        modern = legi / "115052015002.xml"
        modern.write_bytes(b"<?xml version='1.0'?><root/>")

        d = _make_discovery(legi)
        assert d.find_xml_path("115052015002") == modern

    def test_find_xml_path_missing_returns_none(self, tmp_path: Path):
        legi = tmp_path / "legi"
        legi.mkdir()
        d = _make_discovery(legi)
        assert d.find_xml_path("999999999999") is None


class TestHeaderMemoryFreeing:
    """Ensure the streaming parser handles the biggest fixture without blowing up."""

    def test_large_file_parses_quickly(self):
        # penal_code_KarS is 1.1 MB; the header parser should bail out after
        # a few KB and return fast.
        import time

        t0 = time.monotonic()
        info = _parse_header(FIXTURES / "penal_code_KarS_122122025002.xml")
        elapsed = time.monotonic() - t0
        assert info is not None
        # Should be well under 100ms even on a slow machine
        assert elapsed < 0.5, f"Header parse took {elapsed:.3f}s"
