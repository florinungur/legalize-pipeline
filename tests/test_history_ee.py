"""Tests for the Estonian Eelmine chain crawler."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from legalize.fetcher.ee.history import (
    HistoricalVersion,
    _extract_gid_from_href,
    canonical_filename_id,
    extract_dates_from_xml,
    extract_eelmine_gid,
    extract_jargmine_gid,
    follow_chain_backwards,
    follow_chain_forwards,
    full_history,
    validate_chain_contiguity,
)


# ─────────────────────────────────────────────
# HTML parsing
# ─────────────────────────────────────────────


class TestExtractGidFromHref:
    @pytest.mark.parametrize(
        "href, expected",
        [
            ("127042011002", "127042011002"),
            ("/akt/127042011002", "127042011002"),
            ("akt/127042011002", "127042011002"),
            ("12846827", "12846827"),  # legacy 8-digit ID
            ("", None),
            ("https://example.com/", None),
        ],
    )
    def test_various_hrefs(self, href, expected):
        assert _extract_gid_from_href(href) == expected


class TestExtractEelmine:
    def test_real_constitution_page(self):
        html = """
        <p class="drop-button">
          <a class="drop-label" href="127042011002">Eelmine</a>
          <a class="drop-pages" href="#">...</a>
        </p>
        <p class="drop-button">
          <a class="drop-label" href="103072025001">Järgmine</a>
        </p>
        """
        assert extract_eelmine_gid(html) == "127042011002"
        assert extract_jargmine_gid(html) == "103072025001"

    def test_no_eelmine_returns_none(self):
        html = "<p>Some page without version navigation</p>"
        assert extract_eelmine_gid(html) is None
        assert extract_jargmine_gid(html) is None

    def test_only_eelmine_no_jargmine(self):
        html = '<a class="drop-label" href="999">Eelmine</a>'
        assert extract_eelmine_gid(html) == "999"
        assert extract_jargmine_gid(html) is None

    def test_absolute_href(self):
        html = '<a class="drop-label" href="/akt/12846827">Eelmine</a>'
        assert extract_eelmine_gid(html) == "12846827"


# ─────────────────────────────────────────────
# XML date extraction
# ─────────────────────────────────────────────


class TestExtractDatesFromXml:
    def test_minimal_xml(self):
        xml = b"""<?xml version="1.0"?>
        <oigusakt xmlns="test_1">
            <metaandmed>
                <kehtivus>
                    <kehtivuseAlgus>2015-08-13+03:00</kehtivuseAlgus>
                    <kehtivuseLopp>2025-07-08</kehtivuseLopp>
                </kehtivus>
            </metaandmed>
        </oigusakt>
        """
        start, end = extract_dates_from_xml(xml)
        assert start == date(2015, 8, 13)
        assert end == date(2025, 7, 8)

    def test_open_ended_version(self):
        xml = b"""<?xml version="1.0"?>
        <oigusakt xmlns="test_1">
            <metaandmed>
                <kehtivus>
                    <kehtivuseAlgus>2026-03-01</kehtivuseAlgus>
                </kehtivus>
            </metaandmed>
        </oigusakt>
        """
        start, end = extract_dates_from_xml(xml)
        assert start == date(2026, 3, 1)
        assert end is None


# ─────────────────────────────────────────────
# Chain walkers (mocked client)
# ─────────────────────────────────────────────


def _fake_client(pages: dict[str, tuple[bytes, str]]) -> MagicMock:
    """Build a mock RTClient from a dict {gid: (xml_bytes, html_text)}."""
    client = MagicMock()
    client.get_text.side_effect = lambda gid: pages[gid][0]
    client.get_html.side_effect = lambda gid: pages[gid][1].encode("utf-8")
    return client


def _xml(start: str, end: str | None) -> bytes:
    lopp = f"<kehtivuseLopp>{end}</kehtivuseLopp>" if end else ""
    return f"""<?xml version="1.0"?>
    <oigusakt xmlns="test_1">
        <metaandmed>
            <kehtivus>
                <kehtivuseAlgus>{start}</kehtivuseAlgus>
                {lopp}
            </kehtivus>
        </metaandmed>
    </oigusakt>
    """.encode()


def _html(eelmine: str | None = None, jargmine: str | None = None) -> str:
    parts = []
    if eelmine:
        parts.append(f'<a class="drop-label" href="{eelmine}">Eelmine</a>')
    if jargmine:
        parts.append(f'<a class="drop-label" href="{jargmine}">Järgmine</a>')
    return "<p>" + "".join(parts) + "</p>"


class TestFollowChainBackwards:
    def test_three_version_chain(self):
        pages = {
            "100000003": (
                _xml("2015-08-13", "2025-07-08"),
                _html(eelmine="100000002", jargmine=None),
            ),
            "100000002": (
                _xml("2011-07-22", "2015-08-12"),
                _html(eelmine="100000001", jargmine="100000003"),
            ),
            "100000001": (
                _xml("2007-07-21", "2011-07-21"),
                _html(eelmine=None, jargmine="100000002"),
            ),
        }
        client = _fake_client(pages)
        chain = follow_chain_backwards(client, "100000003")
        assert [v.global_id for v in chain] == ["100000003", "100000002", "100000001"]
        assert chain[0].effective_from == date(2015, 8, 13)
        assert chain[2].effective_until == date(2011, 7, 21)

    def test_single_version_chain(self):
        pages = {
            "only": (_xml("2020-01-01", None), _html()),
        }
        client = _fake_client(pages)
        chain = follow_chain_backwards(client, "only")
        assert len(chain) == 1
        assert chain[0].global_id == "only"

    def test_cycle_detection(self):
        pages = {
            "2000": (_xml("2020-01-01", "2020-12-31"), _html(eelmine="2001")),
            "2001": (_xml("2019-01-01", "2019-12-31"), _html(eelmine="2000")),  # cycle!
        }
        client = _fake_client(pages)
        chain = follow_chain_backwards(client, "2000", max_depth=10)
        # Should stop after detecting the cycle — not loop forever
        assert len(chain) == 2
        assert [v.global_id for v in chain] == ["2000", "2001"]


class TestFollowChainForwards:
    def test_forward_chain_excludes_start(self):
        pages = {
            "100000001": (_xml("2007-07-21", "2011-07-21"), _html(jargmine="100000002")),
            "100000002": (_xml("2011-07-22", "2015-08-12"), _html(jargmine="100000003")),
            "100000003": (_xml("2015-08-13", None), _html()),
        }
        client = _fake_client(pages)
        chain = follow_chain_forwards(client, "100000001")
        # start_gid is NOT included; only newer versions
        assert [v.global_id for v in chain] == ["100000002", "100000003"]


class TestFullHistory:
    def test_from_middle_version_reconstructs_all(self):
        pages = {
            "100000001": (_xml("2007-07-21", "2011-07-21"), _html(jargmine="100000002")),
            "100000002": (
                _xml("2011-07-22", "2015-08-12"),
                _html(eelmine="100000001", jargmine="100000003"),
            ),
            "100000003": (_xml("2015-08-13", None), _html(eelmine="100000002")),
        }
        client = _fake_client(pages)
        hist = full_history(client, "100000002")  # start from middle
        assert [v.global_id for v in hist] == ["100000001", "100000002", "100000003"]
        # Ordered oldest first
        assert hist[0].effective_from == date(2007, 7, 21)
        assert hist[-1].effective_from == date(2015, 8, 13)


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────


class TestCanonicalFilename:
    def test_picks_earliest(self):
        hist = [
            HistoricalVersion("1000_oldest", date(2007, 7, 21), date(2011, 7, 21)),
            HistoricalVersion("100000002", date(2011, 7, 22), date(2015, 8, 12)),
            HistoricalVersion("1003_current", date(2015, 8, 13), None),
        ]
        assert canonical_filename_id(hist) == "1000_oldest"

    def test_empty_returns_none(self):
        assert canonical_filename_id([]) is None


class TestValidateChainContiguity:
    def test_perfect_chain_no_warnings(self):
        hist = [
            HistoricalVersion("100000001", date(2007, 7, 21), date(2011, 7, 21)),
            HistoricalVersion("100000002", date(2011, 7, 22), date(2015, 8, 12)),
            HistoricalVersion("100000003", date(2015, 8, 13), None),
        ]
        assert validate_chain_contiguity(hist) == []

    def test_gap_is_detected(self):
        hist = [
            HistoricalVersion("100000001", date(2007, 1, 1), date(2010, 12, 31)),
            HistoricalVersion("100000002", date(2015, 1, 1), None),  # 4-year gap
        ]
        warnings = validate_chain_contiguity(hist)
        assert len(warnings) == 1
        assert "gap" in warnings[0]

    def test_overlap_is_detected(self):
        hist = [
            HistoricalVersion("100000001", date(2007, 1, 1), date(2015, 12, 31)),
            HistoricalVersion("100000002", date(2015, 1, 1), None),  # overlaps v1
        ]
        warnings = validate_chain_contiguity(hist)
        assert len(warnings) == 1
        assert "overlap" in warnings[0]
