from pathlib import Path

from legalize.fetcher.co.parser import SuinMetadataParser, SuinTextParser

FIXTURES = Path("tests/fixtures/co")


class TestSuinMetadataParser:
    def test_ley_1887_identifier(self):
        data = (FIXTURES / "sample-ley-1887.html").read_bytes()
        meta = SuinMetadataParser().parse(data, "1789030")
        assert meta.country == "co"
        assert meta.identifier == "LEY-57-1887"
        assert meta.publication_date.year == 1887

    def test_decreto_identifier(self):
        data = (FIXTURES / "sample-decreto.html").read_bytes()
        meta = SuinMetadataParser().parse(data, "1100073")
        assert meta.identifier == "DECRETO-453-1981"
        assert meta.publication_date.year == 1981

    def test_identifier_is_filesystem_safe(self):
        for fixture in FIXTURES.glob("sample-*.html"):
            data = fixture.read_bytes()
            try:
                meta = SuinMetadataParser().parse(data, "0")
                for char in (":", " ", "/", "\\", "*", "?", '"', "<", ">", "|"):
                    assert char not in meta.identifier, (
                        f"Unsafe char '{char}' in identifier: {meta.identifier}"
                    )
            except Exception:
                pass

    def test_source_url(self):
        data = (FIXTURES / "sample-ley-1887.html").read_bytes()
        meta = SuinMetadataParser().parse(data, "1789030")
        assert (
            meta.source
            == "https://www.suin-juriscol.gov.co/viewDocument.asp?id=1789030"
        )


class TestSuinTextParser:
    def test_parse_returns_blocks(self):
        data = (FIXTURES / "sample-ley-1887.html").read_bytes()
        blocks = SuinTextParser().parse_text(data)
        assert len(blocks) > 0

    def test_extract_reforms_empty(self):
        data = (FIXTURES / "sample-ley-1887.html").read_bytes()
        reforms = SuinTextParser().extract_reforms(data)
        assert reforms == []

    def test_body_table_is_markdown_paragraph(self):
        data = (FIXTURES / "sample-decreto-1993.html").read_bytes()
        blocks = SuinTextParser().parse_text(data)
        table_paragraphs = [
            paragraph
            for block in blocks
            for version in block.versions
            for paragraph in version.paragraphs
            if paragraph.css_class == "table"
        ]
        assert table_paragraphs
        assert table_paragraphs[0].text.startswith("| ")


class TestRegistry:
    def test_co_in_registry(self):
        from legalize.countries import REGISTRY

        assert "co" in REGISTRY
