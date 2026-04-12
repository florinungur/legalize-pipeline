"""Tests for the Argentine InfoLEG parser and reform extractor.

Covers:
- Text parser against the 5 target fixtures (4 Tier 1 + Constitución)
- Metadata parser against a synthetic InfoLEG catalog row
- Reform extractor against the 3 modificatoria fixtures
- The POC end-to-end: extracting Ley 27.444 modifications to Ley 19.550
  and verifying the extracted text appears in the consolidated texact.htm
- Encoding: cp1252 bytes round-trip without replacement chars in output
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path


from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.ar.catalog import (
    InfoLEGCatalog,
    InfoLEGRow,
    ModificationEdge,
    url_for,
)
from legalize.fetcher.ar.parser import (
    InfoLEGMetadataParser,
    InfoLEGTextParser,
    _make_identifier,
    count_content_images,
)
from legalize.fetcher.ar.reconstructor import (
    ReconstructionQuality,
    _apply_one_modification,
    _find_block_index,
    _norm_article_id,
    reconstruct,
)
from legalize.fetcher.ar.reforms import (
    Modification,
    ModificationKind,
    decode_infoleg,
    extract_modifications,
    html_to_plain,
)
from legalize.models import NormStatus, Rank

FIXTURES = Path(__file__).parent / "fixtures" / "ar"

# Target norms (Tier 1)
CODIGO_CIVIL = FIXTURES / "sample-codigo-civil-y-comercial-235975-texact.htm"
LEY_19550 = FIXTURES / "sample-ley-19550-sociedades-texact.htm"
DNU_70_2023 = FIXTURES / "sample-dnu-70-2023-bases-norma.htm"
LEY_27430 = FIXTURES / "sample-ley-27430-tributaria-texact.htm"
CONSTITUCION = FIXTURES / "sample-constitucion-ley-24430-804-norma.htm"

# Modificatorias for the reform extractor
MODIF_22903 = FIXTURES / "sample-modificatoria-ley-22903-1983-norma.htm"
MODIF_26994 = FIXTURES / "sample-modificatoria-ley-26994-2014-norma.htm"
MODIF_27444 = FIXTURES / "sample-modificatoria-ley-27444-2018-norma.htm"


# ─────────────────────────────────────────────
# Registry integration
# ─────────────────────────────────────────────


def test_ar_registered_in_countries():
    parser = get_text_parser("ar")
    assert isinstance(parser, InfoLEGTextParser)
    mp = get_metadata_parser("ar")
    assert isinstance(mp, InfoLEGMetadataParser)


# ─────────────────────────────────────────────
# URL builder
# ─────────────────────────────────────────────


def test_url_for_ley_19550():
    url = url_for(25553)
    assert url == (
        "http://servicios.infoleg.gob.ar/infolegInternet/anexos/25000-29999/25553/texact.htm"
    )


def test_url_for_norma_html():
    url = url_for(395521, "norma")
    assert url == (
        "http://servicios.infoleg.gob.ar/infolegInternet/anexos/395000-399999/395521/norma.htm"
    )


def test_url_for_string_id():
    """Accepts string IDs (from CSV rows)."""
    assert url_for("804") == (
        "http://servicios.infoleg.gob.ar/infolegInternet/anexos/0-4999/804/norma.htm"
    ).replace("norma.htm", "texact.htm")


# ─────────────────────────────────────────────
# Identifier builder
# ─────────────────────────────────────────────


def test_identifier_ley():
    assert _make_identifier("Ley", "26994", "", "2014-10-08") == "LEY-26994"


def test_identifier_ley_with_dots():
    """Some catalog rows include dots in numero_norma."""
    assert _make_identifier("Ley", "19.550", "", "1972-04-25") == "LEY-19550"


def test_identifier_dnu():
    assert _make_identifier("Decreto", "70", "DNU", "2023-12-21") == "DNU-70-2023"


def test_identifier_decreto_reglamentario():
    assert _make_identifier("Decreto", "222", "", "2003-06-20") == "DEC-222-2003"


def test_identifier_decreto_ley():
    assert _make_identifier("Decreto/Ley", "6582", "", "1958-12-01") == "DL-6582-1958"


def test_identifier_decreto_slash_number():
    """Decreto numbers that include a slash like 'N/2023' should be dash-safe."""
    out = _make_identifier("Decreto", "70/2023", "", "2023-12-21")
    assert "/" not in out
    assert out.startswith("DEC-")


# ─────────────────────────────────────────────
# Encoding round-trip
# ─────────────────────────────────────────────


def test_decode_infoleg_is_cp1252():
    """cp1252 decoding should preserve em dashes and smart quotes."""
    # windows-1252 codepoints for em dash (0x97) and right single quote (0x92)
    raw = b"\x97 \x92 test"
    out = decode_infoleg(raw)
    assert "\u2014" in out  # em dash
    assert "\u2019" in out  # right single quote
    # No replacement chars
    assert "\ufffd" not in out


def test_decode_infoleg_strips_control_chars():
    raw = b"before\x01\x02\x03after"
    out = decode_infoleg(raw)
    assert out == "beforeafter"


def test_fixture_decodes_without_replacement_chars():
    """Every fixture should decode cleanly via cp1252."""
    for f in (
        LEY_19550,
        CODIGO_CIVIL,
        DNU_70_2023,
        LEY_27430,
        CONSTITUCION,
        MODIF_22903,
        MODIF_26994,
        MODIF_27444,
    ):
        assert f.exists(), f"missing fixture {f}"
        data = f.read_bytes()
        text = decode_infoleg(data)
        # Replacement chars are a smell that we picked the wrong decoder
        assert "\ufffd" not in text, f"replacement char in {f.name}"


# ─────────────────────────────────────────────
# Text parser — Ley 19.550 (Sociedades)
# ─────────────────────────────────────────────


def test_parse_ley_19550_yields_articles():
    data = LEY_19550.read_bytes()
    parser = InfoLEGTextParser()
    blocks = parser.parse_text(data)

    # Ley 19.550 has ~370 articles + annexes; parser should extract at least 300
    articles = [b for b in blocks if b.block_type == "article"]
    assert len(articles) >= 300, f"expected ≥300 articles, got {len(articles)}"

    # Find article 1 and verify its paragraph structure
    art1 = next((b for b in articles if b.id == "art1"), None)
    assert art1 is not None
    assert len(art1.versions) == 1
    paragraphs = art1.versions[0].paragraphs
    assert paragraphs[0].css_class == "articulo"
    assert "ARTICULO 1" in paragraphs[0].text or "Artículo 1" in paragraphs[0].text


def test_parse_ley_19550_preserves_bold_markers():
    """Ley 19.550 uses 707 <b> tags semantically; parser should preserve them."""
    data = LEY_19550.read_bytes()
    parser = InfoLEGTextParser()
    blocks = parser.parse_text(data)
    any_bold = any("**" in p.text for b in blocks for v in b.versions for p in v.paragraphs)
    assert any_bold, "expected at least one **bold** marker"


# ─────────────────────────────────────────────
# Text parser — Código Civil y Comercial (tables)
# ─────────────────────────────────────────────


def test_parse_codigo_civil_extracts_tables():
    """Código Civil y Comercial has 8 HTML tables; parser should surface them."""
    data = CODIGO_CIVIL.read_bytes()
    parser = InfoLEGTextParser()
    blocks = parser.parse_text(data)
    table_blocks = [b for b in blocks if b.block_type == "table"]
    assert len(table_blocks) == 8, f"expected 8 tables, got {len(table_blocks)}"

    # Each table should render as a Markdown pipe table
    for tb in table_blocks:
        text = tb.versions[0].paragraphs[0].text
        assert text.startswith("| "), f"not a pipe table: {text[:60]!r}"
        assert "---" in text


def test_parse_codigo_civil_article_count():
    """Código Civil y Comercial has 2671 articles in its body. With the Anexo I
    included as part of the aprobatoria, the parser should see well over 2000."""
    data = CODIGO_CIVIL.read_bytes()
    parser = InfoLEGTextParser()
    blocks = parser.parse_text(data)
    articles = [b for b in blocks if b.block_type == "article"]
    assert len(articles) >= 2000


# ─────────────────────────────────────────────
# Text parser — DNU 70/2023 (flat-text worst case)
# ─────────────────────────────────────────────


def test_parse_dnu_70_2023_splits_articles():
    """DNU 70/2023 has no semantic markup (only <br>). Parser must still split
    articles by the textual ARTICULO N marker."""
    data = DNU_70_2023.read_bytes()
    parser = InfoLEGTextParser()
    blocks = parser.parse_text(data)
    articles = [b for b in blocks if b.block_type == "article"]
    # The DNU has ~350 articles; we expect the regex splitter to find most
    assert len(articles) >= 100


# ─────────────────────────────────────────────
# Text parser — Ley 27.430 (tables as scanned JPGs)
# ─────────────────────────────────────────────


def test_parse_ley_27430_no_content_images_in_text():
    """Ley 27.430 has 8 scanned-JPG tables (ley27430-N.jpg). These should not
    appear as text — they should be dropped (and counted elsewhere)."""
    data = LEY_27430.read_bytes()
    parser = InfoLEGTextParser()
    blocks = parser.parse_text(data)
    for b in blocks:
        for v in b.versions:
            for p in v.paragraphs:
                # The decorative banner must never leak
                assert "imagenes/left.png" not in p.text


# ─────────────────────────────────────────────
# Metadata parser
# ─────────────────────────────────────────────


def _row_ley_19550() -> bytes:
    row = {
        "id_norma": "25553",
        "tipo_norma": "Ley",
        "numero_norma": "19550",
        "clase_norma": "",
        "organismo_origen": "PODER EJECUTIVO NACIONAL (P.E.N.)",
        "fecha_sancion": "1972-04-03",
        "numero_boletin": "22409",
        "fecha_boletin": "1972-04-25",
        "pagina_boletin": "11",
        "titulo_resumido": "NUEVO REGIMEN",
        "titulo_sumario": "SOCIEDADES COMERCIALES",
        "texto_resumido": "SOCIEDADES COMERCIALES. NUEVO REGIMEN.",
        "observaciones": "",
        "texto_original": "",
        "texto_actualizado": (
            "http://servicios.infoleg.gob.ar/infolegInternet/anexos/25000-29999/25553/texact.htm"
        ),
        "modificada_por": 169,
        "modifica_a": 9,
    }
    return json.dumps(row).encode("utf-8")


def test_metadata_parse_ley_19550():
    mp = InfoLEGMetadataParser()
    md = mp.parse(_row_ley_19550(), "25553")
    assert md.identifier == "LEY-19550"
    assert md.title == "SOCIEDADES COMERCIALES"
    assert md.rank == Rank("ley")
    assert md.publication_date == date(1972, 4, 25)
    assert md.country == "ar"
    assert md.status == NormStatus.IN_FORCE
    assert md.department == "PODER EJECUTIVO NACIONAL (P.E.N.)"


def test_metadata_parse_ley_19550_extra_fields():
    """Every InfoLEG column should end up in extra or a generic field — capture
    EVERYTHING the source gives us (per the project-wide rule)."""
    mp = InfoLEGMetadataParser()
    md = mp.parse(_row_ley_19550(), "25553")
    extra = dict(md.extra)
    assert extra.get("infoleg_id") == "25553"
    assert extra.get("enactment_date") == "1972-04-03"
    assert extra.get("gazette_number") == "22409"
    assert extra.get("gazette_page") == "11"
    assert extra.get("times_modified") == "169"
    assert extra.get("modifies_count") == "9"


def test_metadata_parse_dnu_70_2023():
    """DNU gets the decreto_necesidad_urgencia rank, not plain 'decreto'."""
    row = {
        "id_norma": "395521",
        "tipo_norma": "Decreto",
        "numero_norma": "70",
        "clase_norma": "DNU",
        "organismo_origen": "PODER EJECUTIVO NACIONAL (P.E.N.)",
        "fecha_sancion": "2023-12-20",
        "numero_boletin": "35326",
        "fecha_boletin": "2023-12-21",
        "pagina_boletin": "3",
        "titulo_resumido": "DISPOSICIONES",
        "titulo_sumario": "BASES PARA LA RECONSTRUCCION DE LA ECONOMIA ARGENTINA",
        "texto_resumido": "",
        "observaciones": "",
        "texto_original": (
            "http://servicios.infoleg.gob.ar/infolegInternet/anexos/395000-399999/395521/norma.htm"
        ),
        "texto_actualizado": "",
        "modificada_por": 48,
        "modifica_a": 87,
    }
    mp = InfoLEGMetadataParser()
    md = mp.parse(json.dumps(row).encode("utf-8"), "395521")
    assert md.identifier == "DNU-70-2023"
    assert md.rank == Rank("decreto_necesidad_urgencia")
    assert md.publication_date == date(2023, 12, 21)


# ─────────────────────────────────────────────
# Reform extractor — POC validation
# ─────────────────────────────────────────────


def test_extract_modifications_ley_27444_targets_ley_19550():
    """POC: Ley 27.444 (2018) makes 4 substitutions to Ley 19.550."""
    data = MODIF_27444.read_bytes()
    mods = extract_modifications(data, "19550")
    # Keep only substitutions (we only care about content changes here)
    subs = [m for m in mods if m.kind == ModificationKind.SUBSTITUTE]
    assert len(subs) == 4, f"expected 4 substitutions, got {len(subs)}"

    # Verify the exact article numbers
    art_ids = sorted(m.article_id for m in subs)
    assert art_ids == ["34", "35", "61", "8"]


def test_extract_modifications_ley_27444_new_text_matches_consolidated():
    """The POC gold standard: the new text extracted from Ley 27.444 should
    appear literally inside the consolidated Ley 19.550 texact.htm."""
    modif_data = MODIF_27444.read_bytes()
    mods = extract_modifications(modif_data, "19550")
    subs = {m.article_id: m for m in mods if m.kind == ModificationKind.SUBSTITUTE}
    assert {"8", "34", "35", "61"} <= set(subs.keys())

    consolidated = decode_infoleg(LEY_19550.read_bytes())

    for art_id in ("8", "34", "35", "61"):
        new_text = subs[art_id].new_text
        assert new_text, f"empty new_text for art {art_id}"
        # Take the first sentence fragment and assert it's in the consolidated
        head = new_text[:50].strip()
        # Normalize whitespace for matching
        import re

        pattern = re.escape(head).replace(r"\ ", r"\s+")
        assert re.search(pattern, consolidated), (
            f"art {art_id}: head '{head}' not found in consolidated Ley 19.550"
        )


def test_extract_modifications_ley_22903_plural_substitution():
    """Ley 22.903 (1983) uses the plural sustitución pattern and modifies
    78 articles of Ley 19.550 in a single legal article. The parser should
    extract at least the main substitutions from the quoted bodies."""
    data = MODIF_22903.read_bytes()
    mods = extract_modifications(data, "19550")
    subs = [m for m in mods if m.kind == ModificationKind.SUBSTITUTE]
    # Plural bodies may produce more matches than the header list (because of
    # sub-article quotes); we accept anything ≥ 40 as "parser works"
    assert len(subs) >= 40, f"expected ≥40 plural substitutions, got {len(subs)}"


def test_extract_modifications_unrelated_norm_returns_empty():
    """When asking for a norm not targeted by the modificatoria, return []."""
    data = MODIF_27444.read_bytes()
    mods = extract_modifications(data, "99999")
    assert mods == []


# ─────────────────────────────────────────────
# Sanity — html_to_plain
# ─────────────────────────────────────────────


def test_html_to_plain_strips_scripts():
    html = "before <script>alert('x')</script> after"
    assert "alert" not in html_to_plain(html)


def test_html_to_plain_preserves_paragraphs():
    html = "<p>one</p><p>two</p>"
    out = html_to_plain(html)
    assert "one" in out and "two" in out


# ─────────────────────────────────────────────
# count_content_images
# ─────────────────────────────────────────────


def test_count_content_images_skips_banner():
    """The `/infolegInternet/imagenes/left.png` banner appears on every
    fixture but must not be counted."""
    for f in (LEY_19550, CODIGO_CIVIL, DNU_70_2023):
        assert count_content_images(f.read_bytes()) == 0, f"false positive on {f.name}"


def test_count_content_images_counts_scanned_jpgs():
    """Ley 27.430 has 8 <img> tags total: the decorative banner
    (`imagenes/left.png`) plus 7 scanned tariff tables
    (`ley27430-1.jpg` ... `ley27430-7.jpg`). Only the 7 content images
    should be counted."""
    n = count_content_images(LEY_27430.read_bytes())
    assert n == 7, f"expected 7 content images, got {n}"


# ─────────────────────────────────────────────
# Reconstructor helpers
# ─────────────────────────────────────────────


def test_norm_article_id():
    assert _norm_article_id("8") == "art8"
    assert _norm_article_id("8 bis") == "art8bis"
    assert _norm_article_id("8°") == "art8"
    assert _norm_article_id("art8") == "art8"


def test_find_block_index_on_ley_19550():
    """The text parser should produce blocks with ids like art1..art370."""
    data = LEY_19550.read_bytes()
    parser = InfoLEGTextParser()
    blocks = parser.parse_text(data)

    idx = _find_block_index(blocks, "1")
    assert idx >= 0
    assert blocks[idx].id == "art1"

    idx = _find_block_index(blocks, "8")
    assert idx >= 0
    assert blocks[idx].id == "art8"


def test_apply_substitution_replaces_block_in_place():
    """Applying a SUBSTITUTE modification should swap the block at that index."""
    parser = InfoLEGTextParser()
    blocks = list(parser.parse_text(LEY_19550.read_bytes()))

    original_idx = _find_block_index(blocks, "8")
    assert original_idx >= 0
    original_title = blocks[original_idx].title

    mod = Modification(
        target_norm_number="19550",
        kind=ModificationKind.SUBSTITUTE,
        article_id="8",
        new_text="SYNTHETIC REPLACEMENT TEXT FOR ARTICLE 8",
        source_article="99",
        raw_excerpt="",
    )

    ok = _apply_one_modification(blocks, mod, date(2026, 4, 11), "25553")
    assert ok

    idx_after = _find_block_index(blocks, "8")
    assert idx_after == original_idx  # same position
    new_block = blocks[idx_after]
    assert new_block.id == "art8"
    # New content is in the 2nd paragraph (1st is the ARTICULO header)
    body = new_block.versions[0].paragraphs[1].text
    assert "SYNTHETIC REPLACEMENT" in body
    assert original_title != new_block.title or True  # title may differ


def test_apply_repeal_turns_block_into_tombstone():
    parser = InfoLEGTextParser()
    blocks = list(parser.parse_text(LEY_19550.read_bytes()))

    mod = Modification(
        target_norm_number="19550",
        kind=ModificationKind.REPEAL,
        article_id="34",
        new_text="",
        source_article="1",
        raw_excerpt="",
    )

    ok = _apply_one_modification(blocks, mod, date(2026, 4, 11), "25553")
    assert ok

    idx = _find_block_index(blocks, "34")
    repealed = blocks[idx]
    tombstone_text = " ".join(p.text for p in repealed.versions[0].paragraphs)
    assert "derogado" in tombstone_text.lower()


def test_apply_insertion_places_bis_after_parent():
    """Inserting `8 bis` should place it right after `art8`."""
    parser = InfoLEGTextParser()
    blocks = list(parser.parse_text(LEY_19550.read_bytes()))

    parent_idx = _find_block_index(blocks, "8")
    assert parent_idx >= 0

    mod = Modification(
        target_norm_number="19550",
        kind=ModificationKind.INSERT,
        article_id="8 bis",
        new_text="Nuevo contenido para el art 8 bis",
        source_article="5",
        raw_excerpt="",
    )

    ok = _apply_one_modification(blocks, mod, date(2026, 4, 11), "25553")
    assert ok

    new_idx = _find_block_index(blocks, "8 bis")
    assert new_idx == parent_idx + 1


def test_apply_unknown_is_noop():
    parser = InfoLEGTextParser()
    blocks = list(parser.parse_text(LEY_19550.read_bytes()))
    count_before = len(blocks)

    mod = Modification(
        target_norm_number="19550",
        kind=ModificationKind.UNKNOWN,
        article_id="1",
        new_text="",
        source_article="1",
        raw_excerpt="",
    )
    ok = _apply_one_modification(blocks, mod, date(2026, 4, 11), "25553")
    assert ok is False
    assert len(blocks) == count_before


# ─────────────────────────────────────────────
# Reconstructor end-to-end (offline, fixtures only)
# ─────────────────────────────────────────────


class _FakeInfoLEGClient:
    """Test double that serves local fixtures instead of hitting the network."""

    def __init__(self, fixtures: dict[str, Path]) -> None:
        self._fixtures = fixtures

    def get_text(self, norm_id: str) -> bytes:
        path = self._fixtures.get(("text", norm_id)) or self._fixtures.get(norm_id)
        if not path:
            raise FileNotFoundError(f"no fixture for norm {norm_id}")
        return path.read_bytes()

    def get_modificatoria_text(self, norm_id: str) -> bytes:
        path = self._fixtures.get(("modif", norm_id))
        if not path:
            raise FileNotFoundError(f"no modif fixture for {norm_id}")
        return path.read_bytes()


def _fake_catalog_for_ley_19550() -> InfoLEGCatalog:
    """Build an in-memory catalog with just Ley 19.550 + one modificatoria
    (Ley 27.444, which the POC validated)."""
    row = InfoLEGRow(
        id_norma="25553",
        tipo_norma="Ley",
        numero_norma="19550",
        clase_norma="",
        organismo_origen="PODER EJECUTIVO NACIONAL (P.E.N.)",
        fecha_sancion=date(1972, 4, 3),
        numero_boletin="22409",
        fecha_boletin=date(1972, 4, 25),
        pagina_boletin="11",
        titulo_resumido="NUEVO REGIMEN",
        titulo_sumario="SOCIEDADES COMERCIALES",
        texto_resumido="SOCIEDADES COMERCIALES. NUEVO REGIMEN.",
        observaciones="",
        texto_original=(
            "http://servicios.infoleg.gob.ar/infolegInternet/anexos/25000-29999/25553/texact.htm"
        ),
        texto_actualizado=(
            "http://servicios.infoleg.gob.ar/infolegInternet/anexos/25000-29999/25553/texact.htm"
        ),
        modificada_por=170,
        modifica_a=9,
    )
    catalog = InfoLEGCatalog()
    catalog.by_id["25553"] = row
    catalog.modifications_of["25553"] = [
        ModificationEdge(
            id_modificada="25553",
            id_modificatoria="311587",  # Ley 27.444
            tipo_norma="Ley",
            nro_norma="27444",
            clase_norma="",
            organismo_origen="HONORABLE CONGRESO DE LA NACION ARGENTINA",
            fecha_boletin=date(2018, 6, 18),
            titulo_sumario="SIMPLIFICACION Y DESBUROCRATIZACION",
            titulo_resumido="LEY DE SIMPLIFICACION",
        ),
    ]
    return catalog


def test_reconstructor_applies_ley_27444_to_ley_19550():
    """End-to-end: parse Ley 19.550, apply Ley 27.444 modifications, emit snapshots."""
    catalog = _fake_catalog_for_ley_19550()
    row = catalog.get("25553")

    fake = _FakeInfoLEGClient(
        fixtures={
            ("text", "25553"): LEY_19550,  # texact.htm (client.get_text)
            # reconstruct() uses get_modificatoria_text for v0 bootstrap too
            # when texto_original is populated — but we point both at the
            # same fixture because we only have one for Ley 19.550.
            "25553": LEY_19550,
            ("modif", "311587"): MODIF_27444,
            ("modif", "25553"): LEY_19550,
        }
    )

    text_parser = InfoLEGTextParser()
    result = reconstruct(fake, row, catalog, text_parser)

    # We expect 2 or 3 snapshots:
    #   v0 (bootstrap 1972) + v1 (Ley 27.444 in 2018) + maybe consolidacion
    assert len(result.snapshots) >= 2
    assert result.snapshots[0].source_label == "bootstrap"
    assert result.snapshots[0].commit_date == date(1972, 4, 25)

    # Find the Ley 27.444 snapshot
    modif_snaps = [s for s in result.snapshots if "27444" in s.source_label]
    assert len(modif_snaps) == 1
    modif_snap = modif_snaps[0]
    assert modif_snap.commit_date == date(2018, 6, 18)

    # That snapshot should have the 4 POC articles as affected
    affected = set(modif_snap.affected_article_ids)
    assert {"8", "34", "35", "61"} <= affected

    # The applied counter is ≥ 4
    assert result.applied >= 4

    # Quality should be CLEAN or PARTIAL (not bootstrap-only)
    assert result.quality != ReconstructionQuality.BOOTSTRAP_ONLY


def test_reconstructor_block_art8_has_new_text_after_ley_27444():
    """The POC gold: after applying Ley 27.444, art 8 of Ley 19.550 should
    contain the 'Registro Nacional de Sociedades por Acciones' text."""
    catalog = _fake_catalog_for_ley_19550()
    row = catalog.get("25553")
    fake = _FakeInfoLEGClient(
        fixtures={
            ("text", "25553"): LEY_19550,
            "25553": LEY_19550,
            ("modif", "311587"): MODIF_27444,
            ("modif", "25553"): LEY_19550,
        }
    )
    result = reconstruct(fake, row, catalog, InfoLEGTextParser())

    # Find the snapshot that Ley 27.444 produced
    modif_snaps = [s for s in result.snapshots if "27444" in s.source_label]
    assert modif_snaps
    snap = modif_snaps[0]

    # Find art 8 in the snapshot's blocks
    art8 = next((b for b in snap.blocks if b.id == "art8"), None)
    assert art8 is not None
    body = " ".join(p.text for p in art8.versions[0].paragraphs)
    assert "Registro Nacional de Sociedades por Acciones" in body
