"""Parser for Estonian Riigi Teataja XML documents.

Riigi Teataja publishes laws as XML using one of several namespaces depending
on the document type:
  - tyviseadus_1_10.02.2010 — consolidated base law (terviktekst)
  - muutmisseadus_1_10.02.2010 — amending law (algtekst)
  - maarus_1_10.02.2010 — regulation
  - muutmismaarus_1_10.02.2010 — amending regulation

Because the namespace varies, all xpath/find operations are namespace-agnostic
(we match on the local-name only).

Hierarchy mapping (Estonian → block_type → markdown):
  osa        → part      → ## (titulo_tit)
  peatykk    → chapter   → ### (capitulo_tit)
  jagu       → division  → #### (seccion)
  jaotis     → subdivision → #### (seccion)
  paragrahv  → article   → ##### (articulo)
  loige      → (paragraph inside article)
  punkt      → (sub-point)
  alampunkt  → (sub-sub-point)
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from lxml import etree

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import (
    Block,
    NormMetadata,
    NormStatus,
    Paragraph,
    Rank,
    Version,
)

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

# Map dokumentLiik to a stable rank string
DOC_TYPE_TO_RANK: dict[str, str] = {
    "seadus": "seadus",
    "määrus": "maarus",
    "korraldus": "korraldus",
    "otsus": "otsus",
    "seadlus": "seadlus",
    "valisleping": "valisleping",
}

# Department fallback
_DEFAULT_DEPARTMENT = "Riigi Teataja"

# Shared lxml parser that can handle huge text nodes — Riigi Teataja has a
# handful of laws whose consolidated body contains a single text chunk of
# over 10 MB, which trips the default libxml2 limit.
_LXML_PARSER = etree.XMLParser(huge_tree=True)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


def _ln(el: etree._Element) -> str:
    """Local element name (strips XML namespace)."""
    return etree.QName(el.tag).localname


def _findone(el: etree._Element | None, local_name: str) -> etree._Element | None:
    """Find first descendant by local name (namespace-agnostic)."""
    if el is None:
        return None
    for child in el.iter():
        if _ln(child) == local_name:
            return child
    return None


def _direct_children(el: etree._Element | None, local_name: str) -> list[etree._Element]:
    """Find direct children only (not descendants), by local name."""
    if el is None:
        return []
    return [c for c in el if _ln(c) == local_name]


def _direct_child_text(el: etree._Element | None, local_name: str) -> str:
    """Get the text of the first direct child with the given local name."""
    if el is None:
        return ""
    for c in el:
        if _ln(c) == local_name:
            return (c.text or "").strip()
    return ""


def _parse_date(s: str | None) -> date | None:
    """Parse YYYY-MM-DD or DD.MM.YYYY date strings; tolerates timezone suffix."""
    if not s:
        return None
    # Strip timezone suffix like "+03:00" or "T00:00:00..."
    s = s.split("+")[0].split("T")[0].strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# Standalone date paragraphs that should be skipped from text body
_DATE_ONLY_RE = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$")


def _extract_inline(el: etree._Element) -> str:
    """Extract text from a tavatekst element, preserving inline formatting.

    Handles:
      <sup>X</sup>      → <sup>X</sup>   (HTML inline; GitHub renders it)
      <i>X</i>          → *X*
      <reavahetus/>     → newline
      <nbsp/>           → \u00a0
    All other tags pass through their text content.
    """
    parts: list[str] = []
    if el.text:
        parts.append(el.text)
    for child in el:
        ctag = _ln(child)
        if ctag == "sup":
            inner = _extract_inline(child)
            parts.append(f"<sup>{inner}</sup>")
        elif ctag == "i":
            inner = _extract_inline(child)
            parts.append(f"*{inner}*")
        elif ctag == "reavahetus":
            parts.append("\n")
        elif ctag == "nbsp":
            parts.append("\u00a0")
        else:
            parts.append(_extract_inline(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def _kuvatav(el: etree._Element | None) -> str:
    """Get the displayed label of an element from <kuvatavNr>.

    Example: <kuvatavNr><![CDATA[§ 1.]]></kuvatavNr> → "§ 1."
    Returns empty string if missing.
    """
    if el is None:
        return ""
    kuv = _findone(el, "kuvatavNr")
    if kuv is None:
        return ""
    return (kuv.text or "").strip()


def _collapse_ws(s: str) -> str:
    """Collapse runs of whitespace, preserving newlines as paragraph breaks."""
    # Split into lines, collapse intra-line whitespace, drop empty lines
    lines = []
    for line in s.split("\n"):
        line = re.sub(r"[ \t\r\f\v]+", " ", line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


_LG_ID_RE = re.compile(r"lg(\d+(?:\^?\d+)?)$")


def _format_loige_label(loige: etree._Element, fallback_index: int) -> str:
    """Format the prefix for a loige body, like '(1) '.

    Resolution order:
      1. <loigeNr>N</loigeNr> child
      2. id attribute matching ``...lgN``
      3. fallback_index (1-based position within the parent)
    """
    nr_el = _findone(loige, "loigeNr")
    if nr_el is not None and (nr_el.text or "").strip():
        return f"({(nr_el.text or '').strip()}) "
    lg_id = loige.get("id", "")
    m = _LG_ID_RE.search(lg_id)
    if m:
        return f"({m.group(1)}) "
    return f"({fallback_index}) "


def _format_punkt_label(punkt: etree._Element) -> str:
    """Format the prefix for a punkt/alampunkt, like '1) ' from kuvatavNr."""
    kuv = _kuvatav(punkt)
    if kuv:
        # kuvatavNr usually contains the trailing space already, e.g. "1) "
        return kuv if kuv.endswith(" ") else f"{kuv} "
    # Fallback: try alampunktNr or punktNr
    for tag in ("alampunktNr", "punktNr"):
        nr_el = _findone(punkt, tag)
        if nr_el is not None and (nr_el.text or "").strip():
            ula = nr_el.get("ylaIndeks")
            n = (nr_el.text or "").strip()
            if ula:
                return f"{n}<sup>{ula}</sup>) "
            return f"{n}) "
    return ""


# ─────────────────────────────────────────────
# Text parser
# ─────────────────────────────────────────────


class RTTextParser(TextParser):
    """Parses Estonian Riigi Teataja XML into a list of Block objects.

    Each ``paragrahv`` becomes one Block. Structural headings (osa/peatykk/jagu)
    are emitted as their own structural Blocks so the markdown renderer prints
    them in the correct order alongside the article blocks.
    """

    def parse_text(self, data: bytes) -> list[Any]:
        root = etree.fromstring(data, parser=_LXML_PARSER)
        sisu = _findone(root, "sisu")
        if sisu is None:
            return []

        pub_date = self._publication_date(root) or date(1900, 1, 1)
        norm_id = _direct_child_text(_findone(root, "metaandmed"), "globaalID")

        blocks: list[Block] = []
        # Walk DIRECT children of <sisu> in document order so structural
        # headings interleave correctly with paragrahvs.
        self._walk(sisu, pub_date, norm_id, blocks)

        # <lisaViide> elements can appear either inside <sisu> (handled
        # above) or as direct children of <oigusakt> at the document root.
        # Pick up the root-level ones here.
        for child in root:
            if _ln(child) == "lisaViide":
                lb = self._lisa_block(child, pub_date, norm_id)
                if lb is not None:
                    blocks.append(lb)

        # Append signers as a final structural block, if present
        signers_block = self._signers_block(root, pub_date, norm_id)
        if signers_block is not None:
            blocks.append(signers_block)

        return blocks

    # --- structural walk ---

    def _walk(
        self,
        parent: etree._Element,
        pub_date: date,
        norm_id: str,
        blocks: list[Block],
    ) -> None:
        """Walk direct children of <parent> in order, emitting Blocks."""
        for el in parent:
            tag = _ln(el)

            if tag == "preambul":
                preamble_block = self._preamble_block(el, pub_date, norm_id)
                if preamble_block is not None:
                    blocks.append(preamble_block)

            elif tag == "osa":
                blocks.append(
                    self._heading_block(
                        el, pub_date, norm_id, css="titulo_tit", suffix="osa", id_prefix="osa"
                    )
                )
                self._walk(el, pub_date, norm_id, blocks)

            elif tag == "peatykk":
                blocks.append(
                    self._heading_block(
                        el,
                        pub_date,
                        norm_id,
                        css="capitulo_tit",
                        suffix="peatykk",
                        id_prefix="ptk",
                    )
                )
                self._walk(el, pub_date, norm_id, blocks)

            elif tag in ("jagu", "jaotis"):
                blocks.append(
                    self._heading_block(
                        el, pub_date, norm_id, css="seccion", suffix=tag, id_prefix=tag
                    )
                )
                self._walk(el, pub_date, norm_id, blocks)

            elif tag == "paragrahv":
                blocks.append(self._paragrahv_block(el, pub_date, norm_id))

            elif tag == "lisaViide":
                lisa_block = self._lisa_block(el, pub_date, norm_id)
                if lisa_block is not None:
                    blocks.append(lisa_block)

            elif tag == "sisuTekst":
                # Top-level free-form content. When it appears BEFORE any
                # structural element (peatykk/osa/jagu/paragrahv), it's
                # effectively the preamble of the law. Otherwise it's
                # generic content (typical in algtekst documents).
                paragraphs = self._sisutekst_to_paragraphs(el)
                if not paragraphs:
                    continue
                is_preamble = not any(
                    b.block_type in ("preamble", "osa", "ptk", "jagu", "jaotis", "article")
                    for b in blocks
                )
                blocks.append(
                    Block(
                        id=f"{norm_id}-{'preambul' if is_preamble else 'sisu'}"
                        if norm_id
                        else ("preambul" if is_preamble else "sisu"),
                        block_type="preamble" if is_preamble else "content",
                        title="",
                        versions=(
                            Version(
                                norm_id=norm_id,
                                publication_date=pub_date,
                                effective_date=pub_date,
                                paragraphs=tuple(paragraphs),
                            ),
                        ),
                    )
                )

    # --- block factories ---

    def _heading_block(
        self,
        el: etree._Element,
        pub_date: date,
        norm_id: str,
        *,
        css: str,
        suffix: str,
        id_prefix: str,
    ) -> Block:
        """Build a structural heading block (osa/peatykk/jagu/jaotis)."""
        kuv = _kuvatav(el)  # e.g. "I. peatükk"
        # Title elements vary by level — try each in turn
        title_el: etree._Element | None = None
        for tag_name in ("peatykkPealkiri", "osaPealkiri", "jaguPealkiri", "jaotisPealkiri"):
            found = _findone(el, tag_name)
            if found is not None:
                title_el = found
                break
        title = ""
        if title_el is not None:
            title = _collapse_ws(_extract_inline(title_el)).strip()

        if kuv and title:
            heading = f"{kuv} {title}"
        elif kuv:
            heading = kuv
        elif title:
            heading = title
        else:
            heading = suffix

        block_id = el.get("id") or f"{id_prefix}-{kuv or 'unknown'}"
        return Block(
            id=block_id,
            block_type=id_prefix,
            title=heading,
            versions=(
                Version(
                    norm_id=norm_id,
                    publication_date=pub_date,
                    effective_date=pub_date,
                    paragraphs=(Paragraph(css_class=css, text=heading),),
                ),
            ),
        )

    def _preamble_block(self, el: etree._Element, pub_date: date, norm_id: str) -> Block | None:
        paragraphs: list[Paragraph] = []
        for tt in el.iter():
            if _ln(tt) == "tavatekst":
                text = _collapse_ws(_extract_inline(tt))
                if text:
                    paragraphs.append(Paragraph(css_class="parrafo", text=text))
        if not paragraphs:
            return None
        return Block(
            id=f"{norm_id}-preambul" if norm_id else "preambul",
            block_type="preamble",
            title="",
            versions=(
                Version(
                    norm_id=norm_id,
                    publication_date=pub_date,
                    effective_date=pub_date,
                    paragraphs=tuple(paragraphs),
                ),
            ),
        )

    def _paragrahv_block(self, el: etree._Element, pub_date: date, norm_id: str) -> Block:
        """Build a Block for a single <paragrahv> (= one article §)."""
        kuv = _kuvatav(el)  # e.g. "§ 1."
        title_el = _findone(el, "paragrahvPealkiri")
        title_text = ""
        if title_el is not None:
            title_text = _collapse_ws(_extract_inline(title_el)).strip()

        if kuv and title_text:
            heading = f"{kuv} {title_text}"
        elif kuv:
            heading = kuv
        else:
            heading = title_text or "§"

        paragraphs: list[Paragraph] = [Paragraph(css_class="articulo", text=heading)]

        # Walk loige/punkt/alampunkt/sisuTekst children of <paragrahv>
        self._collect_body(el, paragraphs, top_level=True)

        block_id = el.get("id") or _direct_child_text(el, "paragrahvNr") or "para"
        return Block(
            id=block_id,
            block_type="article",
            title=heading,
            versions=(
                Version(
                    norm_id=norm_id,
                    publication_date=pub_date,
                    effective_date=pub_date,
                    paragraphs=tuple(paragraphs),
                ),
            ),
        )

    def _collect_body(
        self,
        el: etree._Element,
        paragraphs: list[Paragraph],
        *,
        top_level: bool,
    ) -> None:
        """Recursively collect body paragraphs from a paragrahv subtree.

        Walks direct children only and dispatches:
          - sisuTekst → emit text
          - loige     → recurse with "(N) " prefix on first text
          - punkt / alampunkt → recurse with "N) " prefix
          - muutmismarge → skip (only used for reform tracking, not body text)
        """
        loige_index = 0
        for child in el:
            tag = _ln(child)
            if tag == "sisuTekst":
                self._sisutekst_to_paragraphs_into(child, paragraphs)
            elif tag == "loige":
                loige_index += 1
                self._loige_into(child, paragraphs, loige_index)
            elif tag in ("punkt", "alampunkt"):
                self._punkt_into(child, paragraphs)
            elif tag == "muutmismarge":
                continue  # tracked separately, not body content
            # Ignore Nr/title/etc. — those were already used by the parent

    def _loige_into(self, el: etree._Element, paragraphs: list[Paragraph], index: int) -> None:
        prefix = _format_loige_label(el, index)
        # Collect first the text of any sisuTekst, then descend into punkts
        first_paragraph_idx = len(paragraphs)
        for child in el:
            tag = _ln(child)
            if tag == "sisuTekst":
                self._sisutekst_to_paragraphs_into(child, paragraphs)
            elif tag in ("punkt", "alampunkt"):
                self._punkt_into(child, paragraphs)
            elif tag == "muutmismarge":
                continue
        # Prepend the loige label to the first paragraph that came from this
        # loige's body text, if any.
        if prefix and first_paragraph_idx < len(paragraphs):
            first = paragraphs[first_paragraph_idx]
            paragraphs[first_paragraph_idx] = Paragraph(
                css_class=first.css_class, text=f"{prefix}{first.text}"
            )

    def _punkt_into(self, el: etree._Element, paragraphs: list[Paragraph]) -> None:
        prefix = _format_punkt_label(el)
        first_paragraph_idx = len(paragraphs)
        for child in el:
            tag = _ln(child)
            if tag == "sisuTekst":
                self._sisutekst_to_paragraphs_into(child, paragraphs)
            elif tag in ("punkt", "alampunkt"):
                self._punkt_into(child, paragraphs)
            elif tag == "muutmismarge":
                continue
        if prefix and first_paragraph_idx < len(paragraphs):
            first = paragraphs[first_paragraph_idx]
            paragraphs[first_paragraph_idx] = Paragraph(
                css_class=first.css_class, text=f"{prefix}{first.text}"
            )

    def _sisutekst_to_paragraphs(self, el: etree._Element) -> list[Paragraph]:
        out: list[Paragraph] = []
        self._sisutekst_to_paragraphs_into(el, out)
        return out

    def _sisutekst_to_paragraphs_into(
        self, el: etree._Element, paragraphs: list[Paragraph]
    ) -> None:
        """Convert a <sisuTekst> element's body into Paragraph objects."""
        for child in el:
            tag = _ln(child)
            if tag == "tavatekst":
                text = _collapse_ws(_extract_inline(child))
                if text and not _DATE_ONLY_RE.match(text):
                    # tavatekst may contain its own newlines (from <reavahetus/>)
                    # Split on double-newline so each paragraph stays separate
                    for chunk in re.split(r"\n+", text):
                        chunk = chunk.strip()
                        if chunk:
                            paragraphs.append(Paragraph(css_class="parrafo", text=chunk))
            elif tag == "tavatekstLopp":
                text = _collapse_ws(_extract_inline(child))
                if text:
                    paragraphs.append(Paragraph(css_class="parrafo", text=text))
            elif tag == "HTMLKonteiner":
                # CDATA-wrapped HTML used in algtekst documents (amendments).
                # Convert to markdown using the same Paragraph stream.
                self._html_konteiner_to_paragraphs(child, paragraphs)
            elif tag == "muutmismarge":
                continue  # reform metadata, not body content

    def _html_konteiner_to_paragraphs(
        self, el: etree._Element, paragraphs: list[Paragraph]
    ) -> None:
        """Parse the CDATA HTML inside <HTMLKonteiner> into Paragraphs."""
        # The CDATA contents are accessible via .text on the element
        cdata = el.text or ""
        if not cdata.strip():
            return
        # Wrap in a root and parse as HTML fragment
        try:
            root = etree.fromstring(f"<root>{cdata}</root>")
        except etree.XMLSyntaxError:
            # Fall back to lxml.html which is more permissive
            try:
                from lxml import html as lhtml

                root = lhtml.fragment_fromstring(cdata, create_parent="root")
            except Exception:
                # Last resort: emit as plain text
                text = re.sub(r"<[^>]+>", "", cdata).strip()
                if text:
                    paragraphs.append(Paragraph(css_class="parrafo", text=text))
                return

        for el_p in root.iter():
            tag = _ln(el_p)
            if tag == "p":
                text = _html_inline(el_p).strip()
                if text:
                    paragraphs.append(Paragraph(css_class="parrafo", text=text))

    def _lisa_block(self, el: etree._Element, pub_date: date, norm_id: str) -> Block | None:
        """Build a Block for a <lisaViide> (annex reference).

        Strategy (from PDF_INVESTIGATION.md):
          1. Extract the base64-encoded PDF from ``<fail failVorming="pdf">``
          2. Run pdfplumber to detect structured tables in the PDF
          3. If tables are found, embed them inline as Markdown pipe tables
             (css_class="table") — 75 % of Estonian annexes are real legal
             tables (damage scales, permit matrices, balance sheet schemas…)
          4. If no tables are found (graphic-only PDFs: tax stamp designs,
             blank forms), fall back to a plain link to the original PDF on
             riigiteataja.ee.
        """
        import base64

        from legalize.fetcher.ee.pdf_lisa import (
            has_tabular_content,
            pdf_to_markdown_tables,
        )

        # Find the title (lisaPealkiri/lisaNimi)
        title_el = _findone(el, "lisaNimi")
        title = (title_el.text or "").strip() if title_el is not None else ""

        # Find any embedded files
        fail = _findone(el, "fail")
        annex_lines: list[Paragraph] = []
        if title:
            annex_lines.append(Paragraph(css_class="centro_negrita", text=f"Lisa: {title}"))

        if fail is not None:
            fail_name = fail.get("failNimi", "lisa.pdf")
            fail_format = (fail.get("failVorming") or "pdf").lower()
            link_url = f"https://www.riigiteataja.ee/aktilisa/{fail_name}"

            # Try to extract tables from embedded PDFs
            tables_md: list[str] = []
            if fail_format == "pdf" and (fail.text or "").strip():
                try:
                    pdf_bytes = base64.b64decode(re.sub(r"\s+", "", fail.text or ""))
                    tables_md = pdf_to_markdown_tables(pdf_bytes)
                except Exception:
                    tables_md = []

            if has_tabular_content(tables_md):
                # Embed the converted tables inline
                for md_table in tables_md:
                    annex_lines.append(Paragraph(css_class="table", text=md_table))
                # Always keep a reference to the original PDF at the bottom
                annex_lines.append(
                    Paragraph(
                        css_class="parrafo",
                        text=f"📎 [Original PDF: {fail_name}]({link_url})",
                    )
                )
            else:
                # Fall back to a plain link — the PDF is a graphic design,
                # a blank form template, or pdfplumber failed.
                annex_lines.append(
                    Paragraph(
                        css_class="parrafo",
                        text=f"📎 [{fail_name}]({link_url}) ({fail_format.upper()} annex)",
                    )
                )

        if not annex_lines:
            return None

        return Block(
            id=el.get("id") or "lisa",
            block_type="annex",
            title=title or "Lisa",
            versions=(
                Version(
                    norm_id=norm_id,
                    publication_date=pub_date,
                    effective_date=pub_date,
                    paragraphs=tuple(annex_lines),
                ),
            ),
        )

    def _signers_block(self, root: etree._Element, pub_date: date, norm_id: str) -> Block | None:
        """Build a final Block with the signatories of the act."""
        signers: list[Paragraph] = []
        for allkiri in root.iter():
            if _ln(allkiri) != "allkiri":
                continue
            for sig in allkiri.iter():
                if _ln(sig) != "allkirjastaja":
                    continue
                position = _direct_child_text(sig, "ametinimetus")
                first = _direct_child_text(sig, "eesnimi")
                last = _direct_child_text(sig, "perekonnanimi")
                full_name = " ".join(p for p in (first, last) if p)
                if position:
                    signers.append(Paragraph(css_class="firma_rey", text=position))
                if full_name:
                    signers.append(Paragraph(css_class="firma_ministro", text=full_name))
        if not signers:
            return None
        return Block(
            id="allkiri",
            block_type="signature",
            title="",
            versions=(
                Version(
                    norm_id=norm_id,
                    publication_date=pub_date,
                    effective_date=pub_date,
                    paragraphs=tuple(signers),
                ),
            ),
        )

    # --- helpers ---

    def _publication_date(self, root: etree._Element) -> date | None:
        meta = _findone(root, "metaandmed")
        if meta is None:
            return None
        for am in _direct_children(meta, "avaldamismarge"):
            kp = _findone(am, "avaldamineKuupaev")
            if kp is not None and kp.text:
                d = _parse_date(kp.text)
                if d:
                    return d
        # Fallback: kehtivuseAlgus
        keh = _findone(meta, "kehtivus")
        if keh is not None:
            algus = _findone(keh, "kehtivuseAlgus")
            if algus is not None and algus.text:
                return _parse_date(algus.text)
        return None


def _html_inline(el: etree._Element) -> str:
    """Extract HTML inline text into Markdown.

    Handles:
      <b>X</b>     → **X**
      <i>X</i>     → *X*
      <sup>X</sup> → <sup>X</sup>  (kept as HTML; GitHub renders it)
      <br/>        → newline
      &nbsp;       → space
    All other tags pass through their content.
    """
    parts: list[str] = []
    if el.text:
        parts.append(el.text)
    for child in el:
        tag = _ln(child)
        if tag == "b" or tag == "strong":
            parts.append(f"**{_html_inline(child)}**")
        elif tag == "i" or tag == "em":
            parts.append(f"*{_html_inline(child)}*")
        elif tag == "sup":
            parts.append(f"<sup>{_html_inline(child)}</sup>")
        elif tag == "br":
            parts.append("\n")
        else:
            parts.append(_html_inline(child))
        if child.tail:
            parts.append(child.tail)
    return _collapse_ws("".join(parts))


# ─────────────────────────────────────────────
# Metadata parser
# ─────────────────────────────────────────────


class RTMetadataParser(MetadataParser):
    """Parses Estonian Riigi Teataja XML <metaandmed> into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        root = etree.fromstring(data, parser=_LXML_PARSER)
        meta = _findone(root, "metaandmed")
        if meta is None:
            raise ValueError(f"No <metaandmed> element in XML for {norm_id}")

        # Title
        title_el = _findone(_findone(root, "aktinimi"), "pealkiri")
        title = ""
        if title_el is not None:
            title = _collapse_ws(_extract_inline(title_el)).strip()

        # Document type → rank
        doc_type = _direct_child_text(meta, "dokumentLiik")
        rank_str = DOC_TYPE_TO_RANK.get(doc_type, doc_type or "muu")

        # Publication date (avaldamismarge/avaldamineKuupaev)
        pub_date = date(1900, 1, 1)
        for am in _direct_children(meta, "avaldamismarge"):
            kp_el = _findone(am, "avaldamineKuupaev")
            if kp_el is not None and kp_el.text:
                d = _parse_date(kp_el.text)
                if d:
                    pub_date = d
                    break

        # Status from kehtivus.kehtivuseLopp (presence + past date → REPEALED)
        status = NormStatus.IN_FORCE
        eff_until: date | None = None
        eff_from: date | None = None
        keh = _findone(meta, "kehtivus")
        if keh is not None:
            algus_el = _findone(keh, "kehtivuseAlgus")
            if algus_el is not None and algus_el.text:
                eff_from = _parse_date(algus_el.text)
            lopp_el = _findone(keh, "kehtivuseLopp")
            if lopp_el is not None and lopp_el.text:
                eff_until = _parse_date(lopp_el.text)
                if eff_until and eff_until < date.today():
                    status = NormStatus.REPEALED

        # Issuer
        department = _direct_child_text(meta, "valjaandja") or _DEFAULT_DEPARTMENT

        # Source URL — prefer ELI if available, otherwise the akt page
        global_id = _direct_child_text(meta, "globaalID") or norm_id
        source = f"https://www.riigiteataja.ee/akt/{global_id}"

        # Subjects (marksona — there can be multiple)
        subjects: tuple[str, ...] = tuple(
            (m.text or "").strip()
            for m in _direct_children(meta, "marksona")
            if m is not None and (m.text or "").strip()
        )

        # Last modified
        last_modified = _parse_date(_direct_child_text(meta, "metaandmedVersioonKuupaev"))

        # Short title (lyhend)
        short_title = _direct_child_text(meta, "lyhend") or title

        # Summary
        summary = _direct_child_text(meta, "eesmark")

        # Build country-specific extra fields
        extra: list[tuple[str, str]] = []

        if short_title and short_title != title:
            extra.append(("short_title", short_title))

        text_type = _direct_child_text(meta, "tekstiliik")
        if text_type:
            extra.append(("text_type", text_type))

        # vastuvoetud — adoption metadata
        vastuvoetud = _findone(meta, "vastuvoetud")
        if vastuvoetud is not None:
            adoption_date = _parse_date(_direct_child_text(vastuvoetud, "aktikuupaev"))
            if adoption_date:
                extra.append(("adoption_date", adoption_date.isoformat()))
            act_number = _direct_child_text(vastuvoetud, "aktiNr")
            if act_number:
                extra.append(("act_number", act_number))
            joustumine = _parse_date(_direct_child_text(vastuvoetud, "joustumine"))
            if joustumine:
                extra.append(("original_effective_date", joustumine.isoformat()))
            # Original publication marker
            am_orig = _findone(vastuvoetud, "avaldamismarge")
            if am_orig is not None:
                orig = _format_avaldamismarge(am_orig)
                if orig:
                    extra.append(("original_publication", orig))

        # Current avaldamismarge — RT section/year/issue/article
        for am in _direct_children(meta, "avaldamismarge"):
            rt_section = _direct_child_text(am, "RTosa")
            if rt_section:
                extra.append(("rt_section", rt_section))
            rt_year = _direct_child_text(am, "RTaasta")
            if rt_year:
                extra.append(("rt_year", rt_year))
            rt_number = _direct_child_text(am, "RTnr")
            if rt_number:
                extra.append(("rt_number", rt_number))
            rt_article = _direct_child_text(am, "RTartikkel")
            if rt_article:
                extra.append(("rt_article", rt_article))
            rt_ref = _direct_child_text(am, "aktViide")
            if rt_ref:
                extra.append(("rt_reference", rt_ref))
            break  # Only the first/current one

        if eff_from:
            extra.append(("effective_from", eff_from.isoformat()))
        if eff_until:
            extra.append(("effective_until", eff_until.isoformat()))

        # Versioning info
        version_num = _direct_child_text(meta, "metaandmedVersioon")
        if version_num:
            extra.append(("metadata_version", version_num))
        editor = _direct_child_text(meta, "metaandmedVersioonPohjustaja")
        if editor:
            extra.append(("editor", editor))
        group_id = _direct_child_text(meta, "terviktekstiGrupiID")
        if group_id:
            extra.append(("group_id", group_id))
        schema = _direct_child_text(meta, "skeemiNimi")
        if schema:
            extra.append(("schema", schema))

        return NormMetadata(
            title=title,
            short_title=short_title,
            identifier=global_id,
            country="ee",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=department,
            source=source,
            last_modified=last_modified,
            subjects=subjects,
            summary=summary,
            extra=tuple(extra),
        )


def _format_avaldamismarge(am: etree._Element) -> str:
    """Format an <avaldamismarge> element as a citation string.

    Example output: "RT I, 2003, 29, 174"
    """
    parts: list[str] = []
    osa = _direct_child_text(am, "RTosa")
    aasta = _direct_child_text(am, "RTaasta")
    nr = _direct_child_text(am, "RTnr")
    art = _direct_child_text(am, "RTartikkel")
    if osa:
        parts.append(osa)
    if aasta:
        parts.append(aasta)
    if nr:
        parts.append(nr)
    if art:
        parts.append(art)
    return ", ".join(parts) if parts else ""
