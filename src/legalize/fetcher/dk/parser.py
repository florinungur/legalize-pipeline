"""Parser for Danish LexDania 2.1 XML documents from Retsinformation.

Retsinformation publishes legislation as proprietary LexDania 2.1 XML. This
parser converts those documents into the generic Legalize Block/NormMetadata
model.

Hierarchy mapping (Danish → block_type → markdown heading):
  Bog        → book       → # (titulo_tit)
  Afsnit     → part       → ## (titulo_tit)
  Kapitel    → chapter    → ### (capitulo_tit)
  ParagrafGruppe > Rubrica → section_heading → #### (capitulo_tit)
  Paragraf   → article    → ##### (articulo)
  Stk        → (content inside Paragraf)

Rich formatting:
  <Char formaChar="Bold">   → **text**
  <Char formaChar="Italic"> → *text*
  <Table>/<Tr>/<Td>          → Markdown pipe table
  <Indentatio>               → numbered list item
  <Nota>                     → footnote
"""

from __future__ import annotations

import logging
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
    Reform,
    Version,
)

logger = logging.getLogger(__name__)

_LXML_PARSER = etree.XMLParser(huge_tree=True)

# ─── Type mapping ───

_DOCTYPE_TO_RANK: dict[str, str] = {
    "LOV H": "lov",
    "LOV Æ": "aendringslov",
    "LBK H": "lovbekendtgoerelse",
    "BEK H": "bekendtgoerelse",
    "BEK Æ": "bekendtgoerelse_aendring",
    "Lov": "lov",
}

# ─── Helpers ───


def _text_content(el: etree._Element) -> str:
    """Recursively extract text from an element, handling inline formatting.

    Handles:
    - <Char formaChar="Bold"> → **text**
    - <Char formaChar="Italic"> → *text*
    - Nested elements
    """
    if el is None:
        return ""

    parts: list[str] = []

    if el.text:
        parts.append(el.text)

    for child in el:
        tag = child.tag
        if tag == "Char":
            char_text = child.text or ""
            # Handle nested children (rare)
            for sub in child:
                char_text += _text_content(sub)
                if sub.tail:
                    char_text += sub.tail
            fmt = child.get("formaChar", "")
            if fmt == "Bold" and char_text.strip():
                parts.append(f"**{char_text}**")
            elif fmt == "Italic" and char_text.strip():
                parts.append(f"*{char_text}*")
            else:
                parts.append(char_text)
        elif tag == "Linea":
            parts.append(_text_content(child))
        elif tag == "Exitus":
            parts.append(_text_content(child))
        elif tag == "Explicatus":
            parts.append(child.text or "")
        else:
            parts.append(_text_content(child))

        if child.tail:
            parts.append(child.tail)

    return "".join(parts)


def _parse_date(date_str: str) -> date | None:
    """Parse YYYY-MM-DD date string."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def _extract_char_text(parent: etree._Element) -> str:
    """Extract text from Exitus > Linea > Char hierarchy."""
    lines: list[str] = []
    for exitus in parent.findall("Exitus"):
        for linea in exitus.findall("Linea"):
            line_text = _text_content(linea).strip()
            if line_text:
                lines.append(line_text)
    return "\n".join(lines)


# ─── Table parsing ───


def _parse_table(table_el: etree._Element) -> str:
    """Convert a LexDania <Table> element to a Markdown pipe table."""
    rows: list[list[str]] = []
    for tr in table_el.findall("Tr"):
        cells: list[str] = []
        for td in tr.findall("Td"):
            cell_text = _text_content(td).strip().replace("\n", " ").replace("|", "\\|")
            cells.append(cell_text)
        if cells:
            rows.append(cells)

    if not rows:
        return ""

    max_cols = max(len(r) for r in rows)
    for row in rows:
        while len(row) < max_cols:
            row.append("")

    lines: list[str] = []
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join("---" for _ in range(max_cols)) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


# ─── Indentation / list parsing ───


def _parse_index(index_el: etree._Element) -> list[str]:
    """Parse an Index element containing Indentatio list items."""
    items: list[str] = []
    for indent in index_el.findall("Indentatio"):
        _collect_indentatio(indent, items, depth=0)
    return items


def _collect_indentatio(indent: etree._Element, items: list[str], depth: int) -> None:
    """Recursively collect text from an Indentatio and its nested sub-items."""
    num_el = indent.find("Explicatus")
    num_text = (num_el.text or "").strip() if num_el is not None else "-"

    # Collect text from direct Exitus > Linea > Char (skip nested Index)
    texts: list[str] = []
    for exitus in indent.findall("Exitus"):
        # Only take Linea children (not Index which contains sub-items)
        for linea in exitus.findall("Linea"):
            line_text = _text_content(linea).strip()
            if line_text:
                texts.append(line_text)

    prefix = "  " * depth
    if texts:
        items.append(f"{prefix}{num_text} {' '.join(texts)}")

    # Recurse into nested Index > Indentatio (sub-items like a), b))
    for exitus in indent.findall("Exitus"):
        for sub_index in exitus.findall("Index"):
            for sub_indent in sub_index.findall("Indentatio"):
                _collect_indentatio(sub_indent, items, depth + 1)


# ─── Block construction ───


def _make_block(
    block_id: str,
    block_type: str,
    title: str,
    paragraphs: list[Paragraph],
    pub_date: date,
    norm_id: str,
) -> Block:
    """Create a Block with a single Version."""
    version = Version(
        norm_id=norm_id,
        publication_date=pub_date,
        effective_date=pub_date,
        paragraphs=tuple(paragraphs),
    )
    return Block(
        id=block_id,
        block_type=block_type,
        title=title,
        versions=(version,),
    )


# ─── Stk (subsection) parsing ───


def _parse_stk(stk: etree._Element) -> list[Paragraph]:
    """Parse a Stk (subsection) element into paragraphs."""
    paragraphs: list[Paragraph] = []

    # Subsection number (e.g. "Stk. 2.")
    expl = stk.find("Explicatus")
    prefix = (expl.text or "").strip() + " " if expl is not None and expl.text else ""

    # Walk Exitus children: may contain Linea (text), Table, or Index
    first_line = True
    for exitus in stk.findall("Exitus"):
        for child in exitus:
            if child.tag == "Linea":
                text = _text_content(child).strip()
                if text:
                    if first_line and prefix:
                        text = f"{prefix}{text}"
                        first_line = False
                    paragraphs.append(Paragraph(css_class="parrafo", text=text))
            elif child.tag == "Table":
                table_md = _parse_table(child)
                if table_md:
                    paragraphs.append(Paragraph(css_class="table_row", text=table_md))
            elif child.tag == "Index":
                items = _parse_index(child)
                for item in items:
                    paragraphs.append(Paragraph(css_class="list_item", text=item))

    # Amendment elements (in LOV Æ documents)
    for anu in stk.findall("AendringsNummer"):
        anu_text = _parse_amendment_number(anu)
        if anu_text:
            paragraphs.append(Paragraph(css_class="parrafo", text=anu_text))

    return paragraphs


def _parse_amendment_number(anu: etree._Element) -> str:
    """Parse an AendringsNummer (amendment instruction) into text."""
    parts: list[str] = []
    expl = anu.find("Explicatus")
    if expl is not None and expl.text:
        parts.append(expl.text.strip())

    for aendring in anu.findall("Aendring"):
        defn = aendring.find("AendringDefinition")
        if defn is not None:
            text = _extract_char_text(defn)
            if text:
                parts.append(text)
        action = aendring.find("AendringAktion")
        if action is not None:
            new_text = action.find("AendringNyTekst")
            if new_text is not None:
                text = _extract_char_text(new_text)
                if text:
                    parts.append(f"> {text}")

    return " ".join(parts)


# ─── Paragraf parsing ───


def _parse_paragraf(
    paragraf: etree._Element, pub_date: date, norm_id: str, idx: int
) -> Block | None:
    """Parse a <Paragraf> (§) element into a single Block."""
    expl = paragraf.find("Explicatus")
    par_num = (expl.text or "").strip() if expl is not None else ""

    paragraphs: list[Paragraph] = []

    # Section heading
    if par_num:
        paragraphs.append(Paragraph(css_class="articulo", text=par_num))

    # Parse each Stk
    for stk in paragraf.findall("Stk"):
        paragraphs.extend(_parse_stk(stk))

    if not paragraphs:
        return None

    eid = paragraf.get("id", f"par-{idx}")
    title = par_num or f"Paragraf {idx}"
    return _make_block(eid, "article", title, paragraphs, pub_date, norm_id)


# ─── Kapitel (chapter) parsing ───


def _parse_kapitel(
    kapitel: etree._Element, pub_date: date, norm_id: str, start_idx: int
) -> list[Block]:
    """Parse a <Kapitel> element into blocks (heading + child Paragraf)."""
    blocks: list[Block] = []
    idx = start_idx

    # Chapter number and title
    expl = kapitel.find("Explicatus")
    rubrica = kapitel.find("Rubrica")
    num_text = (expl.text or "").strip() if expl is not None else ""
    heading_text = _text_content(rubrica).strip() if rubrica is not None else ""

    if num_text or heading_text:
        combined = (
            f"{num_text} {heading_text}".strip()
            if num_text and heading_text
            else (num_text or heading_text)
        )
        paragraphs = [Paragraph(css_class="capitulo_tit", text=combined)]
        blocks.append(
            _make_block(
                kapitel.get("id", f"kap-{idx}"),
                "chapter",
                combined,
                paragraphs,
                pub_date,
                norm_id,
            )
        )
        idx += 1

    # ParagrafGruppe may have its own Rubrica (section sub-heading)
    for pg in kapitel.findall("ParagrafGruppe"):
        pg_rubrica = pg.find("Rubrica")
        if pg_rubrica is not None:
            pg_heading = _text_content(pg_rubrica).strip()
            if pg_heading:
                paragraphs = [Paragraph(css_class="capitulo_tit", text=pg_heading)]
                blocks.append(
                    _make_block(
                        pg.get("id", f"pg-{idx}"),
                        "section_heading",
                        pg_heading,
                        paragraphs,
                        pub_date,
                        norm_id,
                    )
                )
                idx += 1

        for paragraf in pg.findall("Paragraf"):
            blk = _parse_paragraf(paragraf, pub_date, norm_id, idx)
            if blk:
                blocks.append(blk)
                idx += 1

    # Nota elements (footnotes at chapter level)
    for nota in kapitel.findall("Nota"):
        nota_text = _extract_nota(nota)
        if nota_text:
            paragraphs = [Paragraph(css_class="parrafo", text=f"*{nota_text}*")]
            blocks.append(_make_block(f"nota-{idx}", "note", "Note", paragraphs, pub_date, norm_id))
            idx += 1

    return blocks


def _extract_nota(nota: etree._Element) -> str:
    """Extract footnote text from a Nota element."""
    parts: list[str] = []
    expl = nota.find("Explicatus")
    if expl is not None and expl.text:
        parts.append(expl.text.strip())
    text = _extract_char_text(nota)
    if text:
        parts.append(text)
    return " ".join(parts)


# ─── Amendment section parsing (LOV Æ) ───


def _parse_amendment_section(
    acp: etree._Element, pub_date: date, norm_id: str, start_idx: int
) -> list[Block]:
    """Parse an AendringCentreretParagraf (amendment section) into blocks.

    These appear in LOV Æ (amendment laws) and contain the amendment
    instructions for each modified section of the target law.
    """
    blocks: list[Block] = []
    idx = start_idx

    paragraphs: list[Paragraph] = []

    # Section header (e.g. "§ 1")
    expl = acp.find("Explicatus")
    header = (expl.text or "").strip() if expl is not None else ""
    if header:
        paragraphs.append(Paragraph(css_class="articulo", text=header))

    # Introductory text
    for exitus in acp.findall("Exitus"):
        for linea in exitus.findall("Linea"):
            text = _text_content(linea).strip()
            if text:
                paragraphs.append(Paragraph(css_class="parrafo", text=text))

    # Amendment instructions
    for anu in acp.findall("AendringsNummer"):
        anu_text = _parse_amendment_number(anu)
        if anu_text:
            paragraphs.append(Paragraph(css_class="parrafo", text=anu_text))

    if paragraphs:
        title = header or f"Amendment {idx}"
        blocks.append(
            _make_block(
                acp.get("id", f"acp-{idx}"),
                "article",
                title,
                paragraphs,
                pub_date,
                norm_id,
            )
        )

    return blocks


# ─── Top-level body parsing ───


def _parse_body(content_root: etree._Element, pub_date: date, norm_id: str) -> list[Block]:
    """Parse DokumentIndhold body into blocks."""
    blocks: list[Block] = []
    idx = 0

    # Introduction (LBK preamble)
    intro = content_root.find("Indledning")
    if intro is not None:
        text = _extract_char_text(intro).strip()
        if text:
            paragraphs = [Paragraph(css_class="parrafo", text=text)]
            blocks.append(
                _make_block("intro", "preamble", "Indledning", paragraphs, pub_date, norm_id)
            )
            idx += 1

    # Hymne (royal assent formula — LOV documents)
    hymne = content_root.find("Hymne")
    if hymne is not None:
        text = _extract_char_text(hymne).strip()
        if text:
            paragraphs = [Paragraph(css_class="parrafo", text=text)]
            blocks.append(
                _make_block("hymne", "preamble", "Preamble", paragraphs, pub_date, norm_id)
            )
            idx += 1

    # Walk the body hierarchy: Bog → Afsnit → Kapitel → ParagrafGruppe → Paragraf
    for bog in content_root.findall("Bog"):
        # Book heading (rare)
        rubrica = bog.find("Rubrica")
        if rubrica is not None:
            text = _text_content(rubrica).strip()
            if text:
                paragraphs = [Paragraph(css_class="titulo_tit", text=text)]
                blocks.append(
                    _make_block(
                        bog.get("id", f"bog-{idx}"),
                        "book",
                        text,
                        paragraphs,
                        pub_date,
                        norm_id,
                    )
                )
                idx += 1

        for afsnit in bog.findall("Afsnit"):
            blocks.extend(_parse_afsnit(afsnit, pub_date, norm_id, idx))
            idx = len(blocks)

    # Direct Afsnit without Bog wrapper
    for afsnit in content_root.findall("Afsnit"):
        blocks.extend(_parse_afsnit(afsnit, pub_date, norm_id, idx))
        idx = len(blocks)

    # Direct Kapitel without Afsnit/Bog wrappers
    for kapitel in content_root.findall("Kapitel"):
        blocks.extend(_parse_kapitel(kapitel, pub_date, norm_id, idx))
        idx = len(blocks)

    # Direct ParagrafGruppe without wrappers
    for pg in content_root.findall("ParagrafGruppe"):
        for paragraf in pg.findall("Paragraf"):
            blk = _parse_paragraf(paragraf, pub_date, norm_id, idx)
            if blk:
                blocks.append(blk)
                idx += 1

    # AendringCentreretParagraf (amendment sections — LOV Æ documents)
    for acp in content_root.findall("AendringCentreretParagraf"):
        acp_blocks = _parse_amendment_section(acp, pub_date, norm_id, idx)
        blocks.extend(acp_blocks)
        idx = len(blocks)

    # Ikraft (entry-into-force provisions) — LBK documents may have many
    for ikraft in content_root.findall("Ikraft"):
        ikraft_paragraphs: list[Paragraph] = []
        for child in ikraft:
            if child.tag == "Exitus":
                # Text lines with inline Nota footnotes
                for subchild in child:
                    if subchild.tag == "Linea":
                        text = _text_content(subchild).strip()
                        if text:
                            ikraft_paragraphs.append(Paragraph(css_class="parrafo", text=text))
                    elif subchild.tag == "Nota":
                        nota_text = _extract_nota(subchild)
                        if nota_text:
                            ikraft_paragraphs.append(
                                Paragraph(css_class="parrafo", text=f"*{nota_text}*")
                            )
            elif child.tag == "IkraftCentreretParagraf":
                # Centered paragraphs with Stk subsections
                icp_expl = child.find("Explicatus")
                icp_header = (icp_expl.text or "").strip() if icp_expl is not None else ""
                if icp_header:
                    ikraft_paragraphs.append(Paragraph(css_class="articulo", text=icp_header))
                for stk in child.findall("Stk"):
                    ikraft_paragraphs.extend(_parse_stk(stk))
            elif child.tag == "Paragraf":
                blk = _parse_paragraf(child, pub_date, norm_id, idx)
                if blk:
                    blocks.append(blk)
                    idx += 1

        if ikraft_paragraphs:
            blocks.append(
                _make_block(
                    "ikraft",
                    "entry_into_force",
                    "Ikrafttrædelse",
                    ikraft_paragraphs,
                    pub_date,
                    norm_id,
                )
            )
            idx += 1

    # Footnotes at document level
    for nota in content_root.findall(".//Nota"):
        # Only process top-level Nota (not already in Kapitel)
        parent = nota.getparent()
        if parent is not None and parent.tag not in ("Kapitel", "Afsnit"):
            nota_text = _extract_nota(nota)
            if nota_text:
                paragraphs = [Paragraph(css_class="parrafo", text=f"*{nota_text}*")]
                blocks.append(
                    _make_block(f"nota-{idx}", "note", "Note", paragraphs, pub_date, norm_id)
                )
                idx += 1

    return blocks


def _parse_afsnit(
    afsnit: etree._Element, pub_date: date, norm_id: str, start_idx: int
) -> list[Block]:
    """Parse an Afsnit (Part/Section) element."""
    blocks: list[Block] = []
    idx = start_idx

    # Part heading
    expl = afsnit.find("Explicatus")
    rubrica = afsnit.find("Rubrica")
    num_text = (expl.text or "").strip() if expl is not None else ""
    heading_text = _text_content(rubrica).strip() if rubrica is not None else ""

    if num_text or heading_text:
        combined = (
            f"{num_text} {heading_text}".strip()
            if num_text and heading_text
            else (num_text or heading_text)
        )
        paragraphs = [Paragraph(css_class="titulo_tit", text=combined)]
        blocks.append(
            _make_block(
                afsnit.get("id", f"afs-{idx}"),
                "part",
                combined,
                paragraphs,
                pub_date,
                norm_id,
            )
        )
        idx += 1

    for kapitel in afsnit.findall("Kapitel"):
        blocks.extend(_parse_kapitel(kapitel, pub_date, norm_id, idx))
        idx = start_idx + len(blocks)

    return blocks


# ─── TextParser ───


class DanishTextParser(TextParser):
    """Parses LexDania 2.1 XML body into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        if not data:
            return []

        try:
            root = etree.fromstring(data, parser=_LXML_PARSER)
        except etree.XMLSyntaxError as exc:
            logger.warning("Failed to parse XML: %s", exc)
            return []

        body = root.find("DokumentIndhold")
        if body is None:
            return []

        pub_date = _extract_pub_date(root)
        norm_id = _extract_accession_number(root)

        return _parse_body(body, pub_date, norm_id)

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline from Change elements in Meta."""
        if not data:
            return []

        try:
            root = etree.fromstring(data, parser=_LXML_PARSER)
        except etree.XMLSyntaxError:
            return []

        meta = root.find("Meta")
        if meta is None:
            return []

        reforms: list[Reform] = []
        changes = meta.findall("Change")

        for change in changes:
            change_id = change.get("id", "")
            ref_accn_el = meta.find(f"Ref_Accn[@REFid='{change_id}']")
            ref_af_el = meta.find(f"Ref_Af[@REFid='{change_id}']")

            if ref_accn_el is None or ref_af_el is None:
                continue

            ref_accn = (ref_accn_el.text or "").strip()
            ref_date = _parse_date((ref_af_el.text or "").strip())

            if ref_date and ref_accn:
                reforms.append(
                    Reform(
                        date=ref_date,
                        norm_id=ref_accn,
                        affected_blocks=(),
                    )
                )

        reforms.sort(key=lambda r: r.date)
        return reforms


# ─── Metadata extraction helpers ───


def _extract_pub_date(root: etree._Element) -> date:
    """Extract publication date from DiesSigni or StartDate."""
    meta = root.find("Meta")
    if meta is not None:
        for field in ("DiesSigni", "StartDate", "DiesEdicti"):
            el = meta.find(field)
            if el is not None and el.text:
                d = _parse_date(el.text)
                if d:
                    return d
    return date(2000, 1, 1)


def _extract_accession_number(root: etree._Element) -> str:
    """Extract AccessionNumber from Meta."""
    meta = root.find("Meta")
    if meta is not None:
        el = meta.find("AccessionNumber")
        if el is not None and el.text:
            return el.text.strip()
    return ""


# ─── Document type extraction ───

_DOCTYPE_PATTERN = re.compile(r"^((?:LOV|LBK|BEK)\s+[HÆ])")


def _extract_doc_type(root: etree._Element) -> str:
    """Extract short document type (e.g. 'LBK H') from DocumentType field.

    DocumentType format: ``LBK H#LOKDOK03`` → returns ``LBK H``.
    """
    meta = root.find("Meta")
    if meta is None:
        return ""
    el = meta.find("DocumentType")
    if el is None or not el.text:
        return ""
    match = _DOCTYPE_PATTERN.match(el.text.strip())
    return match.group(1) if match else el.text.strip().split("#")[0].strip()


# ─── MetadataParser ───


class DanishMetadataParser(MetadataParser):
    """Extracts NormMetadata from LexDania 2.1 XML."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        root = etree.fromstring(data, parser=_LXML_PARSER)
        meta = root.find("Meta")
        if meta is None:
            raise ValueError(f"No Meta element in XML for {norm_id}")

        # ── Core fields ──
        accn = (meta.findtext("AccessionNumber") or "").strip()
        identifier = accn or norm_id.replace("/", "-")

        title = (meta.findtext("DocumentTitle") or "").strip()
        short_title = title[:80] + "..." if len(title) > 80 else title

        # Document type → rank
        doc_type = _extract_doc_type(root)
        rank = Rank(_DOCTYPE_TO_RANK.get(doc_type, doc_type.lower().replace(" ", "_") or "unknown"))

        # Status
        status_text = (meta.findtext("Status") or "").strip()
        status = NormStatus.IN_FORCE if status_text == "Valid" else NormStatus.REPEALED

        # Dates
        pub_date = _extract_pub_date(root)

        start_date_el = meta.find("StartDate")
        start_date = (
            _parse_date(start_date_el.text)
            if start_date_el is not None and start_date_el.text
            else None
        )

        end_date_el = meta.find("EndDate")
        end_date = (
            _parse_date(end_date_el.text) if end_date_el is not None and end_date_el.text else None
        )

        # Department
        ministry = (meta.findtext("Ministry") or "").strip()
        authority = (meta.findtext("AdministrativeAuthority") or "").strip()
        department = ministry or authority

        # Source URL
        year = (meta.findtext("Year") or "").strip()
        number = (meta.findtext("Number") or "").strip()
        source = (
            f"https://www.retsinformation.dk/eli/lta/{year}/{number}" if year and number else ""
        )

        # Popular title
        popular_title = (meta.findtext("PopularTitle") or "").strip()

        # Extra fields
        extra: list[tuple[str, str]] = []
        if doc_type:
            extra.append(("document_type", doc_type))
        if year:
            extra.append(("year", year))
        if number:
            extra.append(("number", number))
        if popular_title:
            extra.append(("popular_title", popular_title))
        if start_date:
            extra.append(("start_date", start_date.isoformat()))
        if end_date:
            extra.append(("end_date", end_date.isoformat()))
        if authority and ministry:
            extra.append(("administrative_authority", authority))

        announced_in = (meta.findtext("AnnouncedIn") or "").strip()
        if announced_in:
            extra.append(("announced_in", announced_in))

        journal = (meta.findtext("JournalNumber") or "").strip()
        journal = journal.replace("\\n", " ").replace("\n", " ").strip()
        if journal:
            extra.append(("journal_number", journal))

        signatory = (meta.findtext("Signature") or "").strip()
        if signatory and signatory != "x":
            extra.append(("signatory", signatory))

        # Count changes (amendments)
        changes = meta.findall("Change")
        if changes:
            extra.append(("amendments_count", str(len(changes))))

        # Images dropped
        img_count = len(root.findall(".//img")) + len(root.findall(".//Image"))
        if img_count:
            extra.append(("images_dropped", str(img_count)))

        return NormMetadata(
            title=title,
            short_title=short_title,
            identifier=identifier,
            country="dk",
            rank=rank,
            publication_date=pub_date,
            status=status,
            department=department,
            source=source,
            last_modified=end_date if status == NormStatus.REPEALED else start_date,
            extra=tuple(extra),
        )
