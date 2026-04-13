"""Ukraine text and metadata parsers.

Text parser:  splits ``.txt`` plain text into Block objects using regex
              to detect articles (Стаття), chapters (Розділ/Глава), etc.
Metadata parser:  extracts structured metadata from ``.xml`` endpoint
              which returns HTML with ``<meta>`` tags.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any

import lxml.html

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.fetcher.ua.discovery import nreg_to_identifier
from legalize.models import Block, NormMetadata, NormStatus, Paragraph, Rank, Reform, Version

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Rank mapping  (Ukrainian type → legalize rank)
# ─────────────────────────────────────────────

RANK_MAP: dict[str, str] = {
    "Конституція": "konstytutsiia",
    "Конституція України": "konstytutsiia",
    "Закон": "zakon",
    "Кодекс": "kodeks",
    "Кодекс України": "kodeks",
    "Указ": "ukaz",
    "Декрет": "dekret",
    "Постанова": "postanova",
    "Розпорядження": "rozporiadzhennia",
}

STAN_MAP: dict[str, NormStatus] = {
    "Чинний": NormStatus.IN_FORCE,
    "Нечинний": NormStatus.REPEALED,
    "Втратив чинність": NormStatus.REPEALED,
    "Не набрав чинності": NormStatus.IN_FORCE,
}

# ─────────────────────────────────────────────
# Text parsing helpers
# ─────────────────────────────────────────────

# Article heading: "Стаття 1." or "Стаття 361-1. Title text"
_ARTICLE_RE = re.compile(r"^Стаття\s+(\d+(?:[-‑‒–—]\d+)*)\s*\.?\s*(.*)$", re.UNICODE)

# Chapter/section heading: "Розділ I", "Розділ XIV", "Розділ 1"
_ROZDIL_RE = re.compile(r"^Розділ\s+([IVXLCDM\d]+)", re.UNICODE)

# Sub-chapter: "Глава 1", "Глава 12"
_HLAVA_RE = re.compile(r"^Глава\s+(\d+)", re.UNICODE)

# Annex: "Додаток" (possibly followed by number/letter)
_DODATOK_RE = re.compile(r"^Додаток", re.UNICODE)

# Editorial annotation: lines fully enclosed in {braces}
_ANNOTATION_RE = re.compile(r"^\{.*\}$", re.UNICODE)

# Signature block indicators
_SIGNATURE_PREFIXES = (
    "Президент України",
    "Голова Верховної Ради",
    "Прем'єр-міністр",
    "м. Київ",
)

# Date from Vidomosti line: (Відомості ... (ВВР), 2007, № 35, ст.484)
_VIDOMOSTI_RE = re.compile(r"\(Відомості.*?,\s*(\d{4})\s*,", re.UNICODE)

# Ukrainian month names for date parsing
_UA_MONTHS: dict[str, int] = {
    "січня": 1,
    "лютого": 2,
    "березня": 3,
    "квітня": 4,
    "травня": 5,
    "червня": 6,
    "липня": 7,
    "серпня": 8,
    "вересня": 9,
    "жовтня": 10,
    "листопада": 11,
    "грудня": 12,
}

# Full date: "31 травня 2007 року" or "28 червня 1996 року"
_FULL_DATE_RE = re.compile(r"(\d{1,2})\s+(\w+)\s+(\d{4})\s+року", re.UNICODE)

# Short date in rozporiadzhennia/postanova: "від 6 червня 2007 р."
_VID_DATE_RE = re.compile(r"від\s+(\d{1,2})\s+(\w+)\s+(\d{4})\s+р\b", re.UNICODE)

# Dotted date in text: "12.08.1987"
_DOTTED_DATE_IN_TEXT_RE = re.compile(r"\b(\d{2})\.(\d{2})\.(\d{4})\b")

# Amendment reference: "№ 2222-IV від 08.12.2004" or "№ 1804-VI від 19.01.2010"
_AMENDMENT_REF_RE = re.compile(
    r"№\s+([\w/\-]+(?:-[IVXLCDM]+)?)\s+від\s+(\d{2})\.(\d{2})\.(\d{4})",
    re.UNICODE,
)


def _int_to_date(d: int) -> date | None:
    """Convert YYYYMMDD int to date. Returns None for 0 or invalid."""
    if not d:
        return None
    try:
        return date(d // 10000, (d // 100) % 100, d % 100)
    except ValueError:
        return None


# ── Organ ID → name mapping (from Rada API conventions) ──
_ORGAN_MAP: dict[int, str] = {
    1: "Верховна Рада України",
    2: "Президент України",
    3: "Кабінет Міністрів України",
}


def _resolve_organ(organs_str: str) -> str:
    """Parse organs field like '1:20070531:1103-V' → organ name."""
    if not organs_str:
        return ""
    try:
        org_id = int(organs_str.split(":")[0])
        return _ORGAN_MAP.get(org_id, f"Орган {org_id}")
    except (ValueError, IndexError):
        return ""


def extract_reforms_from_card(card: dict) -> list[Reform]:
    """Extract reform timeline from card JSON's eds[] array.

    Each edition in eds[] has:
    - datred: edition date as YYYYMMDD int
    - pidstava: nreg of the amending law (empty for original)

    Returns Reform objects sorted chronologically, skipping the original
    version (podid=4) which becomes the bootstrap commit.
    """
    reforms: list[Reform] = []
    for ed in card.get("eds", []):
        datred = ed.get("datred", 0)
        pidstava = ed.get("pidstava", "")
        podid = ed.get("podid", 0)

        # Skip the original version (podid=4) — that's the bootstrap
        if podid == 4 or not pidstava:
            continue

        d = _int_to_date(datred)
        if d:
            reforms.append(Reform(date=d, norm_id=pidstava, affected_blocks=()))

    reforms.sort(key=lambda r: r.date)
    return reforms


def _try_ua_date(day: int, month_name: str, year: int) -> date | None:
    """Try to build a date from day, Ukrainian month name, and year."""
    month = _UA_MONTHS.get(month_name.lower())
    if month:
        try:
            return date(year, month, day)
        except ValueError:
            return None
    return None


def _extract_date_from_text(text: str) -> date | None:
    """Try to extract publication date from law text content."""
    # First try: Vidomosti line (most common)
    m = _VIDOMOSTI_RE.search(text)
    if m:
        year = int(m.group(1))
        return date(year, 1, 1)

    # Second try: full date with "року" suffix (signature block)
    for m in _FULL_DATE_RE.finditer(text):
        result = _try_ua_date(int(m.group(1)), m.group(2), int(m.group(3)))
        if result:
            return result

    # Third try: "від DD month YYYY р." (CMU rozporiadzhennia/postanova)
    m = _VID_DATE_RE.search(text)
    if m:
        result = _try_ua_date(int(m.group(1)), m.group(2), int(m.group(3)))
        if result:
            return result

    # Fourth try: dotted date DD.MM.YYYY in text
    m = _DOTTED_DATE_IN_TEXT_RE.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    return None


def _is_annotation(line: str) -> bool:
    """Check if a line is an editorial annotation ``{...}``."""
    return line.startswith("{") and line.endswith("}")


def _is_signature(line: str) -> bool:
    """Check if a line is part of a signature block."""
    stripped = line.strip()
    if any(stripped.startswith(p) for p in _SIGNATURE_PREFIXES):
        return True
    # Standalone number like "№ 1103-V" at end of document
    if re.match(r"^№\s+\S+$", stripped):
        return True
    # Standalone date like "31 травня 2007 року"
    if re.match(r"^\d{1,2}\s+\w+\s+\d{4}\s+року$", stripped):
        return True
    return False


def _make_block(
    block_id: str,
    block_type: str,
    title: str,
    paragraphs: list[Paragraph],
    pub_date: date,
    norm_id: str,
) -> Block:
    version = Version(
        norm_id=norm_id,
        publication_date=pub_date,
        effective_date=pub_date,
        paragraphs=tuple(paragraphs),
    )
    return Block(id=block_id, block_type=block_type, title=title, versions=(version,))


def _flush_article(
    article_num: str,
    article_title: str,
    body_lines: list[str],
    pub_date: date,
    norm_id: str,
    block_index: int,
) -> Block:
    """Build an article Block from accumulated lines."""
    heading = f"Стаття {article_num}"
    if article_title:
        heading = f"Стаття {article_num}. {article_title}"

    paragraphs: list[Paragraph] = [Paragraph(css_class="articulo", text=heading)]

    for line in body_lines:
        if _is_annotation(line):
            continue
        paragraphs.append(Paragraph(css_class="parrafo", text=line))

    block_id = f"st{article_num}"
    return _make_block(block_id, "article", heading, paragraphs, pub_date, norm_id)


# ─────────────────────────────────────────────
# RadaTextParser
# ─────────────────────────────────────────────


class RadaTextParser(TextParser):
    """Parse Ukrainian law plain text into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse ``.txt`` content into a list of Block objects."""
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("cp1251", errors="replace")

        lines = text.splitlines()
        if not lines:
            return []

        blocks: list[Block] = []
        block_index = 0

        # Preamble lines (before any structural element)
        preamble_lines: list[str] = []
        in_preamble = True

        # Current article state
        cur_article_num: str | None = None
        cur_article_title: str = ""
        cur_body: list[str] = []

        # Extract publication date from text content.  The pipeline uses the
        # Version.publication_date from blocks for commit author dates, so this
        # must be as accurate as possible.  Falls back to 1991-08-24 for ~782
        # docs (short decrees, appendix-only, Soviet-era) whose text has no
        # parseable date.  A pipeline-level hook passing metadata.publication_date
        # would fix these (see Portugal's parse_text_with_date pattern).
        pub_date = _extract_date_from_text(text) or date(1991, 8, 24)
        norm_id = ""

        # Track multi-line annotation blocks {... \n ... \n ...}
        in_annotation = False

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Skip multi-line annotation blocks
            if stripped.startswith("{") and stripped.endswith("}"):
                continue  # single-line annotation
            if stripped.startswith("{"):
                in_annotation = True
                continue
            if in_annotation:
                if stripped.endswith("}"):
                    in_annotation = False
                continue

            # Check structural patterns
            m_article = _ARTICLE_RE.match(stripped)
            m_rozdil = _ROZDIL_RE.match(stripped)
            m_hlava = _HLAVA_RE.match(stripped)
            m_dodatok = _DODATOK_RE.match(stripped)

            if m_article:
                # Flush previous article
                if cur_article_num is not None:
                    blocks.append(
                        _flush_article(
                            cur_article_num,
                            cur_article_title,
                            cur_body,
                            pub_date,
                            norm_id,
                            block_index,
                        )
                    )
                    block_index += 1
                elif in_preamble and preamble_lines:
                    # Flush preamble
                    paras = [
                        Paragraph(css_class="parrafo", text=ln)
                        for ln in preamble_lines
                        if not _is_annotation(ln)
                    ]
                    if paras:
                        blocks.append(
                            _make_block(
                                "preamble",
                                "preamble",
                                "Преамбула",
                                paras,
                                pub_date,
                                norm_id,
                            )
                        )
                        block_index += 1
                    in_preamble = False

                cur_article_num = m_article.group(1)
                cur_article_title = m_article.group(2).strip()
                cur_body = []
                in_preamble = False

            elif m_rozdil:
                # Flush current article first
                if cur_article_num is not None:
                    blocks.append(
                        _flush_article(
                            cur_article_num,
                            cur_article_title,
                            cur_body,
                            pub_date,
                            norm_id,
                            block_index,
                        )
                    )
                    block_index += 1
                    cur_article_num = None
                    cur_body = []
                elif in_preamble and preamble_lines:
                    paras = [
                        Paragraph(css_class="parrafo", text=ln)
                        for ln in preamble_lines
                        if not _is_annotation(ln)
                    ]
                    if paras:
                        blocks.append(
                            _make_block(
                                "preamble",
                                "preamble",
                                "Преамбула",
                                paras,
                                pub_date,
                                norm_id,
                            )
                        )
                        block_index += 1
                    in_preamble = False

                # The next line may be the chapter title in UPPERCASE
                heading = stripped
                paras = [Paragraph(css_class="titulo_tit", text=heading)]
                blocks.append(
                    _make_block(
                        f"rozdil-{block_index}",
                        "chapter",
                        heading,
                        paras,
                        pub_date,
                        norm_id,
                    )
                )
                block_index += 1

            elif m_hlava:
                if cur_article_num is not None:
                    blocks.append(
                        _flush_article(
                            cur_article_num,
                            cur_article_title,
                            cur_body,
                            pub_date,
                            norm_id,
                            block_index,
                        )
                    )
                    block_index += 1
                    cur_article_num = None
                    cur_body = []

                heading = stripped
                paras = [Paragraph(css_class="capitulo_tit", text=heading)]
                blocks.append(
                    _make_block(
                        f"hlava-{block_index}",
                        "chapter",
                        heading,
                        paras,
                        pub_date,
                        norm_id,
                    )
                )
                block_index += 1

            elif m_dodatok:
                if cur_article_num is not None:
                    blocks.append(
                        _flush_article(
                            cur_article_num,
                            cur_article_title,
                            cur_body,
                            pub_date,
                            norm_id,
                            block_index,
                        )
                    )
                    block_index += 1
                    cur_article_num = None
                    cur_body = []

                heading = stripped
                paras = [Paragraph(css_class="capitulo_tit", text=heading)]
                blocks.append(
                    _make_block(
                        f"dodatok-{block_index}",
                        "annex",
                        heading,
                        paras,
                        pub_date,
                        norm_id,
                    )
                )
                block_index += 1

            elif _is_signature(stripped):
                # Skip signature lines at end of document
                continue

            elif cur_article_num is not None:
                # Inside an article — accumulate body lines
                cur_body.append(stripped)

            elif in_preamble:
                preamble_lines.append(stripped)

        # Flush final article
        if cur_article_num is not None:
            blocks.append(
                _flush_article(
                    cur_article_num,
                    cur_article_title,
                    cur_body,
                    pub_date,
                    norm_id,
                    block_index,
                )
            )

        # If no structural elements found at all, make a single text block
        if not blocks and preamble_lines:
            paras = [
                Paragraph(css_class="parrafo", text=ln)
                for ln in preamble_lines
                if not _is_annotation(ln)
            ]
            if paras:
                blocks.append(_make_block("text-0", "text", "Текст", paras, pub_date, norm_id))

        return blocks

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline from amendment annotations in the text.

        Ukrainian laws include editorial annotations listing every amending
        law with its date, e.g.:

            {Із змінами, внесеними згідно із Законами
            № 2222-IV від 08.12.2004, ВВР, 2005, № 2, ст.44
            № 2952-VI від 01.02.2011, ВВР, 2011, № 10, ст.68}

        Also inline per-article annotations like:
            {Статтю 1 доповнено абзацом згідно із Законом № 4441-VI від 23.02.2012}

        We only extract from annotations that mention "згідно із Закон"
        (amended according to Law), filtering out Constitutional Court
        interpretation references ("Офіційне тлумачення", "Рішенн").

        Returns Reform objects sorted chronologically, deduplicated by
        (source_law, date) pairs.
        """
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("cp1251", errors="replace")

        # Collect all {annotation} blocks, including multi-line ones
        annotation_blocks: list[str] = []
        in_block = False
        current: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("{"):
                in_block = True
                current = [stripped]
                if stripped.endswith("}"):
                    annotation_blocks.append(" ".join(current))
                    in_block = False
            elif in_block:
                current.append(stripped)
                if stripped.endswith("}"):
                    annotation_blocks.append(" ".join(current))
                    in_block = False

        seen: set[tuple[str, date]] = set()
        reforms: list[Reform] = []

        for block in annotation_blocks:
            # Only process blocks that reference amending laws
            if "згідно із Закон" not in block and "редакції Закону" not in block:
                continue
            for m in _AMENDMENT_REF_RE.finditer(block):
                law_num = m.group(1)
                try:
                    d = date(int(m.group(4)), int(m.group(3)), int(m.group(2)))
                except ValueError:
                    continue
                key = (law_num, d)
                if key not in seen:
                    seen.add(key)
                    reforms.append(Reform(date=d, norm_id=law_num, affected_blocks=()))

        reforms.sort(key=lambda r: r.date)
        return reforms


# ─────────────────────────────────────────────
# RadaMetadataParser
# ─────────────────────────────────────────────

_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")


def _parse_dotted_date(text: str) -> date | None:
    """Parse ``DD.MM.YYYY`` → date."""
    m = _DATE_RE.search(text)
    if m:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    return None


def _parse_rfc2822_date(text: str) -> date | None:
    """Parse RFC 2822 date like ``Fri, 28 Jun 1996 06:45:00 GMT`` → date."""
    try:
        dt = datetime.strptime(text.strip(), "%a, %d %b %Y %H:%M:%S %Z")
        return dt.date()
    except (ValueError, TypeError):
        return None


def _resolve_rank(types_str: str) -> Rank:
    """Determine the rank from the Types meta content.

    Types is comma-separated, e.g. ``"Конституція України, Конституція, Закон"``.
    We try each type against RANK_MAP and return the first match.
    """
    for raw_type in types_str.split(","):
        t = raw_type.strip()
        if t in RANK_MAP:
            return Rank(RANK_MAP[t])
    return Rank("zakon")


class RadaMetadataParser(MetadataParser):
    """Parse metadata from the card JSON endpoint or .xml fallback.

    Accepts either:
    - Card JSON bytes (from /laws/card/{nreg}.json) — preferred
    - HTML bytes (from /laws/show/{nreg}.xml) — fallback
    """

    # ── Status codes from card JSON ──
    _STATUS_MAP: dict[int, NormStatus] = {
        5: NormStatus.IN_FORCE,  # чинний
        3: NormStatus.REPEALED,  # нечинний / втратив чинність
    }

    # ── Type codes from card JSON (typ field) ──
    _TYPE_MAP: dict[int, str] = {
        1: "zakon",
        2: "ukaz",
        3: "postanova",
        4: "rozporiadzhennia",
        5: "dekret",
    }

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        # Try card JSON first
        try:
            card = json.loads(data)
            if isinstance(card, dict) and "nazva" in card:
                return self._parse_card(card, norm_id)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # Fallback to XML/HTML parsing
        return self._parse_xml(data, norm_id)

    def _parse_card(self, card: dict, norm_id: str) -> NormMetadata:
        """Parse structured card JSON into NormMetadata."""
        title = card.get("nazva", "")
        nreg = card.get("nreg", norm_id)

        # Publication date from orgdat (YYYYMMDD int)
        orgdat = card.get("orgdat", 0)
        pub_date = _int_to_date(orgdat) or date(2000, 1, 1)

        # Last modified from datred (latest edition date)
        datred = card.get("datred", 0)
        last_modified = _int_to_date(datred)

        # Status
        status_code = card.get("status", 5)
        status = self._STATUS_MAP.get(status_code, NormStatus.IN_FORCE)

        # Rank — try typ field, then fall back to klasy
        typ = card.get("typ", 0)
        rank = Rank(self._TYPE_MAP.get(typ, "zakon"))

        # Department from organs field (format: "orgid:date:number")
        organs_str = card.get("organs", "")
        department = _resolve_organ(organs_str)

        # Official number
        official_number = card.get("n_vlas") or card.get("orgnum", "")

        source = f"https://zakon.rada.gov.ua/laws/show/{norm_id}"
        identifier = nreg_to_identifier(nreg)

        # Extra — capture everything useful
        extra: list[tuple[str, str]] = []
        if official_number:
            extra.append(("official_number", official_number))
        if nreg:
            extra.append(("nreg", nreg))
        extra.append(("edition_count", str(card.get("edcnt", 0))))
        publics = card.get("publics", "")
        if publics:
            extra.append(("publications", publics))

        return NormMetadata(
            title=title,
            short_title=title,
            identifier=identifier,
            country="ua",
            rank=rank,
            publication_date=pub_date,
            status=status,
            department=department,
            source=source,
            last_modified=last_modified,
            extra=tuple(extra),
        )

    def _parse_xml(self, data: bytes, norm_id: str) -> NormMetadata:
        """Fallback: parse metadata from .xml endpoint (HTML with <meta> tags)."""
        doc = lxml.html.fromstring(data)

        def meta(name: str) -> str:
            els = doc.xpath(f'//meta[@name="{name}"]/@content')
            return els[0] if els else ""

        title_el = doc.xpath("//title/text()")
        title = title_el[0].strip() if title_el else ""

        types = meta("Types")
        organs = meta("Organs")
        stan = meta("Stan")
        dates = meta("Dates")
        numbers = meta("Numbers")
        doc_date = meta("DocumentDate")

        pub_date = _parse_dotted_date(dates)
        if pub_date is None:
            pub_date = _parse_rfc2822_date(meta("Created")) or date(2000, 1, 1)

        last_modified = _parse_rfc2822_date(doc_date)
        status = STAN_MAP.get(stan.strip(), NormStatus.IN_FORCE)
        rank = _resolve_rank(types)
        source = f"https://zakon.rada.gov.ua/laws/show/{norm_id}"

        nreg = meta("Nreg")
        description = meta("Description")

        extra: list[tuple[str, str]] = []
        if numbers:
            extra.append(("official_number", numbers))
        if nreg:
            extra.append(("nreg", nreg))
        if types:
            extra.append(("types", types))
        if stan:
            extra.append(("stan", stan))
        if description:
            extra.append(("description", description))

        identifier = nreg_to_identifier(norm_id)

        return NormMetadata(
            title=title,
            short_title=title,
            identifier=identifier,
            country="ua",
            rank=rank,
            publication_date=pub_date,
            status=status,
            department=organs,
            source=source,
            last_modified=last_modified,
            extra=tuple(extra),
        )
