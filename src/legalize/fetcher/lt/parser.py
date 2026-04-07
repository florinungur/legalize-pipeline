"""Parser for Lithuanian data.gov.lt metadata (JSON) and text (plain text).

Both metadata and full text come from the data.gov.lt Spinta API.
The tekstas_lt field contains the full law text as plain text with
newline-separated paragraphs.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any

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

# Map Lithuanian act type names (rusis) to rank strings
RUSIS_TO_RANK: dict[str, str] = {
    "Konstitucija": "konstitucija",
    "Konstitucinis įstatymas": "konstitucinis_istatymas",
    "Įstatymas": "istatymas",
    "Kodeksas": "istatymas",
    "Nutarimas": "nutarimas",
    "Įsakymas": "isakymas",
    "Sprendimas": "sprendimas",
    "Potvarkis": "potvarkis",
    "Dekretas": "dekretas",
    "Rezoliucija": "rezoliucija",
}

# Map Lithuanian status values (galioj_busena) to NormStatus
# Only 3 values exist in the DB: galioja (380K), negalioja (89K), neįsigaliojęs (413)
STATUS_MAP: dict[str, NormStatus] = {
    "galioja": NormStatus.IN_FORCE,
    "negalioja": NormStatus.REPEALED,
    "neįsigaliojęs": NormStatus.IN_FORCE,  # not yet in force — treat as in_force
}

# Lithuanian structural element patterns
_ARTICLE_RE = re.compile(r"(?P<num>\d+)\s*straipsnis\b", re.IGNORECASE)
_CHAPTER_RE = re.compile(r"(?P<num>[IVXLCDM]+)\s*(?:skyrius|SKYRIUS)\b", re.IGNORECASE)
_SECTION_RE = re.compile(r"(?P<num>[IVXLCDM]+|\d+)\s*(?:skirsnis|SKIRSNIS)\b", re.IGNORECASE)
_PART_RE = re.compile(r"(?P<num>[IVXLCDM]+|\d+)\s*(?:dalis|DALIS)\b", re.IGNORECASE)


def _parse_date(s: str | None) -> date | None:
    """Parse ISO date string (YYYY-MM-DD)."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, IndexError):
        return None


def _classify_line(text: str) -> str:
    """Classify a text line by Lithuanian structural patterns."""
    if _ARTICLE_RE.search(text):
        return "article_heading"
    if _CHAPTER_RE.search(text):
        return "chapter_heading"
    if _SECTION_RE.search(text):
        return "section_heading"
    if _PART_RE.search(text):
        return "part_heading"
    return "text"


def _text_to_paragraphs(text: str) -> list[Paragraph]:
    """Convert plain text into a list of Paragraph objects.

    The tekstas_lt field from data.gov.lt is plain text with
    newline-separated lines (not HTML).
    """
    paragraphs: list[Paragraph] = []

    for line in text.split("\n"):
        line = line.strip()
        if not line or line == "\xa0":
            continue
        css_class = _classify_line(line)
        paragraphs.append(Paragraph(css_class=css_class, text=line))

    return paragraphs


class TARTextParser(TextParser):
    """Parses plain text from data.gov.lt tekstas_lt into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse data.gov.lt JSON response containing tekstas_lt.

        The input is JSON with _data[0].tekstas_lt containing the full text
        and optionally priimtas (adoption date) for block versioning.
        Groups consecutive paragraphs under article headings into blocks.
        """
        raw = data.decode("utf-8", errors="replace")
        pub_date = date(1900, 1, 1)

        # Handle JSON response from API
        try:
            api_data = json.loads(raw)
            items = api_data.get("_data", [])
            if not items:
                return []
            text = items[0].get("tekstas_lt", "")
            pub_date = _parse_date(items[0].get("priimtas")) or date(1900, 1, 1)
        except (json.JSONDecodeError, KeyError, IndexError):
            # Fall back to treating input as plain text
            text = raw

        if not text or not text.strip():
            return []

        paragraphs = _text_to_paragraphs(text)
        if not paragraphs:
            return []

        return self._parse_paragraphs_to_blocks(paragraphs, pub_date)

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline from Dokumentas text.

        Dokumentas only has the current consolidated text, so this returns
        an empty list. Use parse_suvestine() for historical versions.
        """
        return []

    def parse_suvestine(
        self, suvestine_data: bytes, norm_id: str
    ) -> tuple[list[Block], list[Reform]]:
        """Parse Suvestine (historical versions) into versioned Blocks + Reforms.

        Each Suvestine entry is a full snapshot of the law at a point in time.
        We parse each snapshot into blocks, then merge blocks across versions
        so each Block has multiple Version objects (one per Suvestine entry).

        Returns (blocks, reforms) where blocks have multi-version content
        and reforms is the chronological list of changes.
        """
        raw = json.loads(suvestine_data)
        items = raw.get("_data", [])
        if not items:
            return [], []

        # Parse each version's text into blocks
        snapshots: list[tuple[date, str, list[Block]]] = []
        for item in items:
            version_date = _parse_date(item.get("galioja_nuo"))
            if not version_date:
                continue
            version_id = item.get("suvestines_id", "")
            text = item.get("tekstas_lt", "")
            if not text or not text.strip():
                continue

            paragraphs = _text_to_paragraphs(text)
            if not paragraphs:
                continue

            # Reuse the same block-building logic
            blocks = self._parse_paragraphs_to_blocks(paragraphs, version_date)
            snapshots.append((version_date, version_id, blocks))

        if not snapshots:
            return [], []

        # Build merged blocks: collect all block IDs across versions
        # and create one Block per ID with multiple Versions
        all_block_ids: list[str] = []
        seen_ids: set[str] = set()
        for _, _, blocks in snapshots:
            for block in blocks:
                if block.id not in seen_ids:
                    all_block_ids.append(block.id)
                    seen_ids.add(block.id)

        # For each block ID, collect versions from each snapshot
        merged_blocks: list[Block] = []
        for block_id in all_block_ids:
            versions: list[Version] = []
            block_title = ""
            block_type = "article"

            for snap_date, snap_id, blocks in snapshots:
                for block in blocks:
                    if block.id == block_id:
                        block_title = block_title or block.title
                        block_type = block.block_type
                        versions.append(
                            Version(
                                norm_id=f"{norm_id}:{snap_id}",
                                publication_date=snap_date,
                                effective_date=snap_date,
                                paragraphs=block.versions[0].paragraphs,
                            )
                        )
                        break

            if versions:
                merged_blocks.append(
                    Block(
                        id=block_id,
                        block_type=block_type,
                        title=block_title,
                        versions=tuple(versions),
                    )
                )

        # Build reforms: one per snapshot, with affected_blocks
        reforms: list[Reform] = []
        prev_block_texts: dict[str, str] = {}

        for snap_date, snap_id, blocks in snapshots:
            current_texts: dict[str, str] = {}
            for block in blocks:
                text = "\n".join(p.text for p in block.versions[0].paragraphs)
                current_texts[block.id] = text

            if not prev_block_texts:
                # First version — all blocks are "affected" (bootstrap)
                affected = tuple(b.id for b in blocks)
            else:
                # Find blocks that changed or were added/removed
                changed: list[str] = []
                all_ids = set(current_texts) | set(prev_block_texts)
                for bid in all_ids:
                    if current_texts.get(bid) != prev_block_texts.get(bid):
                        changed.append(bid)
                affected = tuple(changed)

            reforms.append(
                Reform(
                    date=snap_date,
                    norm_id=f"{norm_id}:{snap_id}",
                    affected_blocks=affected,
                )
            )

            prev_block_texts = current_texts

        return merged_blocks, reforms

    def _parse_paragraphs_to_blocks(
        self, paragraphs: list[Paragraph], pub_date: date
    ) -> list[Block]:
        """Parse a flat list of paragraphs into Block objects (shared logic)."""
        blocks: list[Block] = []
        current_id = "full"
        current_title = "Full text"
        current_paragraphs: list[Paragraph] = []
        block_index = 0

        for para in paragraphs:
            if para.css_class == "article_heading":
                if current_paragraphs:
                    blocks.append(
                        self._make_block(current_id, current_title, current_paragraphs, pub_date)
                    )
                match = _ARTICLE_RE.search(para.text)
                num = match.group("num") if match else str(block_index)
                current_id = f"str{num}"
                current_title = para.text
                current_paragraphs = [para]
                block_index += 1
            else:
                current_paragraphs.append(para)

        if current_paragraphs:
            blocks.append(self._make_block(current_id, current_title, current_paragraphs, pub_date))

        return blocks

    @staticmethod
    def _make_block(
        block_id: str,
        title: str,
        paragraphs: list[Paragraph],
        pub_date: date = date(1900, 1, 1),
    ) -> Block:
        """Create a Block with a single version from paragraphs."""
        version = Version(
            norm_id=block_id,
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=tuple(paragraphs),
        )
        return Block(
            id=block_id,
            block_type="article" if block_id != "full" else "full",
            title=title,
            versions=(version,),
        )


class TARMetadataParser(MetadataParser):
    """Parses data.gov.lt Spinta API JSON into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse JSON metadata from data.gov.lt.

        Real API fields:
            dokumento_id → identifier
            pavadinimas → title
            alt_pavadinimas → short_title (fallback to pavadinimas)
            rusis → rank (via RUSIS_TO_RANK mapping)
            galioj_busena → status (via STATUS_MAP)
            priimtas → publication_date
            pakeista → last_modified (latest amendment date)
            isigalioja → last_modified fallback (entry into force)
            negalioja → used for status inference
            priemusi_inst → department
            nuoroda → source URL
        """
        api_data = json.loads(data)
        items = api_data.get("_data", [])

        if not items:
            raise ValueError(f"No metadata found for dokumento_id {norm_id}")

        item = items[0]

        title = item.get("pavadinimas", "").strip()
        short_title = (item.get("alt_pavadinimas") or "").strip() or title
        doc_id = item.get("dokumento_id", norm_id).strip()

        # Rank mapping
        rusis = item.get("rusis", "").strip()
        rank_str = RUSIS_TO_RANK.get(rusis, "kita")

        # Dates
        pub_date = _parse_date(item.get("priimtas")) or date(1900, 1, 1)
        expiry_date = _parse_date(item.get("negalioja"))

        # last_modified: use the latest date from pakeista (amendment dates),
        # falling back to isigalioja (entry into force date)
        pakeista = item.get("pakeista") or ""
        last_mod = None
        if pakeista:
            amendment_dates = [_parse_date(d.strip()) for d in pakeista.split(",")]
            valid_dates = [d for d in amendment_dates if d]
            if valid_dates:
                last_mod = max(valid_dates)
        if not last_mod:
            last_mod = _parse_date(item.get("isigalioja"))

        # Status from galioj_busena
        status_raw = item.get("galioj_busena", "").strip()
        status = STATUS_MAP.get(status_raw, NormStatus.IN_FORCE)
        if expiry_date and not status_raw:
            status = NormStatus.REPEALED

        institution = item.get("priemusi_inst", "").strip()
        source_url = item.get("nuoroda", "").strip()
        if not source_url:
            source_url = f"https://e-tar.lt/portal/lt/legalAct/{doc_id}"

        # Extra fields → frontmatter + DB JSONB
        extra: list[tuple[str, str]] = []
        _extra_map = {
            "tar_kodas": "tar_code",
            "atv_dok_nr": "document_number",
            "paskelbta_tar": "published_in_tar",
            "parengusi_inst": "preparing_institution",
            "dok_grupe": "document_group",
            "dok_busena": "document_status",
            "ratifikuota": "ratification_date",
            "patvirtinta": "approval_date",
            "isigal_sal_lt": "entry_into_force_conditions",
        }
        for api_key, extra_key in _extra_map.items():
            val = item.get(api_key)
            if val is not None and str(val).strip():
                extra.append((extra_key, str(val).strip()))

        # Boolean/numeric flags
        es_flag = item.get("es_teises_aktas")
        if es_flag and int(es_flag) > 0:
            extra.append(("eu_legal_act", "true"))
        if item.get("ar_nacionalinis") is False:
            extra.append(("national", "false"))

        return NormMetadata(
            title=title,
            short_title=short_title,
            identifier=doc_id,
            country="lt",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=institution,
            source=source_url,
            last_modified=last_mod,
            extra=tuple(extra),
        )
