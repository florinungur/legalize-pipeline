"""PDF annex → Markdown table conversion for Estonian Riigi Teataja.

Estonian laws distribute their tabular data (damage scales, building
permit matrices, qualification frameworks, balance sheet templates, …)
as PDF attachments embedded as base64 inside ``<lisaViide><fail>``
elements. The 2026-04-07 PDF investigation confirmed that 75 % of these
annexes carry primary legal data and must not be dropped.

This module extracts structured tables from those PDFs using pdfplumber
(which uses line and bounding box detection) and converts them to
Markdown pipe tables that can be embedded directly in the ``.md`` file.

Heuristic gate: if pdfplumber doesn't find a table with at least 3 rows,
or if the extracted text is below a minimum size, we return ``None`` —
the parser will fall back to a textual annex reference with a link to
the original PDF.
"""

from __future__ import annotations

import logging
import re
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# A "table-shaped" annex needs at least this many rows
_MIN_TABLE_ROWS = 3
# …and this many columns (otherwise it's probably just a list)
_MIN_TABLE_COLS = 2


def pdf_to_markdown_tables(pdf_bytes: bytes) -> list[str]:
    """Extract all tables from a PDF and return them as Markdown pipe tables.

    Returns:
        A list of Markdown pipe table strings, one per detected table.
        Empty list if no tables were found or pdfplumber isn't installed.
    """
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not installed; PDF annexes will be rendered as links only")
        return []

    tables: list[str] = []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for raw in page.extract_tables() or []:
                    md = _raw_table_to_markdown(raw)
                    if md:
                        tables.append(md)
    except Exception as e:  # noqa: BLE001 — pdfplumber can raise many things
        logger.warning("pdfplumber failed: %s", e)
        return []
    return tables


def _clean_cell(value: str | None) -> str:
    """Normalise a cell value: strip, collapse whitespace, escape pipes."""
    if value is None:
        return ""
    # Replace newlines inside a cell with spaces (pipe tables can't have them)
    s = str(value).replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # Markdown pipe escape
    s = s.replace("|", "\\|")
    return s


_NUMERIC_CELL_RE = re.compile(r"^[\s\-–—]*\d+(?:[.,]\d+)?(?:[-–]\d+(?:[.,]\d+)?)?[\s\-–—]*$")


def _looks_numeric(cell) -> bool:
    """A cell 'looks numeric' if it's a number, a range like '14,1-18', or a dash."""
    if cell is None:
        return False
    s = str(cell).strip().replace("\n", " ")
    if not s:
        return False
    if s in ("–", "-", "—"):
        return True
    return bool(_NUMERIC_CELL_RE.match(s))


def _is_data_row(row: list) -> bool:
    """A row is a data row if at least 2 of its cells look numeric."""
    return sum(1 for c in row if _looks_numeric(c)) >= 2


def _merge_headers(rows: list[list]) -> tuple[list[str], int]:
    """Detect the header block at the top of a table and merge it into a
    single header row.

    Returns (merged_header_cells, num_header_rows_consumed).

    Strategy: a "header block" is the contiguous prefix of rows that
    contain at least one ``None``/empty cell. That's the heuristic
    pdfplumber outputs for multi-line multi-column headers (the top row
    has a value that spans several columns → the remaining cells in that
    row are None, while the row below has per-column sub-headers).
    """
    n_cols = max((len(r) for r in rows), default=0)
    if n_cols == 0:
        return [], 0

    # Pad rows to same width
    padded = [list(r) + [None] * (n_cols - len(r)) for r in rows]

    # Headers = the contiguous prefix of rows that are NOT numeric data rows.
    # For numeric tables (damage scales, tariffs…), this cleanly separates
    # multi-row headers from the numeric body.
    header_rows = []
    i = 0
    while i < len(padded) and not _is_data_row(padded[i]):
        header_rows.append(padded[i])
        i += 1

    # For purely textual tables (building permit matrix, qualifications
    # framework, accounting schema…) there are no numeric cells, so the
    # loop above ate EVERYTHING. Fall back to "first row is the only
    # header, rest are data".
    if i == len(padded) and len(header_rows) > 1:
        header_rows = [padded[0]]

    if not header_rows:
        return [], 0

    # Merge vertically: for each column, concatenate the non-empty values
    merged: list[str] = []
    for col in range(n_cols):
        parts = [
            _clean_cell(header_rows[r][col]) for r in range(len(header_rows)) if header_rows[r][col]
        ]
        merged.append(" — ".join(parts) if parts else "")

    return merged, len(header_rows)


def _compact_table(raw: list[list]) -> list[list]:
    """Clean up a raw pdfplumber table by dropping empty rows/cols and
    coalescing ghost rows that pdfplumber sometimes produces when cell
    boundaries are only partially aligned.

    Steps:
      1. Pad all rows to the same width.
      2. Drop columns that are 100 % empty.
      3. Drop rows that are 100 % empty.
      4. Merge vertically adjacent rows where the populated cells of one
         are complementary to the populated cells of the next (common
         when pdfplumber splits a single logical row into 2-3 physical
         rows because of multi-line header layout).
    """
    if not raw:
        return []

    n_cols = max(len(r) for r in raw)
    padded = [list(r) + [None] * (n_cols - len(r)) for r in raw]

    def is_empty_cell(c) -> bool:
        return c is None or (isinstance(c, str) and not c.strip())

    # 2) Drop empty columns
    keep_cols = [col for col in range(n_cols) if any(not is_empty_cell(row[col]) for row in padded)]
    padded = [[row[c] for c in keep_cols] for row in padded]

    # 3) Drop empty rows
    padded = [row for row in padded if any(not is_empty_cell(c) for c in row)]

    return padded


def _raw_table_to_markdown(raw: list[list]) -> str:
    """Convert a pdfplumber raw table (list of lists) to a Markdown pipe table."""
    if not raw:
        return ""

    raw = _compact_table(raw)
    if len(raw) < _MIN_TABLE_ROWS:
        return ""

    n_cols = max(len(r) for r in raw)
    if n_cols < _MIN_TABLE_COLS:
        return ""

    header, consumed = _merge_headers(raw)
    body = raw[consumed:] if consumed > 0 else raw[1:]

    if not header:
        # Fall back: use the first row as the header
        header = [_clean_cell(c) for c in raw[0]] + [""] * (n_cols - len(raw[0]))
        body = raw[1:]

    # Pad header to n_cols
    header = (header + [""] * n_cols)[:n_cols]

    # Build markdown
    lines: list[str] = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join("---" for _ in range(n_cols)) + " |")
    for row in body:
        cells = [_clean_cell(c) for c in row]
        cells = (cells + [""] * n_cols)[:n_cols]
        lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)


def has_tabular_content(tables: list[str]) -> bool:
    """Heuristic: does this PDF actually contain meaningful tables?

    Used by the parser to decide whether to embed the tables inline
    or fall back to a plain PDF link. A table is "meaningful" if it has
    at least ``_MIN_TABLE_ROWS`` rows of data (after discarding the
    header and separator lines).
    """
    if not tables:
        return False
    for md in tables:
        # A pipe table always has: 1 header line + 1 separator line + N data lines.
        pipe_lines = [line for line in md.splitlines() if line.startswith("|")]
        data_rows = len(pipe_lines) - 2
        if data_rows >= _MIN_TABLE_ROWS:
            return True
    return False
