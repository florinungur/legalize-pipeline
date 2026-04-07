"""Eelmine chain crawler for Estonian historical versions.

Riigi Teataja publishes each consolidated version of a law as a
separate document with its own ``globaalID``. The HTML page of each
version contains explicit navigation to the previous (``Eelmine``) and
next (``Järgmine``) versions:

    <p class="drop-button">
      <a class="drop-label" href="{prev_gid}">Eelmine</a>  <!-- previous -->
    </p>
    <p class="drop-button">
      <a class="drop-label" href="{next_gid}">Järgmine</a> <!-- next -->
    </p>

All versions in the chain share the same ``terviktekstiGrupiID``.
Each has its own ``kehtivuseAlgus``/``kehtivuseLopp`` (effective date
range), and they never overlap — they form a perfect timeline.

This module:
  - ``extract_eelmine_gid(html)`` — parse a single HTML page
  - ``follow_chain_backwards(client, start_gid)`` — walk Eelmine to the root
  - ``follow_chain_forwards(client, start_gid)`` — walk Järgmine to the tip
  - ``full_history(client, any_gid)`` — complete timeline in chronological order
  - ``HistoricalVersion`` — dataclass with gid + effective_from/until
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from lxml import etree

from legalize.fetcher.ee.parser import (
    _LXML_PARSER,
    _direct_child_text,
    _findone,
    _parse_date,
)

if TYPE_CHECKING:
    from legalize.fetcher.ee.client import RTClient

logger = logging.getLogger(__name__)


# Safety caps to prevent infinite loops on malformed HTML
_MAX_CHAIN_DEPTH = 200


_EELMINE_RE = re.compile(
    r'<a[^>]*class="drop-label"[^>]*href="([^"#?]+)"[^>]*>\s*Eelmine\s*</a>',
    re.IGNORECASE,
)

_JARGMINE_RE = re.compile(
    r'<a[^>]*class="drop-label"[^>]*href="([^"#?]+)"[^>]*>\s*J(?:ä|&auml;)rgmine\s*</a>',
    re.IGNORECASE,
)

_GID_FROM_HREF = re.compile(r"(?:^|/)(?:akt/)?(\d+)$")


@dataclass(frozen=True)
class HistoricalVersion:
    """One point in the history of a law.

    Attributes:
        global_id: the ``globaalID`` of this specific version
        effective_from: ``kehtivuseAlgus`` — when this version took effect
        effective_until: ``kehtivuseLopp`` — when it stopped being current
            (None if this is the currently in-force version)
    """

    global_id: str
    effective_from: date | None
    effective_until: date | None


# ─────────────────────────────────────────────
# HTML parsing
# ─────────────────────────────────────────────


def _extract_gid_from_href(href: str) -> str | None:
    """Parse ``href="127042011002"`` or ``href="/akt/127042011002"`` → ``"127042011002"``."""
    if not href:
        return None
    href = href.strip()
    m = _GID_FROM_HREF.search(href)
    return m.group(1) if m else None


def extract_eelmine_gid(html_text: str) -> str | None:
    """Extract the ``Eelmine`` (previous) globaalID from a RT HTML page.

    Returns None if no previous version exists (i.e. we've reached the root).
    """
    m = _EELMINE_RE.search(html_text)
    if not m:
        return None
    return _extract_gid_from_href(m.group(1))


def extract_jargmine_gid(html_text: str) -> str | None:
    """Extract the ``Järgmine`` (next) globaalID from a RT HTML page.

    Returns None if no newer version exists (i.e. this is the tip of the chain).
    """
    m = _JARGMINE_RE.search(html_text)
    if not m:
        return None
    return _extract_gid_from_href(m.group(1))


# ─────────────────────────────────────────────
# XML date extraction (cheap header-only parse)
# ─────────────────────────────────────────────


def extract_dates_from_xml(xml_bytes: bytes) -> tuple[date | None, date | None]:
    """Extract ``(kehtivuseAlgus, kehtivuseLopp)`` from a RT XML.

    Parses only the ``<metaandmed>`` subtree for speed.
    """
    try:
        root = etree.fromstring(xml_bytes, parser=_LXML_PARSER)
    except etree.XMLSyntaxError as e:
        logger.warning("Malformed XML when extracting dates: %s", e)
        return None, None
    meta = _findone(root, "metaandmed")
    if meta is None:
        return None, None
    keh = _findone(meta, "kehtivus")
    if keh is None:
        return None, None
    start = _parse_date(_direct_child_text(keh, "kehtivuseAlgus"))
    end = _parse_date(_direct_child_text(keh, "kehtivuseLopp"))
    return start, end


# ─────────────────────────────────────────────
# Chain walkers
# ─────────────────────────────────────────────


def _fetch_version(client: "RTClient", gid: str) -> HistoricalVersion | None:
    """Fetch one version's XML and build its HistoricalVersion record."""
    try:
        xml_bytes = client.get_text(gid)
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", gid, e)
        return None
    eff_from, eff_until = extract_dates_from_xml(xml_bytes)
    return HistoricalVersion(
        global_id=gid,
        effective_from=eff_from,
        effective_until=eff_until,
    )


def follow_chain_backwards(
    client: "RTClient",
    start_gid: str,
    max_depth: int = _MAX_CHAIN_DEPTH,
) -> list[HistoricalVersion]:
    """Walk the Eelmine chain from ``start_gid`` backwards to the root.

    Returns a list ordered most-recent-first (``start_gid`` is index 0).
    The walk stops when a page has no ``Eelmine`` link or a cycle is detected.
    """
    chain: list[HistoricalVersion] = []
    seen: set[str] = set()
    current: str | None = start_gid

    for _ in range(max_depth):
        if current is None:
            break
        if current in seen:
            logger.warning("Cycle detected in Eelmine chain at %s", current)
            break
        seen.add(current)

        version = _fetch_version(client, current)
        if version is None:
            break
        chain.append(version)

        try:
            html_bytes = client.get_html(current)
            html_text = html_bytes.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("Failed to fetch HTML for %s: %s", current, e)
            break

        current = extract_eelmine_gid(html_text)

    if len(chain) == max_depth:
        logger.warning(
            "Hit max_depth=%d walking Eelmine from %s — chain may be truncated",
            max_depth,
            start_gid,
        )
    return chain


def follow_chain_forwards(
    client: "RTClient",
    start_gid: str,
    max_depth: int = _MAX_CHAIN_DEPTH,
) -> list[HistoricalVersion]:
    """Walk the Järgmine chain from ``start_gid`` forwards to the tip.

    Returns a list ordered oldest-first (``start_gid`` is index 0).
    Does NOT include ``start_gid`` itself — only newer versions.
    """
    chain: list[HistoricalVersion] = []
    seen: set[str] = {start_gid}
    current: str | None = start_gid

    for _ in range(max_depth):
        if current is None:
            break
        try:
            html_bytes = client.get_html(current)
            html_text = html_bytes.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("Failed to fetch HTML for %s: %s", current, e)
            break

        nxt = extract_jargmine_gid(html_text)
        if nxt is None or nxt in seen:
            break
        seen.add(nxt)

        version = _fetch_version(client, nxt)
        if version is None:
            break
        chain.append(version)
        current = nxt

    return chain


def full_history(
    client: "RTClient",
    any_gid: str,
) -> list[HistoricalVersion]:
    """Reconstruct the full version timeline from any known globaalID.

    Walks ``any_gid`` backwards via Eelmine AND forwards via Järgmine,
    then returns a combined list sorted CHRONOLOGICALLY (oldest first).

    This is robust: you can start from any version of the law (even the
    current one, the oldest one, or an intermediate one) and get the
    complete history.
    """
    backwards = follow_chain_backwards(client, any_gid)
    # backwards is most-recent-first: [any_gid, prev, prev_of_prev, ...]
    # So we reverse to get oldest-first excluding any_gid
    oldest_first = list(reversed(backwards))

    # Now walk forwards from any_gid (does not include any_gid itself)
    forwards = follow_chain_forwards(client, any_gid)

    return oldest_first + forwards


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────


def canonical_filename_id(history: list[HistoricalVersion]) -> str | None:
    """Pick the canonical filename ID for a chain: the EARLIEST globaalID.

    Using the oldest ID as the repo filename guarantees stable filenames
    across reruns: new reforms add commits to the same file, they never
    rename it.
    """
    if not history:
        return None
    return history[0].global_id


def validate_chain_contiguity(history: list[HistoricalVersion]) -> list[str]:
    """Return a list of human-readable warnings about date gaps/overlaps.

    A valid chain has each version's ``effective_from`` == previous
    version's ``effective_until + 1 day`` (or very close to it). Gaps
    or overlaps may indicate a missing version.
    """
    from datetime import timedelta

    warnings: list[str] = []
    for i in range(1, len(history)):
        prev = history[i - 1]
        curr = history[i]
        if prev.effective_until is None or curr.effective_from is None:
            continue
        expected = prev.effective_until + timedelta(days=1)
        diff = (curr.effective_from - expected).days
        if diff > 1:
            warnings.append(
                f"gap of {diff} days between {prev.global_id} "
                f"(ends {prev.effective_until}) and {curr.global_id} "
                f"(starts {curr.effective_from})"
            )
        elif diff < 0:
            warnings.append(
                f"overlap of {-diff} days between {prev.global_id} "
                f"(ends {prev.effective_until}) and {curr.global_id} "
                f"(starts {curr.effective_from})"
            )
    return warnings
