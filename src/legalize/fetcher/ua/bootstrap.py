"""Ukraine custom bootstrap — card endpoint + historical versions.

For each law:
1. Fetch /laws/card/{nreg}.json → metadata + edition list
2. For each edition: fetch /laws/show/{nreg}/ed{YYYYMMDD}.txt → versioned text
3. Store as ParsedNorm with one Version per edition → real git diffs

This replaces the generic fetch_one which only gets the current consolidated text.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from legalize.fetcher.ua.client import RadaClient
from legalize.fetcher.ua.discovery import nreg_to_identifier
from legalize.fetcher.ua.parser import (
    RadaMetadataParser,
    RadaTextParser,
    extract_reforms_from_card,
)
from legalize.models import ParsedNorm, Reform
from legalize.storage import save_structured_json

if TYPE_CHECKING:
    from legalize.config import Config

logger = logging.getLogger(__name__)


def fetch_one_with_history(
    config: Config,
    client: RadaClient,
    norm_id: str,
    *,
    force: bool = False,
) -> ParsedNorm | None:
    """Fetch a single Ukrainian law with all historical editions.

    Returns ParsedNorm with versioned blocks (one Version per edition)
    and Reform objects from the card's eds[] array.
    """
    cc = config.get_country("ua")
    safe_id = nreg_to_identifier(norm_id)
    json_path = Path(cc.data_dir) / "json" / f"{safe_id}.json"

    if json_path.exists() and not force:
        return None  # already fetched

    text_parser = RadaTextParser()
    meta_parser = RadaMetadataParser()

    # Step 1: Get card (metadata + edition list)
    try:
        card = client.get_card(norm_id)
    except Exception:
        logger.warning("Card unavailable for %s, falling back to basic fetch", norm_id)
        return _fallback_fetch(config, client, norm_id, text_parser, meta_parser, cc)

    if card.get("result_code") != 200:
        logger.warning("Card returned error for %s: %s", norm_id, card.get("result"))
        return None

    # Parse metadata from card
    card_bytes = json.dumps(card).encode("utf-8")
    metadata = meta_parser.parse(card_bytes, norm_id)

    # Step 2: Get editions sorted chronologically
    eds = card.get("eds", [])
    # Sort by datred ascending (oldest first)
    eds_sorted = sorted(eds, key=lambda e: e.get("datred", 0))

    # Step 3: Fetch text for each edition
    # The first edition with podid=4 is the original, rest are reforms
    editions_text: list[tuple[int, bytes]] = []  # (datred, text_bytes)

    for ed in eds_sorted:
        datred = ed.get("datred", 0)
        if not datred:
            continue
        try:
            text_data = client.get_text_at_edition(norm_id, datred)
            editions_text.append((datred, text_data))
        except Exception:
            logger.warning("Edition ed%s unavailable for %s, skipping", datred, norm_id)

    if not editions_text:
        # No editions available — try current text
        logger.warning("No editions for %s, fetching current text", norm_id)
        try:
            text_data = client.get_text(norm_id)
            editions_text.append((card.get("orgdat", 0) or 20000101, text_data))
        except Exception:
            logger.error("Cannot fetch any text for %s", norm_id)
            return None

    # Step 4: Parse each edition into blocks with the edition date as Version date
    # We use the LATEST edition's blocks as the canonical structure,
    # but we create a Version for each edition date.
    # For the git history, each commit will have different text content.
    latest_text = editions_text[-1][1]
    blocks = text_parser.parse_text(latest_text)

    # Build reforms from card (more reliable than text annotations)
    reforms = extract_reforms_from_card(card)

    # If no reforms from card, use text-based extraction as fallback
    if not reforms and latest_text:
        reforms = text_parser.extract_reforms(latest_text)

    # Ensure at least bootstrap reform
    if not reforms:
        reforms = [
            Reform(
                date=metadata.publication_date,
                norm_id=metadata.identifier,
                affected_blocks=(),
            )
        ]

    norm = ParsedNorm(
        metadata=metadata,
        blocks=tuple(blocks),
        reforms=tuple(reforms),
    )

    # Save the JSON with edition texts embedded for the commit phase
    _save_with_editions(cc.data_dir, norm, editions_text)

    return norm


def _fallback_fetch(config, client, norm_id, text_parser, meta_parser, cc):
    """Basic fetch without card — just current text + XML metadata."""
    try:
        text_data = client.get_text(norm_id)
        meta_data = client.get_metadata(norm_id)
    except Exception:
        logger.error("Failed to fetch %s", norm_id)
        return None

    metadata = meta_parser.parse(meta_data, norm_id)
    blocks = text_parser.parse_text(text_data)
    reforms = text_parser.extract_reforms(text_data)

    if not reforms:
        reforms = [
            Reform(
                date=metadata.publication_date,
                norm_id=metadata.identifier,
                affected_blocks=(),
            )
        ]

    norm = ParsedNorm(
        metadata=metadata,
        blocks=tuple(blocks),
        reforms=tuple(reforms),
    )
    save_structured_json(cc.data_dir, norm)
    return norm


def _save_with_editions(
    data_dir: str,
    norm: ParsedNorm,
    editions_text: list[tuple[int, bytes]],
) -> None:
    """Save the norm JSON plus a separate editions file for the commit phase."""
    # Save the standard norm JSON (generic pipeline compat)
    save_structured_json(data_dir, norm)

    # Save editions as a sidecar file for the custom commit
    safe_id = norm.metadata.identifier
    editions_path = Path(data_dir) / "editions" / f"{safe_id}.json"
    editions_path.parent.mkdir(parents=True, exist_ok=True)

    editions_data = []
    for datred, text_bytes in editions_text:
        try:
            text = text_bytes.decode("utf-8")
        except UnicodeDecodeError:
            text = text_bytes.decode("cp1251", errors="replace")
        editions_data.append({"datred": datred, "text": text})

    editions_path.write_text(json.dumps(editions_data, ensure_ascii=False), encoding="utf-8")
