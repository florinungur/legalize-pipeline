"""Norm discovery in the BOE catalog.

The BOE API does not expose a directly filterable catalog endpoint.
For bootstrap, we use two strategies:
1. Fixed norms list (normas_fijas in config): always processed
2. Summary sweep: iterate summaries by date to discover new norms

For Phase 2, the bootstrap works primarily with normas_fijas.
Automatic discovery via summaries is used in the daily flow.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, timedelta

from legalize.config import Config
from legalize.fetcher.es.client import BOEClient
from legalize.fetcher.es.sumario import parse_summary

logger = logging.getLogger(__name__)


def iter_fixed_norms(config: Config) -> Iterator[str]:
    """Generates BOE IDs from the fixed norms list in config.

    Fixed norms are those always included in bootstrap,
    regardless of the scope dates.
    """
    for boe_id in config.scope.normas_fijas:
        yield boe_id


def iter_norms_from_summaries(
    client: BOEClient,
    config: Config,
    start_date: date,
    end_date: date,
) -> Iterator[str]:
    """Discovers BOE IDs by iterating daily summaries over a date range.

    Useful for bootstrap when all legislation published in a period
    should be included, not just fixed norms.

    Summaries are published Monday through Saturday only.

    Args:
        client: BOE HTTP client.
        config: Configuration (for scope).
        start_date: Start date (inclusive).
        end_date: End date (inclusive).

    Yields:
        BOE IDs of dispositions within scope.
    """
    seen: set[str] = set()
    current = start_date

    while current <= end_date:
        # No BOE on Sundays
        if current.weekday() == 6:
            current += timedelta(days=1)
            continue

        try:
            xml_data = client.get_sumario(current)
            dispositions = parse_summary(xml_data, config.scope)

            for disp in dispositions:
                if disp.id_boe not in seen:
                    seen.add(disp.id_boe)
                    yield disp.id_boe

        except Exception:
            logger.warning("Error processing summary for %s, continuing", current, exc_info=True)

        current += timedelta(days=1)
