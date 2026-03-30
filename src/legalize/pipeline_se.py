"""Pipeline for Sweden — fetch, commit and bootstrap from Riksdagen API.

Uses Riksdagen open data API (data.riksdagen.se) for statute text and metadata,
and rkrattsbaser.gov.se/sfsr for amendment history.

Flow:
  1. fetch-se --discover     → Paginate catalog + fetch each law in one pass
  2. fetch-se SFS_NUMBER     → Fetch single law
  3. commit --all            → pipeline.commit_all() (generic, reads JSON)
  4. ingest                  → web.ingest (generic, JSON → DB + Blob)
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from urllib.parse import quote

import requests

from legalize.config import Config
from legalize.fetcher.parser_se import SwedishMetadataParser, SwedishTextParser
from legalize.models import NormaCompleta
from legalize.storage import save_structured_json
from legalize.transformer.xml_parser import extract_reforms

logger = logging.getLogger(__name__)

_text_parser = SwedishTextParser()
_meta_parser = SwedishMetadataParser()

_UA = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)"
_RIKSDAGEN_LIST = "https://data.riksdagen.se/dokumentlista"
_RIKSDAGEN_DOC = "https://data.riksdagen.se/dokument"
_SFSR_URL = "https://rkrattsbaser.gov.se/sfsr"


def _safe_id(sfs_number: str) -> str:
    """Normalize SFS number for filesystem: '1962:700' → 'SFS-1962-700'."""
    return f"SFS-{sfs_number.replace(':', '-').replace(' ', '_')}"


def _json_path(data_dir: str, sfs_number: str) -> Path:
    return Path(data_dir) / "json" / f"{_safe_id(sfs_number)}.json"


def _fetch_url(url: str, timeout: int = 30) -> requests.Response:
    """Fetch URL with retry on failure."""
    for attempt in range(5):
        try:
            r = requests.get(url, headers={"User-Agent": _UA}, timeout=timeout)
            if r.status_code in (429, 503):
                wait = (attempt + 1) * 2
                logger.warning("HTTP %d on %s, retrying in %ds", r.status_code, url[:80], wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout):
            wait = (attempt + 1) * 3
            logger.warning("Connection error on %s, retrying in %ds", url[:80], wait, exc_info=True)
            time.sleep(wait)
    # Last attempt — let it raise
    return requests.get(url, headers={"User-Agent": _UA}, timeout=timeout)


def fetch_one_se(config: Config, sfs_number: str, force: bool = False) -> NormaCompleta | None:
    """Fetch and parse ONE Swedish statute from Riksdagen API."""
    jp = _json_path(config.data_dir, sfs_number)
    if jp.exists() and not force:
        print(f"  {sfs_number} already processed, skipping", flush=True)
        return _load_norma_from_json(jp)

    try:
        print(f"  Processing {sfs_number}...", flush=True)

        # Search for dok_id
        search_url = f"{_RIKSDAGEN_LIST}/?sok={quote(sfs_number)}&doktyp=sfs&format=json&utformat=json"
        search_data = _fetch_url(search_url).json()
        docs = search_data.get("dokumentlista", {}).get("dokument", [])
        if not docs:
            logger.warning("No document found for %s", sfs_number)
            return None

        dok_id = docs[0].get("dok_id", "")

        # Fetch full document
        doc_data = _fetch_url(f"{_RIKSDAGEN_DOC}/{dok_id}.json").content
        metadata = _meta_parser.parse(doc_data, sfs_number)
        bloques = _text_parser.parse_texto(doc_data)

        # SFSR amendment register
        try:
            sfsr_data = _fetch_url(f"{_SFSR_URL}?bet={quote(sfs_number)}").content
            reforms = _text_parser.extract_reforms_from_sfsr(sfsr_data)
        except Exception:
            reforms = extract_reforms(bloques)

        norma = NormaCompleta(
            metadata=metadata, bloques=tuple(bloques), reforms=tuple(reforms),
        )
        save_structured_json(config.data_dir, norma)

        print(f"  ✓ {metadata.titulo_corto}: {len(bloques)} blocks, {len(reforms)} reforms", flush=True)
        return norma

    except Exception:
        logger.error("Error processing %s", sfs_number, exc_info=True)
        print(f"  ✗ Error processing {sfs_number}", flush=True)
        return None


def fetch_all_se(config: Config, force: bool = False) -> list[str]:
    """Discover and fetch all Swedish statutes in one pass.

    Paginates the Riksdagen catalog. For each base statute found,
    fetches text + SFSR and saves JSON. Skips existing JSONs.
    """
    print("Discover + fetch — scanning Riksdagen catalog...", flush=True)

    page = 1
    discovered = 0
    fetched = 0
    skipped = 0
    errors = 0
    fetched_ids = []
    t0 = time.time()

    while True:
        url = f"{_RIKSDAGEN_LIST}/?doktyp=sfs&sort=datum&sortorder=asc&format=json&utformat=json&p={page}"
        try:
            r = _fetch_url(url, timeout=15)
            data = r.json()
        except Exception as e:
            logger.error("Catalog page %d failed: %s", page, e)
            break

        docs = data.get("dokumentlista", {}).get("dokument", [])
        has_more = bool(data.get("dokumentlista", {}).get("@nasta_sida"))

        if not docs:
            break

        for doc in docs:
            title = doc.get("titel", "")
            sfs = doc.get("beteckning", "")
            if not sfs:
                continue

            # Skip amendments
            title_lower = title.lower()
            if "ändring i" in title_lower or "upphävande av" in title_lower:
                continue

            discovered += 1
            jp = _json_path(config.data_dir, sfs)

            if jp.exists() and not force:
                skipped += 1
                continue

            # Fetch this law inline
            try:
                dok_id = doc.get("dok_id", "")
                doc_data = _fetch_url(f"{_RIKSDAGEN_DOC}/{dok_id}.json").content
                metadata = _meta_parser.parse(doc_data, sfs)
                bloques = _text_parser.parse_texto(doc_data)

                try:
                    sfsr_data = _fetch_url(f"{_SFSR_URL}?bet={quote(sfs)}").content
                    reforms = _text_parser.extract_reforms_from_sfsr(sfsr_data)
                except Exception:
                    reforms = extract_reforms(bloques)

                norma = NormaCompleta(
                    metadata=metadata, bloques=tuple(bloques), reforms=tuple(reforms),
                )
                save_structured_json(config.data_dir, norma)
                fetched += 1
                fetched_ids.append(sfs)
            except Exception:
                errors += 1

        if discovered % 200 == 0 and discovered > 0:
            elapsed = time.time() - t0
            print(
                f"  [p{page}] {discovered} discovered, {fetched} new, "
                f"{skipped} skipped, {errors} errors ({elapsed:.0f}s)",
                flush=True,
            )

        if not has_more:
            break
        page += 1

    elapsed = time.time() - t0
    print(
        f"Done: {discovered} discovered, {fetched} new, "
        f"{skipped} skipped, {errors} errors ({elapsed / 60:.1f}min)",
        flush=True,
    )
    return fetched_ids


def bootstrap_se(config: Config, dry_run: bool = False) -> int:
    """Full bootstrap for Sweden: discover + fetch + commit."""
    from legalize.pipeline import commit_all

    print("Bootstrap Sweden — Riksdagen Open Data", flush=True)
    print(f"  Data dir: {config.data_dir}", flush=True)
    print(f"  Repo output: {config.git.repo_path}", flush=True)

    fetched = fetch_all_se(config, force=False)

    # commit_all processes ALL JSONs in data_dir, not just the newly fetched ones
    print("\nCommit — generating git history...", flush=True)
    total_commits = commit_all(config, dry_run=dry_run)

    print("\n✓ Bootstrap Sweden completed", flush=True)
    print(f"  {len(fetched)} new statutes fetched, {total_commits} commits created", flush=True)

    return total_commits


def _load_norma_from_json(json_path: Path) -> NormaCompleta:
    """Reuse the generic loader from pipeline.py."""
    from legalize.pipeline import _load_norma_from_json as _load
    return _load(json_path)
