"""Czech Republic bootstrap with full version history.

The e-Sbírka API provides point-in-time access to every law version:
each version is accessible by appending the effective date to the
staleUrl (/sb/{year}/{number}/{date}). This bootstrap:

  1. Discovers all laws via paginated search.
  2. For each law (parallelized):
     - Fetches metadata → extracts amendment list from citation text.
     - Fetches each amendment's metadata → gets effective date.
     - Fetches the law's text at each historical date.
     - Renders each version to Markdown.
  3. Commits versions sequentially (oldest first per law), with
     GIT_AUTHOR_DATE set to the version's effective date.

This module is discovered automatically by
:func:`legalize.pipeline.generic_bootstrap` via the optional
``fetcher/{country}/bootstrap.py`` hook.
"""

from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date

import requests

from rich.console import Console

from legalize.committer.git_ops import GitRepo
from legalize.committer.message import build_commit_info
from legalize.config import Config
from legalize.fetcher.cz.client import ESbirkaClient
from legalize.fetcher.cz.discovery import ESbirkaDiscovery
from legalize.fetcher.cz.parser import ESbirkaMetadataParser, ESbirkaTextParser
from legalize.models import CommitType, NormMetadata, Reform
from legalize.transformer.markdown import render_norm_at_date
from legalize.transformer.slug import norm_to_filepath

console = Console()
logger = logging.getLogger(__name__)

# Regex for extracting amendment numbers from full citation text.
_AMENDMENT_RE = re.compile(r"č\.\s*(\d+)/(\d+)\s*Sb\.")


@dataclass
class _VersionSnapshot:
    """One version of a law, ready to commit."""

    effective_date: date
    markdown: str
    source_amendment: str  # e.g. "347/1997 Sb." or "original"


@dataclass
class _PreparedLaw:
    """A law with all its versions fetched and rendered."""

    stale_url: str
    metadata: NormMetadata
    file_path: str
    versions: list[_VersionSnapshot] = field(default_factory=list)
    error: str | None = None


def bootstrap(
    config: Config,
    dry_run: bool = False,
    limit: int | None = None,
    workers: int | None = None,
) -> int:
    """CZ bootstrap: discover → parallel fetch versions → sequential commits.

    Returns the total number of commits created.
    """
    cc = config.get_country("cz")
    if workers is None:
        workers = getattr(cc, "max_workers", 4) or 4

    console.print("[bold]Bootstrap CZ — e-Sbírka with version history[/bold]\n")
    console.print(f"  Repo: {cc.repo_path}")
    console.print(f"  Workers: {workers}\n")

    # 1. Discovery
    console.print("[bold]Phase 1: Discovery[/bold]")
    with ESbirkaClient.create(cc) as client:
        discovery = ESbirkaDiscovery()
        all_ids: list[str] = []
        for stale_url in discovery.discover_all(client):
            all_ids.append(stale_url)
            if limit and len(all_ids) >= limit:
                break

    console.print(f"  Found {len(all_ids)} laws\n")

    if not all_ids:
        return 0

    # 2. Parallel fetch + version reconstruction
    console.print(f"[bold]Phase 2: Fetch versions ({workers} workers)[/bold]")
    start_time = time.monotonic()
    prepared: list[_PreparedLaw] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_prepare_one_law, cc, stale_url): stale_url for stale_url in all_ids}

        for i, future in enumerate(as_completed(futures), 1):
            stale_url = futures[future]
            try:
                law = future.result()
                if law.error:
                    errors += 1
                    logger.warning("Error preparing %s: %s", stale_url, law.error)
                else:
                    prepared.append(law)
            except Exception as e:
                errors += 1
                logger.error("Exception preparing %s: %s", stale_url, e)

            if i % 100 == 0:
                elapsed = time.monotonic() - start_time
                rate = i / elapsed
                console.print(f"  {i}/{len(all_ids)} laws fetched ({rate:.1f}/s), {errors} errors")

    fetch_time = time.monotonic() - start_time
    total_versions = sum(len(p.versions) for p in prepared)
    console.print(
        f"\n  Fetched {len(prepared)} laws with {total_versions} versions "
        f"in {fetch_time:.0f}s ({errors} errors)\n"
    )

    if dry_run:
        console.print("[yellow]Dry run — no commits created[/yellow]")
        return 0

    # 3. Sequential commits (oldest first per law)
    console.print("[bold]Phase 3: Commit versions[/bold]")
    repo = GitRepo(cc.repo_path, config.git.committer_name, config.git.committer_email)
    total_commits = 0

    for i, law in enumerate(prepared, 1):
        commits = _commit_law(repo, law)
        total_commits += commits
        if i % 100 == 0:
            console.print(f"  {i}/{len(prepared)} laws committed, {total_commits} total commits")

    console.print(
        f"\n[bold green]✓ Bootstrap CZ complete[/bold green]\n"
        f"  {len(prepared)} laws, {total_versions} versions, "
        f"{total_commits} commits"
    )
    return total_commits


def _prepare_one_law(cc, stale_url: str) -> _PreparedLaw:
    """Fetch metadata + all historical versions for one law.

    Each worker creates its own client (with its own rate limiter).
    """
    meta_parser = ESbirkaMetadataParser()
    text_parser = ESbirkaTextParser()

    with ESbirkaClient.create(cc) as client:
        try:
            # Fetch current metadata
            meta_bytes = client.get_metadata(stale_url)
            metadata = meta_parser.parse(meta_bytes, stale_url)
            file_path = norm_to_filepath(metadata)
            meta_json = json.loads(meta_bytes)
        except Exception as e:
            return _PreparedLaw(
                stale_url=stale_url,
                metadata=NormMetadata(
                    title="",
                    short_title="",
                    identifier=stale_url,
                    country="cz",
                    rank="unknown",
                    publication_date=date(1970, 1, 1),
                    status="unknown",
                    department="",
                    source="",
                ),
                file_path="",
                error=str(e),
            )

        # Build version timeline
        version_dates = _build_version_timeline(client, meta_json, stale_url)

        # Fetch text at each version date
        versions: list[_VersionSnapshot] = []
        for v_date, source in version_dates:
            try:
                text_bytes = client.get_text(f"{stale_url}/{v_date.isoformat()}")
                blocks = text_parser.parse_text(text_bytes)
                md = render_norm_at_date(metadata, blocks, v_date)
                versions.append(
                    _VersionSnapshot(
                        effective_date=v_date,
                        markdown=md,
                        source_amendment=source,
                    )
                )
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (400, 404):
                    # Version not available at this date — skip
                    logger.debug(
                        "Version %s at %s not available: %s",
                        stale_url,
                        v_date,
                        e,
                    )
                else:
                    logger.warning(
                        "Error fetching %s at %s: %s",
                        stale_url,
                        v_date,
                        e,
                    )
            except Exception as e:
                logger.warning(
                    "Error processing %s at %s: %s",
                    stale_url,
                    v_date,
                    e,
                )

    return _PreparedLaw(
        stale_url=stale_url,
        metadata=metadata,
        file_path=file_path,
        versions=versions,
    )


def _build_version_timeline(
    client: ESbirkaClient,
    meta: dict,
    stale_url: str,
) -> list[tuple[date, str]]:
    """Build a chronological list of (effective_date, source) for all versions.

    Parses amendment numbers from the full citation text, then fetches
    each amendment's metadata to get its effective date.
    """
    # Original version date
    original_date_str = meta.get("datumUcinnostiOd", "")
    if not original_date_str:
        return []

    original_date = date.fromisoformat(original_date_str[:10])
    timeline: list[tuple[date, str]] = [(original_date, "original")]

    # Extract amendment numbers from full citation
    citation = meta.get("uplnaCitaceSNovelami", "")
    amendments = _AMENDMENT_RE.findall(citation)

    for num, year in amendments:
        # Skip the law itself
        if f"/{year}/{num}" in stale_url:
            continue

        amendment_url = f"/sb/{year}/{num}"
        try:
            amend_bytes = client.get_metadata(amendment_url)
            amend_meta = json.loads(amend_bytes)
            eff_date_str = amend_meta.get("datumUcinnostiOd", "")
            if eff_date_str:
                eff_date = date.fromisoformat(eff_date_str[:10])
                timeline.append((eff_date, f"{num}/{year} Sb."))
        except Exception:
            logger.debug("Could not fetch amendment %s/%s metadata", num, year)

    # Sort chronologically and deduplicate dates
    timeline.sort(key=lambda x: x[0])
    seen: set[date] = set()
    unique: list[tuple[date, str]] = []
    for d, s in timeline:
        if d not in seen:
            seen.add(d)
            unique.append((d, s))

    return unique


def _commit_law(repo: GitRepo, law: _PreparedLaw) -> int:
    """Create git commits for all versions of a law (oldest first)."""
    if not law.versions or not law.file_path:
        return 0

    commits = 0
    for i, version in enumerate(law.versions):
        changed = repo.write_and_add(law.file_path, version.markdown)
        if not changed and i > 0:
            # No text difference from previous version — skip
            continue

        if i == 0:
            commit_type = CommitType.BOOTSTRAP
        else:
            commit_type = CommitType.REFORM

        reform = Reform(
            date=version.effective_date,
            norm_id=version.source_amendment,
            affected_blocks=(),
        )
        info = build_commit_info(
            commit_type,
            law.metadata,
            reform,
            [],  # blocks not needed for commit message
            law.file_path,
            version.markdown,
        )
        sha = repo.commit(info)
        if sha:
            commits += 1

    return commits
