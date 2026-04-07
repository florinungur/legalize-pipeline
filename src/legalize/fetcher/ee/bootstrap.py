"""Estonia-specific bootstrap: full history via Eelmine chain crawl.

Unlike the generic bootstrap (which has one canonical version per law and
emits one commit per reform extracted from the data), Estonia's bootstrap:

  1. Uses the bulk-dump discovery to enumerate *canonical* laws (one per
     ``terviktekstiGrupiID`` group).
  2. For each canonical law, fetches its HTML page to walk the
     ``Eelmine``/``Järgmine`` chain and reconstruct the full timeline.
  3. For every historical version in the chain, fetches its XML, parses
     it, renders the markdown, and emits a git commit with
     ``GIT_AUTHOR_DATE = kehtivuseAlgus``.

The filename in the repo is derived from the *oldest* globaalID in the
group so that new reforms always append commits to the same file instead
of renaming it.

This module is discovered automatically by ``pipeline.generic_bootstrap``
via the optional ``fetcher/{country}/bootstrap.py`` hook.
"""

from __future__ import annotations

import logging

from rich.console import Console

from legalize.committer.git_ops import GitRepo
from legalize.committer.message import build_commit_info
from legalize.config import Config
from legalize.fetcher.ee.client import RTClient
from legalize.fetcher.ee.discovery import RTDiscovery
from legalize.fetcher.ee.history import (
    HistoricalVersion,
    canonical_filename_id,
    full_history,
    validate_chain_contiguity,
)
from legalize.fetcher.ee.parser import RTMetadataParser, RTTextParser
from legalize.models import CommitType, NormMetadata, Reform
from legalize.transformer.markdown import render_norm_at_date

console = Console()
logger = logging.getLogger(__name__)


def bootstrap(
    config: Config,
    dry_run: bool = False,
    limit: int | None = None,
) -> int:
    """Estonia bootstrap: discovery → for each law, crawl Eelmine chain
    and commit each historical version.

    Returns the total number of commits created.
    """
    cc = config.get_country("ee")

    console.print("[bold]Bootstrap EE — full history via Eelmine chain[/bold]\n")
    console.print(f"  Data dir: {cc.data_dir}")
    console.print(f"  Repo output: {cc.repo_path}\n")

    # 1. Discovery — enumerate canonical IDs from the bulk dump
    discovery = RTDiscovery.create(cc)
    console.print("  Discovering canonical laws from bulk dump...")
    canonical_ids = list(discovery.discover_all(client=None))
    if limit:
        canonical_ids = canonical_ids[:limit]
    console.print(f"  Found {len(canonical_ids)} canonical laws\n")

    if not canonical_ids:
        console.print("[yellow]No laws found. Run ensure_bulk_dump() first.[/yellow]")
        return 0

    # 2. Initialize repo
    repo = GitRepo(cc.repo_path, config.git.committer_name, config.git.committer_email)
    if not dry_run:
        repo.init()

    text_parser = RTTextParser()
    meta_parser = RTMetadataParser()

    total_commits = 0
    errors: list[str] = []

    # 3. For each canonical law: crawl chain, fetch every version, commit it
    with RTClient.create(cc) as client:
        for i, canonical_id in enumerate(canonical_ids, 1):
            try:
                commits = _bootstrap_one_law(
                    client,
                    discovery,
                    text_parser,
                    meta_parser,
                    repo,
                    canonical_id,
                    dry_run=dry_run,
                )
                total_commits += commits
            except Exception as e:
                msg = f"{canonical_id}: {type(e).__name__}: {e}"
                logger.error("Bootstrap failed for %s", canonical_id, exc_info=True)
                errors.append(msg)
                console.print(f"  [red]✗ {msg}[/red]")

            if i % 25 == 0:
                console.print(
                    f"  [dim][{i}/{len(canonical_ids)}] {total_commits} commits, "
                    f"{len(errors)} errors[/dim]"
                )

    console.print(
        f"\n[bold green]✓ Bootstrap EE finished[/bold green]\n"
        f"  {len(canonical_ids)} canonical laws processed\n"
        f"  {total_commits} commits created\n"
        f"  {len(errors)} errors"
    )
    return total_commits


def _bootstrap_one_law(
    client: RTClient,
    discovery: RTDiscovery,
    text_parser: RTTextParser,
    meta_parser: RTMetadataParser,
    repo: GitRepo,
    canonical_id: str,
    *,
    dry_run: bool,
) -> int:
    """Walk one law's Eelmine chain and emit a commit per historical version.

    Returns the number of commits created for this law.
    """
    # Walk the chain both ways from the canonical (= newest at time of
    # discovery) version. This gives us every historical version even
    # if the bulk dump only contained the newest one.
    history = full_history(client, canonical_id)
    if not history:
        return 0

    # Validate the chain for contiguity and log any weirdness.
    # Gaps/overlaps are not fatal — the source has them sometimes — but
    # they deserve a warning.
    for warning in validate_chain_contiguity(history):
        logger.warning("%s: %s", canonical_id, warning)

    # The oldest version's globaalID becomes the canonical filename so
    # that the repo path is stable across reruns and new future reforms.
    filename_id = canonical_filename_id(history)
    if filename_id is None:
        return 0

    commits_created = 0

    for version_idx, version in enumerate(history):
        commits_created += _commit_one_version(
            client,
            text_parser,
            meta_parser,
            repo,
            version,
            filename_id=filename_id,
            is_first=version_idx == 0,
            dry_run=dry_run,
        )

    return commits_created


def _commit_one_version(
    client: RTClient,
    text_parser: RTTextParser,
    meta_parser: RTMetadataParser,
    repo: GitRepo,
    version: HistoricalVersion,
    *,
    filename_id: str,
    is_first: bool,
    dry_run: bool,
) -> int:
    """Fetch one historical version's XML, render markdown, emit commit."""
    # Fetch the XML for this specific version (cheap HTTP GET, ~100 KB avg)
    try:
        xml_bytes = client.get_text(version.global_id)
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", version.global_id, e)
        return 0

    # Parse metadata + body with the current-version parser
    metadata = meta_parser.parse(xml_bytes, version.global_id)
    blocks = text_parser.parse_text(xml_bytes)

    # Override the metadata identifier so the repo path uses the
    # CANONICAL (oldest) globaalID — even though the per-version metadata
    # reports its own globaalID. This makes the filename stable across
    # the entire version history.
    canonical_meta = _with_identifier(metadata, filename_id)

    # Use the version's kehtivuseAlgus as the commit date
    commit_date = version.effective_from or canonical_meta.publication_date

    # Render markdown at the version's effective date
    markdown = render_norm_at_date(canonical_meta, blocks, commit_date, include_all=True)

    file_path = f"ee/{filename_id}.md"

    if dry_run:
        console.print(
            f"    [dim]{commit_date} {version.global_id} — "
            f"{'bootstrap' if is_first else 'reform'} ({len(blocks)} blocks, "
            f"{len(markdown)} bytes)[/dim]"
        )
        return 0

    # Idempotency check: if this exact version's Source-Id + Norm-Id pair
    # is already in the repo, skip it.
    if repo.has_commit_with_source_id(version.global_id, filename_id):
        return 0

    changed = repo.write_and_add(file_path, markdown)
    if not changed and not is_first:
        return 0

    # Build a synthetic Reform for the commit info
    reform = Reform(
        date=commit_date,
        norm_id=version.global_id,
        affected_blocks=(),
    )

    commit_type = CommitType.BOOTSTRAP if is_first else CommitType.REFORM
    info = build_commit_info(
        commit_type,
        canonical_meta,
        reform,
        blocks,
        file_path,
        markdown,
    )
    sha = repo.commit(info)
    if sha:
        console.print(
            f"    [green]✓[/green] {commit_date} — {version.global_id} "
            f"({'bootstrap' if is_first else 'reform'})"
        )
        return 1
    return 0


def _with_identifier(meta: NormMetadata, new_id: str) -> NormMetadata:
    """Return a copy of ``meta`` with a different ``identifier`` field.

    NormMetadata is a frozen dataclass so we build a new instance.
    """
    from dataclasses import replace

    return replace(meta, identifier=new_id)
