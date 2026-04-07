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

Parallelism: HTTP fetching (the bottleneck) runs in a ThreadPoolExecutor
with N workers — the Riigi Teataja server has a sharp rate-limit cliff
around 5-6 concurrent connections, so 4 workers is the sweet spot
(empirically validated at 34 req/s with 0% errors, jumping to 56% errors
at 8 workers). Git commits run sequentially on the main thread because
the repo is not thread-safe.

This module is discovered automatically by ``pipeline.generic_bootstrap``
via the optional ``fetcher/{country}/bootstrap.py`` hook.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import date

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
from legalize.models import Block, CommitType, NormMetadata, Reform
from legalize.transformer.markdown import render_norm_at_date

console = Console()
logger = logging.getLogger(__name__)


# Default parallelism — 4 workers is the sweet spot for Riigi Teataja
_DEFAULT_WORKERS = 4


@dataclass
class _PreparedVersion:
    """A fully fetched + parsed + rendered version, ready to commit."""

    version: HistoricalVersion
    metadata: NormMetadata
    blocks: tuple[Block, ...]
    markdown: str
    commit_date: date


@dataclass
class _PreparedLaw:
    """A whole law with all its historical versions prepared for commit."""

    canonical_id: str
    filename_id: str
    prepared: list[_PreparedVersion]
    error: str | None = None


def bootstrap(
    config: Config,
    dry_run: bool = False,
    limit: int | None = None,
    workers: int = _DEFAULT_WORKERS,
) -> int:
    """Estonia bootstrap: discovery → parallel crawl+fetch → sequential commits.

    Args:
        config: loaded Config
        dry_run: if True, print actions without committing
        limit: only process the first N canonical laws (useful for testing)
        workers: number of parallel HTTP workers (default 4; >6 will trigger
            server-side rate limiting with 404 errors)

    Returns the total number of commits created.
    """
    cc = config.get_country("ee")

    console.print("[bold]Bootstrap EE — full history via Eelmine chain[/bold]\n")
    console.print(f"  Data dir: {cc.data_dir}")
    console.print(f"  Repo output: {cc.repo_path}")
    console.print(f"  Parallel workers: {workers}\n")

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

    # 2. Initialize repo + load existing commits for idempotency
    repo = GitRepo(cc.repo_path, config.git.committer_name, config.git.committer_email)
    if not dry_run:
        repo.init()
        console.print("  Loading existing commits for idempotency...")
        t0 = time.monotonic()
        repo.load_existing_commits()
        existing = getattr(repo, "_existing_commits", set())
        console.print(
            f"  Loaded {len(existing)} existing (Source-Id, Norm-Id) pairs "
            f"in {time.monotonic() - t0:.1f}s\n"
        )

    text_parser = RTTextParser()
    meta_parser = RTMetadataParser()

    # 3. Parallel HTTP/parse stage + sequential commit stage
    #
    # Each worker creates its own RTClient (thread-safety via
    # requests.Session) and fully prepares a law (walks chain, fetches
    # every version, renders markdown) into a _PreparedLaw. The main
    # thread consumes _PreparedLaw results as they complete and runs
    # the git commits sequentially.
    total_commits = 0
    errors: list[str] = []
    processed = 0

    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _prepare_one_law,
                cc,
                text_parser,
                meta_parser,
                canonical_id,
            ): canonical_id
            for canonical_id in canonical_ids
        }

        for future in as_completed(futures):
            canonical_id = futures[future]
            processed += 1
            try:
                prepared_law = future.result()
            except Exception as e:
                msg = f"{canonical_id}: prepare failed: {type(e).__name__}: {e}"
                logger.error(msg, exc_info=True)
                errors.append(msg)
                continue

            if prepared_law.error:
                errors.append(f"{canonical_id}: {prepared_law.error}")
                continue

            # Sequential commit phase (main thread, git is not concurrent)
            try:
                commits = _commit_prepared_law(repo, prepared_law, dry_run=dry_run)
                total_commits += commits
            except Exception as e:
                msg = f"{canonical_id}: commit failed: {type(e).__name__}: {e}"
                logger.error(msg, exc_info=True)
                errors.append(msg)

            if processed % 25 == 0:
                elapsed = time.monotonic() - t0
                rate = processed / elapsed
                eta_seconds = (len(canonical_ids) - processed) / rate if rate > 0 else 0
                console.print(
                    f"  [dim][{processed}/{len(canonical_ids)}] {total_commits} commits, "
                    f"{len(errors)} errors, {rate:.1f} laws/s, "
                    f"ETA {eta_seconds / 60:.0f} min[/dim]"
                )

    elapsed = time.monotonic() - t0
    console.print(
        f"\n[bold green]✓ Bootstrap EE finished[/bold green]\n"
        f"  {len(canonical_ids)} canonical laws processed in {elapsed / 60:.1f} min\n"
        f"  {total_commits} commits created\n"
        f"  {len(errors)} errors"
    )
    if errors:
        for e in errors[:5]:
            console.print(f"  [red]✗ {e}[/red]")
        if len(errors) > 5:
            console.print(f"  [red]... and {len(errors) - 5} more[/red]")
    return total_commits


def _prepare_one_law(
    cc,
    text_parser: RTTextParser,
    meta_parser: RTMetadataParser,
    canonical_id: str,
) -> _PreparedLaw:
    """Runs in a worker thread: walks the chain, fetches every version,
    parses + renders markdown. Does NOT touch git.

    Each worker creates its own RTClient so requests.Session is
    per-thread (requests.Session is not thread-safe).
    """
    try:
        with RTClient.create(cc) as client:
            history = full_history(client, canonical_id)
            if not history:
                return _PreparedLaw(
                    canonical_id=canonical_id,
                    filename_id=canonical_id,
                    prepared=[],
                    error="empty history",
                )

            # Log any chain inconsistencies (non-fatal)
            for warning in validate_chain_contiguity(history):
                logger.warning("%s: %s", canonical_id, warning)

            filename_id = canonical_filename_id(history) or canonical_id

            prepared: list[_PreparedVersion] = []
            for version in history:
                try:
                    xml_bytes = client.get_text(version.global_id)
                except Exception as e:
                    logger.warning(
                        "Failed to fetch version %s of %s: %s",
                        version.global_id,
                        canonical_id,
                        e,
                    )
                    continue

                try:
                    metadata = meta_parser.parse(xml_bytes, version.global_id)
                    blocks = text_parser.parse_text(xml_bytes)
                except Exception as e:
                    logger.warning(
                        "Failed to parse version %s of %s: %s",
                        version.global_id,
                        canonical_id,
                        e,
                    )
                    continue

                # Override the identifier so the repo path uses the
                # canonical (oldest) globaalID even though per-version
                # metadata reports its own.
                canonical_meta = replace(metadata, identifier=filename_id)

                commit_date = version.effective_from or canonical_meta.publication_date

                markdown = render_norm_at_date(
                    canonical_meta, tuple(blocks), commit_date, include_all=True
                )

                prepared.append(
                    _PreparedVersion(
                        version=version,
                        metadata=canonical_meta,
                        blocks=tuple(blocks),
                        markdown=markdown,
                        commit_date=commit_date,
                    )
                )

            return _PreparedLaw(
                canonical_id=canonical_id,
                filename_id=filename_id,
                prepared=prepared,
            )
    except Exception as e:
        logger.error("Worker failed on %s", canonical_id, exc_info=True)
        return _PreparedLaw(
            canonical_id=canonical_id,
            filename_id=canonical_id,
            prepared=[],
            error=f"{type(e).__name__}: {e}",
        )


def _commit_prepared_law(
    repo: GitRepo,
    law: _PreparedLaw,
    *,
    dry_run: bool,
) -> int:
    """Commit all prepared versions of a law sequentially. Main thread only."""
    if not law.prepared:
        return 0

    commits_created = 0
    file_path = f"ee/{law.filename_id}.md"

    for idx, pv in enumerate(law.prepared):
        is_first = idx == 0

        if dry_run:
            console.print(
                f"    [dim]{pv.commit_date} {pv.version.global_id} — "
                f"{'bootstrap' if is_first else 'reform'} "
                f"({len(pv.blocks)} blocks, {len(pv.markdown)} bytes)[/dim]"
            )
            continue

        # Idempotency: if this exact (Source-Id, Norm-Id) pair already
        # exists in the repo, skip it. Much faster than re-writing +
        # re-committing and letting write_and_add detect the no-op.
        if repo.has_commit_with_source_id(pv.version.global_id, law.filename_id):
            continue

        changed = repo.write_and_add(file_path, pv.markdown)
        if not changed and not is_first:
            continue

        reform = Reform(
            date=pv.commit_date,
            norm_id=pv.version.global_id,
            affected_blocks=(),
        )
        commit_type = CommitType.BOOTSTRAP if is_first else CommitType.REFORM
        info = build_commit_info(
            commit_type,
            pv.metadata,
            reform,
            list(pv.blocks),
            file_path,
            pv.markdown,
        )
        sha = repo.commit(info)
        if sha:
            commits_created += 1

    return commits_created
