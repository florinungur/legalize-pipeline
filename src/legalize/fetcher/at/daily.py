"""Austria-specific daily processing.

Queries the RIS OGD API for norms modified on a target date
and generates commits for changed legislation.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import date, timedelta

from rich.console import Console

from legalize.committer.git_ops import GitRepo
from legalize.committer.message import build_commit_info
from legalize.config import Config
from legalize.models import CommitType, Reform
from legalize.state.store import StateStore, infer_last_date_from_git
from legalize.transformer.markdown import render_norm_at_date
from legalize.transformer.slug import norm_to_filepath

console = Console()
logger = logging.getLogger(__name__)


def daily(
    config: Config,
    target_date: date | None = None,
    dry_run: bool = False,
) -> int:
    """Daily processing for Austria: query RIS API for modified norms."""
    from legalize.fetcher.at.client import RISClient
    from legalize.fetcher.at.discovery import RISDiscovery
    from legalize.fetcher.at.parser import RISMetadataParser, RISTextParser

    cc = config.get_country("at")
    state = StateStore(cc.state_path)
    state.load()

    if target_date:
        dates_to_process = [target_date]
    else:
        start = state.last_summary_date
        if start is None:
            start = infer_last_date_from_git(cc.repo_path)
        if start is None:
            console.print("[yellow]No last date found. Use --date or run bootstrap.[/yellow]")
            return 0
        start = start + timedelta(days=1)
        end = date.today()
        dates_to_process = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # RIS updates on weekdays
                dates_to_process.append(current)
            current += timedelta(days=1)

    if not dates_to_process:
        console.print("[green]Nothing to process — up to date[/green]")
        return 0

    console.print(f"[bold]Daily AT — processing {len(dates_to_process)} day(s)[/bold]")

    repo = GitRepo(cc.repo_path, config.git.committer_name, config.git.committer_email)
    commits_created = 0
    errors: list[str] = []

    text_parser = RISTextParser()
    meta_parser = RISMetadataParser()
    discovery = RISDiscovery()

    with RISClient() as client:
        for current_date in dates_to_process:
            console.print(f"\n  [bold]{current_date}[/bold]")

            try:
                modified_ids = list(discovery.discover_daily(client, current_date))
            except Exception:
                msg = f"Error discovering changes for {current_date}"
                logger.error(msg, exc_info=True)
                errors.append(msg)
                continue

            if not modified_ids:
                console.print("    No changes found")
                state.last_summary_date = current_date
                continue

            console.print(f"    {len(modified_ids)} law(s) modified")

            for gesetzesnummer in modified_ids:
                if dry_run:
                    console.print(f"    [dim]{gesetzesnummer} — would process[/dim]")
                    continue

                try:
                    meta_data = client.get_metadata(gesetzesnummer)
                    metadata = meta_parser.parse(meta_data, gesetzesnummer)

                    text_data = client.get_text(gesetzesnummer)
                    blocks = text_parser.parse_text(text_data)

                    file_path = norm_to_filepath(metadata)
                    markdown = render_norm_at_date(metadata, blocks, current_date)

                    changed = repo.write_and_add(file_path, markdown)
                    if not changed:
                        console.print(f"    [dim]⏭ {metadata.short_title} — no changes[/dim]")
                        continue

                    reform = Reform(
                        date=current_date,
                        norm_id=f"RIS-DAILY-{current_date.isoformat()}",
                        affected_blocks=(),
                    )
                    info = build_commit_info(
                        CommitType.REFORM,
                        metadata,
                        reform,
                        blocks,
                        file_path,
                        markdown,
                    )
                    sha = repo.commit(info)

                    if sha:
                        commits_created += 1
                        console.print(f"    [green]✓[/green] {info.subject}")

                except Exception as e:
                    msg = f"Error processing {gesetzesnummer}: {e}"
                    logger.error(msg, exc_info=True)
                    errors.append(msg)

            state.last_summary_date = current_date

    if not dry_run and config.git.push and commits_created > 0:
        try:
            repo.push()
        except subprocess.CalledProcessError:
            logger.error("Error pushing", exc_info=True)
            errors.append("Error pushing")

    state.record_run(
        summaries=[d.isoformat() for d in dates_to_process],
        commits=commits_created,
        errors=errors,
    )
    state.save()

    console.print(f"\n[bold green]✓ {commits_created} commits[/bold green]")
    if errors:
        console.print(f"[yellow]⚠ {len(errors)} errors[/yellow]")

    return commits_created
