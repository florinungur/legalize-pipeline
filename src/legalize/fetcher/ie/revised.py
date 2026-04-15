"""Post-bootstrap pass: apply Revised Acts consolidated versions.

After the initial bootstrap (enacted text), this module fetches
consolidated text from revisedacts.lawreform.ie for the ~560 acts
that have revised versions, and creates a second commit per law
with the updated text.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import date
from pathlib import Path

from rich.console import Console

from legalize.config import Config
from legalize.fetcher.ie.client import ISBClient
from legalize.fetcher.ie.parser import parse_revised_html
from legalize.transformer.markdown import render_paragraphs

logger = logging.getLogger(__name__)
console = Console()


def apply_revised_acts(
    config: Config,
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> int:
    """Fetch and apply Revised Acts versions to the Ireland repo.

    For each norm in the repo, checks if a Revised Acts version exists.
    If it does, overwrites the MD file with the consolidated text and
    creates a REFORM commit dated at the "Updated to" date.

    Returns the number of commits created.
    """
    cc = config.get_country("ie")
    repo_path = Path(cc.repo_path)

    if not repo_path.exists():
        console.print("[red]Repo not found. Run bootstrap first.[/red]")
        return 0

    console.print("[bold]Revised Acts — applying consolidated versions[/bold]\n")

    commits_created = 0
    revised_found = 0
    errors = 0

    with ISBClient.create(cc) as client:
        # Discover which acts have revised versions by scraping the listing
        norm_ids = _discover_revised_ids(client)
        console.print(f"  {len(norm_ids)} acts with revised versions found\n")
        for i, norm_id in enumerate(norm_ids):
            if limit and revised_found >= limit:
                break

            # Try to fetch revised text
            try:
                revised_data = client.get_revised_text(norm_id)
            except Exception as e:
                logger.debug("Error fetching revised %s: %s", norm_id, e)
                errors += 1
                continue

            if revised_data is None:
                continue  # No revised version (404)

            revised_found += 1

            # Parse revised HTML
            paragraphs, updated_to = parse_revised_html(revised_data)
            if not paragraphs:
                logger.warning("No paragraphs from revised %s", norm_id)
                continue

            if updated_to is None:
                updated_to = date.today()

            # Render to markdown
            md_content = render_paragraphs(paragraphs)

            # Read existing frontmatter from the enacted version
            file_path = repo_path / "ie" / f"{norm_id}.md"
            if not file_path.exists():
                logger.debug("No enacted file for %s, skipping", norm_id)
                continue

            existing = file_path.read_text(encoding="utf-8")
            # Extract frontmatter
            if existing.startswith("---"):
                parts = existing.split("---", 2)
                if len(parts) >= 3:
                    frontmatter = parts[1]
                    # Update last_updated in frontmatter
                    import re

                    frontmatter = re.sub(
                        r'last_updated: ".*?"',
                        f'last_updated: "{updated_to.isoformat()}"',
                        frontmatter,
                    )
                    new_content = f"---{frontmatter}---\n{md_content}\n"
                else:
                    new_content = f"{md_content}\n"
            else:
                new_content = f"{md_content}\n"

            # Check if content actually changed
            if new_content == existing:
                logger.debug("No change for %s", norm_id)
                continue

            if dry_run:
                console.print(f"  [yellow]DRY-RUN[/yellow] {norm_id} → revised {updated_to}")
                commits_created += 1
                continue

            # Write updated content
            file_path.write_text(new_content, encoding="utf-8")

            # Git add + commit
            rel_path = f"ie/{norm_id}.md"
            subprocess.run(
                ["git", "-C", str(repo_path), "add", rel_path],
                check=True,
                capture_output=True,
            )

            # Check if there's actually a diff staged
            result = subprocess.run(
                ["git", "-C", str(repo_path), "diff", "--cached", "--quiet"],
                capture_output=True,
            )
            if result.returncode == 0:
                # No changes staged
                continue

            commit_msg = f"[reforma] {norm_id} — consolidated version {updated_to}"
            env = os.environ.copy()
            env["GIT_AUTHOR_DATE"] = f"{updated_to.isoformat()}T12:00:00"
            env["GIT_COMMITTER_DATE"] = f"{updated_to.isoformat()}T12:00:00"
            env["GIT_COMMITTER_NAME"] = config.git.committer_name
            env["GIT_COMMITTER_EMAIL"] = config.git.committer_email

            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_path),
                    "commit",
                    "-m",
                    commit_msg,
                    "--trailer",
                    f"Source-Id={norm_id}",
                    "--trailer",
                    f"Source-Date={updated_to.isoformat()}",
                    "--trailer",
                    f"Norm-Id={norm_id}",
                ],
                check=True,
                capture_output=True,
                env=env,
            )
            commits_created += 1

            if (revised_found % 50) == 0:
                console.print(
                    f"  [{i + 1}/{len(norm_ids)}] {revised_found} revised, "
                    f"{commits_created} commits"
                )

    console.print(
        f"\n[bold green]✓ Revised Acts complete[/bold green]\n"
        f"  {revised_found} revised versions found\n"
        f"  {commits_created} commits created\n"
        f"  {errors} errors"
    )
    return commits_created


def _discover_revised_ids(client: ISBClient) -> list[str]:
    """Scrape the Revised Acts chronological listing to find all act IDs.

    Much faster than probing 4,000+ norms individually (~1 request
    vs ~4,000 requests).
    """
    import re

    url = "https://revisedacts.lawreform.ie/revacts/chron"
    try:
        data = client._get(url)
    except Exception:
        logger.warning("Could not fetch Revised Acts listing")
        return []

    html_text = data.decode("utf-8", errors="replace")

    # Extract act URLs: /eli/{year}/act/{number}/front/revised
    acts = re.findall(r"/eli/(\d{4})/act/(\d+)/front/revised", html_text)

    norm_ids = sorted({f"IE-{year}-act-{num}" for year, num in acts})
    return norm_ids
