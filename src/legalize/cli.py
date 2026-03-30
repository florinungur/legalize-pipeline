"""Legalize pipeline CLI.

Entry point: `legalize <subcommand> [options]`

Unified CLI with --country flag for all operations.
"""

from __future__ import annotations

import logging
from datetime import date

import click
from rich.console import Console
from rich.logging import RichHandler

from legalize.config import load_config
from legalize.countries import supported_countries
from legalize.models import EstadoNorma, NormaMetadata, Rango

console = Console()

# ELI codes for all Spanish autonomous communities
_CCAA_CODES = [
    "es-an", "es-ar", "es-as", "es-cb", "es-cl", "es-cm", "es-cn", "es-ct",
    "es-ex", "es-ga", "es-ib", "es-mc", "es-md", "es-nc", "es-pv", "es-ri", "es-vc",
]


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
    )


def _country_option(default: str = "es"):
    """Shared --country option for all commands."""
    return click.option(
        "--country", "-c",
        default=default,
        type=click.Choice(supported_countries(), case_sensitive=False),
        help="Country code (e.g., es, fr, se).",
    )


@click.group()
@click.option("--config", "config_path", default="config.yaml", help="Path to config file.")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logs.")
@click.pass_context
def cli(ctx: click.Context, config_path: str, verbose: bool) -> None:
    """Legalize — Version-controlled legislation in Git."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config_path)


# ─────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────


@cli.command()
@click.argument("norm_ids", nargs=-1)
@_country_option()
@click.option("--all", "fetch_all_flag", is_flag=True, help="Download all from catalog/config.")
@click.option("--catalog", is_flag=True, help="Spain only: download ALL from BOE catalog.")
@click.option("--force", is_flag=True, help="Re-download even if already exists.")
@click.option("--data-dir", default=None, help="Override data directory.")
@click.option("--legi-dir", default=None, help="France only: path to extracted LEGI dump.")
@click.option("--limit", default=None, type=int, help="Max norms to fetch (for testing).")
@click.pass_context
def fetch(
    ctx: click.Context,
    norm_ids: tuple[str, ...],
    country: str,
    fetch_all_flag: bool,
    catalog: bool,
    force: bool,
    data_dir: str | None,
    legi_dir: str | None,
    limit: int | None,
) -> None:
    """Download laws to data/ (does not touch git).

    Examples:
        legalize fetch BOE-A-1978-31229            # Single Spanish law
        legalize fetch -c fr --all --legi-dir /path # All French codes
        legalize fetch -c se --all                  # All Swedish statutes
        legalize fetch -c de --limit 5              # Test: 5 German laws
    """
    config = ctx.obj["config"]
    if data_dir:
        config.data_dir = data_dir
    if legi_dir:
        config.legi_dir = legi_dir

    if country == "es":
        from legalize.pipeline import fetch_all, fetch_catalog, fetch_one

        if catalog:
            fetch_catalog(config, force=force)
        elif fetch_all_flag:
            fetch_all(config, force=force)
        elif norm_ids:
            for norm_id in norm_ids:
                fetch_one(config, norm_id, force=force)
        else:
            console.print("Use --catalog, --all, or pass norm IDs.")

    elif country == "fr":
        from legalize.pipeline_fr import fetch_all_fr, fetch_one_fr

        if fetch_all_flag:
            fetch_all_fr(config, force=force)
        elif norm_ids:
            for norm_id in norm_ids:
                fetch_one_fr(config, norm_id, force=force)
        else:
            console.print("Use --all or pass LEGITEXT IDs.")

    elif country == "se":
        from legalize.pipeline_se import fetch_all_se, fetch_one_se

        if fetch_all_flag:
            fetch_all_se(config, force=force)
        elif norm_ids:
            for sfs in norm_ids:
                fetch_one_se(config, sfs, force=force)
        else:
            console.print("Use --all or pass SFS numbers.")

    else:
        console.print(f"[red]Country '{country}' fetch not implemented yet.[/red]")


# ─────────────────────────────────────────────
# COMMIT
# ─────────────────────────────────────────────


@cli.command()
@click.argument("norm_ids", nargs=-1)
@_country_option()
@click.option("--all", "commit_all_flag", is_flag=True, help="Commit all from data/json/.")
@click.option("--dry-run", is_flag=True, help="Simulate without creating commits.")
@click.pass_context
def commit(
    ctx: click.Context,
    norm_ids: tuple[str, ...],
    country: str,
    commit_all_flag: bool,
    dry_run: bool,
) -> None:
    """Generate git commits from local data in data/ (does not download anything)."""
    from legalize.pipeline import commit_all, commit_one

    config = ctx.obj["config"]

    if commit_all_flag:
        commit_all(config, dry_run=dry_run)
    elif norm_ids:
        for norm_id in norm_ids:
            commit_one(config, norm_id, dry_run=dry_run)
    else:
        console.print("Use --all or pass norm IDs.")


# ─────────────────────────────────────────────
# BOOTSTRAP
# ─────────────────────────────────────────────


@cli.command()
@_country_option()
@click.option("--repo-path", default=None, help="Override output repo directory.")
@click.option("--data-dir", default=None, help="Override data directory.")
@click.option("--legi-dir", default=None, help="France only: path to extracted LEGI dump.")
@click.option("--xml", "xml_path", default=None, help="Path to local XML (pilot, ES only).")
@click.option("--dry-run", is_flag=True, help="Simulate without creating commits.")
@click.pass_context
def bootstrap(
    ctx: click.Context,
    country: str,
    repo_path: str | None,
    data_dir: str | None,
    legi_dir: str | None,
    xml_path: str | None,
    dry_run: bool,
) -> None:
    """Fetch + commit all norms for a country.

    Examples:
        legalize bootstrap                          # Spain (default)
        legalize bootstrap -c fr --legi-dir /path   # France
        legalize bootstrap -c se --data-dir ../data-se  # Sweden
    """
    config = ctx.obj["config"]
    if repo_path:
        config.git.repo_path = repo_path
    if data_dir:
        config.data_dir = data_dir
    if legi_dir:
        config.legi_dir = legi_dir

    if country == "es":
        from legalize.pipeline import bootstrap as run_bootstrap, bootstrap_from_local_xml

        if xml_path:
            metadata = NormaMetadata(
                titulo="Constitución Española",
                titulo_corto="Constitución Española",
                identificador="BOE-A-1978-31229",
                pais="es",
                rango=Rango.CONSTITUCION,
                fecha_publicacion=date(1978, 12, 29),
                estado=EstadoNorma.VIGENTE,
                departamento="Cortes Generales",
                fuente="https://www.boe.es/eli/es/c/1978/12/27/(1)",
            )
            bootstrap_from_local_xml(config, metadata, xml_path, dry_run=dry_run)
        else:
            run_bootstrap(config, dry_run=dry_run)

    elif country == "fr":
        from legalize.pipeline_fr import bootstrap_fr
        config.country = "fr"
        bootstrap_fr(config, dry_run=dry_run)

    elif country == "se":
        from legalize.pipeline_se import bootstrap_se
        config.country = "se"
        bootstrap_se(config, dry_run=dry_run)

    else:
        console.print(f"[red]Country '{country}' bootstrap not implemented yet.[/red]")


# ─────────────────────────────────────────────
# DAILY
# ─────────────────────────────────────────────


@cli.command()
@_country_option()
@click.option("--date", "target_date", default=None, help="Date to process (YYYY-MM-DD).")
@click.option("--push", is_flag=True, help="Push to remote after commits.")
@click.option("--dry-run", is_flag=True, help="Simulate without creating commits.")
@click.pass_context
def daily(
    ctx: click.Context,
    country: str,
    target_date: str | None,
    push: bool,
    dry_run: bool,
) -> None:
    """Daily processing: process today's new legislation.

    Examples:
        legalize daily                              # Spain, today
        legalize daily -c es --date 2026-03-28      # Spain, specific date
        legalize daily -c se                        # Sweden, today
    """
    from legalize.pipeline import daily as run_daily

    config = ctx.obj["config"]
    if push:
        config.git.push = True

    if country != "es":
        console.print(f"[yellow]Daily for '{country}' not yet implemented. Coming in PR4.[/yellow]")
        return

    parsed_date = date.fromisoformat(target_date) if target_date else None
    run_daily(config, target_date=parsed_date, dry_run=dry_run)


# ─────────────────────────────────────────────
# REPROCESS
# ─────────────────────────────────────────────


@cli.command()
@_country_option()
@click.option("--reason", required=True, help="Reason for reprocessing.")
@click.option("--dry-run", is_flag=True, help="Simulate without creating commits.")
@click.argument("norm_ids", nargs=-1, required=True)
@click.pass_context
def reprocess(
    ctx: click.Context,
    country: str,
    reason: str,
    dry_run: bool,
    norm_ids: tuple[str, ...],
) -> None:
    """Re-download and regenerate specific norms."""
    from legalize.pipeline import reprocess as run_reprocess

    config = ctx.obj["config"]
    run_reprocess(config, list(norm_ids), reason, dry_run=dry_run)


# ─────────────────────────────────────────────
# CCAA (Spain subnational — kept separate)
# ─────────────────────────────────────────────


@cli.command("fetch-ccaa")
@click.argument("jurisdiccion", required=False)
@click.option("--all", "all_flag", is_flag=True, help="Fetch all 17 CCAA.")
@click.option("--force", is_flag=True, help="Re-download even if already exists.")
@click.pass_context
def fetch_ccaa(ctx: click.Context, jurisdiccion: str | None, all_flag: bool, force: bool) -> None:
    """Download CCAA legislation from BOE API.

    Examples:
        legalize fetch-ccaa es-pv          # País Vasco only
        legalize fetch-ccaa --all          # All 17 CCAA
    """
    from legalize.pipeline import fetch_catalog_ccaa

    config = ctx.obj["config"]

    if all_flag:
        for jur in _CCAA_CODES:
            fetch_catalog_ccaa(config, jur, force=force)
    elif jurisdiccion:
        if jurisdiccion not in _CCAA_CODES:
            console.print(f"[red]Unknown: {jurisdiccion}. Valid: {', '.join(_CCAA_CODES)}[/red]")
            return
        fetch_catalog_ccaa(config, jurisdiccion, force=force)
    else:
        console.print("Use --all or pass a jurisdiction code.")
        console.print(f"  Available: {', '.join(_CCAA_CODES)}")


@cli.command("bootstrap-ccaa")
@click.argument("jurisdiccion", required=False)
@click.option("--all", "all_flag", is_flag=True, help="Bootstrap all 17 CCAA.")
@click.option("--force", is_flag=True, help="Re-download even if already exists.")
@click.option("--dry-run", is_flag=True, help="Simulate without creating commits.")
@click.pass_context
def bootstrap_ccaa(
    ctx: click.Context,
    jurisdiccion: str | None,
    all_flag: bool,
    force: bool,
    dry_run: bool,
) -> None:
    """Full CCAA bootstrap: fetch + commit.

    Examples:
        legalize bootstrap-ccaa es-pv          # País Vasco only
        legalize bootstrap-ccaa --all          # All 17 CCAA
    """
    import json
    from pathlib import Path

    from legalize.pipeline import commit_one, fetch_catalog_ccaa

    config = ctx.obj["config"]

    targets = _CCAA_CODES if all_flag else ([jurisdiccion] if jurisdiccion else [])
    if not targets:
        console.print("Use --all or pass a jurisdiction code.")
        console.print(f"  Available: {', '.join(_CCAA_CODES)}")
        return

    if jurisdiccion and jurisdiccion not in _CCAA_CODES:
        console.print(f"[red]Unknown: {jurisdiccion}. Valid: {', '.join(_CCAA_CODES)}[/red]")
        return

    _JUR_TO_DEPT_NAME = {
        "es-an": "Andalucía", "es-ar": "Aragón", "es-as": "Asturias",
        "es-cb": "Cantabria", "es-cl": "Castilla y León", "es-cm": "Castilla-La Mancha",
        "es-cn": "Canarias", "es-ct": "Cataluña", "es-ex": "Extremadura",
        "es-ga": "Galicia", "es-ib": "Balears", "es-mc": "Murcia",
        "es-md": "Madrid", "es-nc": "Navarra", "es-pv": "País Vasco",
        "es-ri": "Rioja", "es-vc": "Valencian",
    }

    grand_total = 0
    for jur in targets:
        console.print(f"\n[bold]{'=' * 50}[/bold]")
        console.print(f"[bold]  {jur.upper()} ({_JUR_TO_DEPT_NAME.get(jur, jur)})[/bold]")
        console.print(f"[bold]{'=' * 50}[/bold]")

        fetch_catalog_ccaa(config, jur, force=force)

        json_dir = Path(config.data_dir) / "json"
        dept_name = _JUR_TO_DEPT_NAME.get(jur, "")
        jur_files = []
        for jf in sorted(json_dir.glob("*.json")):
            with open(jf) as f:
                data = json.load(f)
            jur_code = data.get("metadata", {}).get("jurisdiccion")
            dept = data.get("metadata", {}).get("departamento", "")
            if jur_code == jur or (not jur_code and dept_name in dept):
                jur_files.append(jf)
        jur_files = list(dict.fromkeys(jur_files))

        console.print(f"  {len(jur_files)} norms to commit")

        commits = 0
        errors = 0
        for i, jf in enumerate(jur_files, 1):
            try:
                c = commit_one(config, jf.stem, dry_run=dry_run)
                commits += c
            except Exception:
                errors += 1
            if i % 100 == 0:
                console.print(f"  [{i}/{len(jur_files)}] {commits} commits")

        grand_total += commits
        repo_dir = Path(config.git.repo_path) / jur
        actual = len(list(repo_dir.glob("*.md"))) if repo_dir.exists() else 0
        console.print(f"  [green]=> {actual} files, {commits} new commits, {errors} errors[/green]")

    console.print(f"\n[bold green]Total: {grand_total} new commits[/bold green]")


# ─────────────────────────────────────────────
# STATUS
# ─────────────────────────────────────────────


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show pipeline status."""
    from pathlib import Path

    from legalize.state.mappings import IdToFilename
    from legalize.state.store import StateStore

    config = ctx.obj["config"]

    state = StateStore(config.state_path)
    state.load()

    mappings = IdToFilename(config.mappings_path)
    mappings.load()

    json_dir = Path(config.data_dir) / "json"
    fetched = len(list(json_dir.glob("*.json"))) if json_dir.exists() else 0

    console.print("[bold]Legalize pipeline status[/bold]\n")
    console.print(f"  Downloaded norms (data/): {fetched}")
    console.print(f"  Committed norms: {state.normas_count}")
    console.print(f"  Registered mappings: {len(mappings)}")
    console.print(f"  Last processed summary: {state.ultimo_sumario or '[dim]none[/dim]'}")

    # Show per-country stats if configured
    if config.countries:
        console.print("\n[bold]Per-country:[/bold]")
        for code, cc in config.countries.items():
            if cc.data_dir:
                jdir = Path(cc.data_dir) / "json"
                count = len(list(jdir.glob("*.json"))) if jdir.exists() else 0
                console.print(f"  {code}: {count} norms in {cc.data_dir}")


# ─────────────────────────────────────────────
# DEPRECATED ALIASES (hidden, show warning)
# ─────────────────────────────────────────────


@cli.command("fetch-fr", hidden=True)
@click.argument("norm_ids", nargs=-1)
@click.option("--discover", "discover_flag", is_flag=True)
@click.option("--force", is_flag=True)
@click.option("--legi-dir", default=None)
@click.option("--data-dir", default=None)
@click.pass_context
def fetch_fr_compat(ctx, norm_ids, discover_flag, force, legi_dir, data_dir):
    """[Deprecated] Use: legalize fetch -c fr"""
    console.print("[yellow]Deprecated: use 'legalize fetch -c fr'[/yellow]")
    ctx.invoke(
        fetch,
        norm_ids=norm_ids,
        country="fr",
        fetch_all_flag=discover_flag,
        catalog=False,
        force=force,
        data_dir=data_dir,
        legi_dir=legi_dir,
        limit=None,
    )


@cli.command("fetch-se", hidden=True)
@click.argument("sfs_numbers", nargs=-1)
@click.option("--discover", "discover_flag", is_flag=True)
@click.option("--force", is_flag=True)
@click.option("--data-dir", default=None)
@click.pass_context
def fetch_se_compat(ctx, sfs_numbers, discover_flag, force, data_dir):
    """[Deprecated] Use: legalize fetch -c se"""
    console.print("[yellow]Deprecated: use 'legalize fetch -c se'[/yellow]")
    ctx.invoke(
        fetch,
        norm_ids=sfs_numbers,
        country="se",
        fetch_all_flag=discover_flag,
        catalog=False,
        force=force,
        data_dir=data_dir,
        legi_dir=None,
        limit=None,
    )


@cli.command("bootstrap-fr", hidden=True)
@click.option("--legi-dir", required=True)
@click.option("--repo-path", default="../fr")
@click.option("--data-dir", default="../data-fr")
@click.option("--dry-run", is_flag=True)
@click.pass_context
def bootstrap_fr_compat(ctx, legi_dir, repo_path, data_dir, dry_run):
    """[Deprecated] Use: legalize bootstrap -c fr"""
    console.print("[yellow]Deprecated: use 'legalize bootstrap -c fr'[/yellow]")
    ctx.invoke(
        bootstrap,
        country="fr",
        repo_path=repo_path,
        data_dir=data_dir,
        legi_dir=legi_dir,
        xml_path=None,
        dry_run=dry_run,
    )


@cli.command("bootstrap-se", hidden=True)
@click.option("--repo-path", default="../se")
@click.option("--data-dir", default="../data-se")
@click.option("--dry-run", is_flag=True)
@click.pass_context
def bootstrap_se_compat(ctx, repo_path, data_dir, dry_run):
    """[Deprecated] Use: legalize bootstrap -c se"""
    console.print("[yellow]Deprecated: use 'legalize bootstrap -c se'[/yellow]")
    ctx.invoke(
        bootstrap,
        country="se",
        repo_path=repo_path,
        data_dir=data_dir,
        legi_dir=None,
        xml_path=None,
        dry_run=dry_run,
    )
