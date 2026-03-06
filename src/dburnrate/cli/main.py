from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from ..core.models import ClusterConfig
from ..core.pricing import AZURE_INSTANCE_DBU
from ..estimators.static import CostEstimator
from ..estimators.whatif import apply_photon_scenario, apply_serverless_migration
from ..parsers.notebooks import parse_dbc, parse_notebook

app = typer.Typer(help="dburnrate - Pre-execution cost estimation for Databricks")
console = Console()


@app.command()
def estimate(
    query: str = typer.Argument(..., help="SQL query or path to .sql/.ipynb/.dbc file"),
    cluster_type: str = typer.Option(
        "Standard_DS3_v2", "--cluster", "-c", help="Instance type"
    ),
    workers: int = typer.Option(2, "--workers", "-w", help="Number of workers"),
    photon: bool = typer.Option(False, "--photon", help="Enable Photon"),
    currency: str = typer.Option("USD", "--currency", help="Output currency (USD/EUR)"),
    output: str = typer.Option(
        "table", "--output", "-o", help="Output format: table, json, text"
    ),
):
    query_path = Path(query)
    if query_path.exists():
        if query_path.suffix == ".sql":
            query = query_path.read_text()
        elif query_path.suffix == ".ipynb":
            cells = parse_notebook(query_path)
            query = "\n\n".join(c.source for c in cells if c.language == "sql")
        elif query_path.suffix == ".dbc":
            cells = parse_dbc(query_path)
            query = "\n\n".join(c.source for c in cells if c.language == "sql")

    dbu_rate = AZURE_INSTANCE_DBU.get(cluster_type, 0.75)
    cluster = ClusterConfig(
        instance_type=cluster_type,
        num_workers=workers,
        dbu_per_hour=dbu_rate,
        photon_enabled=photon,
    )

    estimator = CostEstimator(cluster=cluster, target_currency=currency)
    result = estimator.estimate(query)

    if output == "json":
        import json

        console.print(json.dumps(result.model_dump(), indent=2))
    elif output == "text":
        console.print(f"Estimated DBU: {result.estimated_dbu}")
        cost_col = f"estimated_cost_{currency.lower()}"
        cost_val = getattr(result, cost_col, None) or result.estimated_cost_usd
        console.print(f"Estimated Cost ({currency}): ${cost_val}")
        console.print(f"Confidence: {result.confidence}")
    else:
        table = Table(title="Cost Estimate")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Estimated DBU", str(result.estimated_dbu))
        cost_col = f"estimated_cost_{currency.lower()}"
        cost_val = getattr(result, cost_col, None) or result.estimated_cost_usd
        table.add_row(f"Estimated Cost ({currency})", f"${cost_val}")
        table.add_row("Confidence", result.confidence)
        console.print(table)


@app.command()
def whatif(
    query: str = typer.Argument(..., help="SQL query to model scenarios for"),
    scenario: str = typer.Option(
        ..., "--scenario", "-s", help="Scenario: photon, serverless, resize"
    ),
    cluster_type: str = typer.Option("Standard_DS3_v2", "--cluster", "-c"),
    workers: int = typer.Option(2, "--workers", "-w"),
    utilization: float = typer.Option(
        50.0, "--utilization", help="Cluster utilization %"
    ),
):
    cluster = ClusterConfig(
        instance_type=cluster_type,
        num_workers=workers,
        dbu_per_hour=AZURE_INSTANCE_DBU.get(cluster_type, 0.75),
    )

    estimator = CostEstimator(cluster=cluster)
    base = estimator.estimate(query)

    if scenario == "photon":
        result = apply_photon_scenario(base, "complex_join")
    elif scenario == "serverless":
        result = apply_serverless_migration(base, "ALL_PURPOSE", utilization)
    else:
        console.print(f"[red]Unknown scenario: {scenario}[/red]")
        raise typer.Exit(1)

    console.print(f"[bold]Base Cost:[/bold] ${base.estimated_cost_usd:.4f}")
    console.print(f"[bold]With {scenario}:[/bold] ${result.estimated_cost_usd:.4f}")

    if result.warnings:
        for w in result.warnings:
            console.print(f"[yellow]Warning:[/yellow] {w}")


@app.command()
def version():
    """Print version info."""
    from .. import __version__

    console.print(f"dburnrate v{__version__}")


if __name__ == "__main__":
    app()
