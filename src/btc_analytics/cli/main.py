from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import click

from btc_analytics.config import load_settings
from btc_analytics.core.pipeline import ParsePipeline
from btc_analytics.db.connection import get_connection, run_sql_file
from btc_analytics.db.repository import Repository
from btc_analytics.logging_utils import configure_logging


@click.group()
def cli() -> None:
    """Bitcoin analytics CLI."""


@cli.command("init-db")
def init_db() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    schema_path = Path(__file__).resolve().parents[3] / "sql" / "schema.sql"
    with get_connection(settings.database_url) as conn:
        run_sql_file(conn, str(schema_path))
        conn.commit()
    click.echo("Database schema initialized")


@cli.command("load-price")
@click.option("--date", "day", required=True, help="Date YYYY-MM-DD")
@click.option("--price", "price", required=True, type=float, help="BTC/USD close or spot")
@click.option("--source", default="manual", show_default=True)
def load_price(day: str, price: float, source: str) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    parsed_day = date.fromisoformat(day)

    with get_connection(settings.database_url) as conn:
        repo = Repository(conn)
        repo.upsert_price(parsed_day, Decimal(str(price)), source)
        conn.commit()
    click.echo(f"Loaded price for {parsed_day}")


@cli.command("parse")
@click.option("--end-height", type=int, default=None, help="Optional ending block height")
def parse(end_height: int | None) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    pipeline = ParsePipeline(settings)
    parsed = pipeline.run_parse(end_height=end_height)
    click.echo(f"Parsed new blocks: {parsed}")


@cli.command("sync-initial")
@click.option("--start-height", type=int, default=0, show_default=True)
@click.option("--end-height", type=int, default=None, help="Optional ending block height")
def sync_initial(start_height: int, end_height: int | None) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    pipeline = ParsePipeline(settings)
    parsed = pipeline.run_parse(end_height=end_height, start_height_override=start_height)
    aggregated = pipeline.run_aggregate(from_day=None)
    click.echo(f"Initial sync complete: parsed_new_blocks={parsed} aggregated_days={aggregated}")


@cli.command("sync-resume")
@click.option("--end-height", type=int, default=None, help="Optional ending block height")
def sync_resume(end_height: int | None) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    pipeline = ParsePipeline(settings)
    parsed = pipeline.run_parse(end_height=end_height)
    aggregated = pipeline.run_aggregate(from_day=None)
    click.echo(f"Resume sync complete: parsed_new_blocks={parsed} aggregated_days={aggregated}")


@cli.command("aggregate")
@click.option("--from-date", type=str, default=None, help="Optional first day YYYY-MM-DD")
def aggregate(from_date: str | None) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    pipeline = ParsePipeline(settings)
    day = date.fromisoformat(from_date) if from_date else None
    count = pipeline.run_aggregate(day)
    click.echo(f"Aggregated days: {count}")


@cli.command("recompute-metrics")
@click.option("--from-date", required=True, type=str, help="Start date YYYY-MM-DD")
@click.option("--to-date", required=True, type=str, help="End date YYYY-MM-DD")
def recompute_metrics(from_date: str, to_date: str) -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    pipeline = ParsePipeline(settings)
    from_day = date.fromisoformat(from_date)
    to_day = date.fromisoformat(to_date)
    count = pipeline.run_aggregate_range(from_day, to_day)
    click.echo(f"Recomputed metrics days: {count}")


@cli.command("daily-update")
def daily_update() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    pipeline = ParsePipeline(settings)
    parsed, aggregated = pipeline.run_daily_incremental_update()
    click.echo(f"Daily update complete: parsed_new_blocks={parsed} aggregated_days={aggregated}")


@cli.command("status")
def status() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    with get_connection(settings.database_url) as conn:
        repo = Repository(conn)
        state = repo.get_parser_state(settings.parser_name)
        latest_metric_day = repo.get_latest_metric_day()
    click.echo(
        f"parser_name={settings.parser_name} last_processed_height={state.last_processed_height} "
        f"last_aggregated_day={state.last_aggregated_day} latest_metric_day={latest_metric_day}"
    )


if __name__ == "__main__":
    cli()
