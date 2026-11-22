"""`mailroom-archive` command line: schema, serve, verify."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Annotated

import structlog
import typer

from .blobs import FsBlobStore
from .settings import Settings

app = typer.Typer(no_args_is_help=True, add_completion=False)
schema_app = typer.Typer(no_args_is_help=True, help="Cassandra schema.")
verify_app = typer.Typer(no_args_is_help=True, help="Recompute hash chains.")
app.add_typer(schema_app, name="schema")
app.add_typer(verify_app, name="verify")


def _json_logs() -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper("iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )


@schema_app.command("apply")
def schema_apply(
    profile: str = typer.Option("small", help="single | small | full"),
    keyspace: str | None = typer.Option(None, help="defaults to MAILROOM_ARCHIVE_KEYSPACE"),
) -> None:
    """Create the keyspace and every table (idempotent)."""
    from .store.schema import apply_schema
    from .store.session import connect

    if profile not in ("single", "small", "full"):
        raise typer.BadParameter("profile must be single, small or full")
    s = Settings()
    session = connect(s.cassandra_hosts, username=s.cassandra_username, password=s.cassandra_password)
    try:
        tables = apply_schema(session, keyspace or s.keyspace, profile)  # type: ignore[arg-type]
    finally:
        session.cluster.shutdown()
    typer.echo(f"{keyspace or s.keyspace}: {len(tables)} tables")


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8544) -> None:
    """Run the ingest and read API."""
    import uvicorn

    from .api.app import create_app

    _json_logs()
    uvicorn.run(create_app(Settings()), host=host, port=port, log_config=None)


@app.command()
def follow(
    sqlite: Annotated[Path, typer.Option(exists=True, file_okay=True, dir_okay=False, readable=True)],
    name: str = typer.Option(..., help="Stable, unique identity for this pipeline database."),
    archive_url: str = typer.Option("http://127.0.0.1:8544", help="Running archive ingest API."),
    once: bool = typer.Option(False, help="Run one bounded pass and exit; otherwise poll every second."),
) -> None:
    """Follow a mailroom SQLite database read-only; checkpoint in Cassandra."""
    import sqlite3

    import httpx

    from .follower.runner import ArchiveClient, Follower
    from .follower.sqlite import SourceChanged, SQLiteSource
    from .store.cursors import CursorStore
    from .store.session import StoreUnavailable, connect

    if not name.strip():
        raise typer.BadParameter("name must not be blank")
    settings = Settings()
    if not settings.token:
        raise typer.BadParameter("set MAILROOM_ARCHIVE_TOKEN for ingest")
    _json_logs()

    async def run() -> None:
        session = connect(
            settings.cassandra_hosts,
            username=settings.cassandra_username,
            password=settings.cassandra_password,
        )
        try:
            async with httpx.AsyncClient(
                base_url=archive_url, headers={"Authorization": f"Bearer {settings.token}"}, timeout=10
            ) as client:
                follower = Follower(
                    SQLiteSource(sqlite),
                    ArchiveClient(client),
                    CursorStore(session, settings.keyspace),
                    name=name,
                )
                while True:
                    try:
                        result = await follower.poll_once()
                    except (httpx.HTTPError, StoreUnavailable, sqlite3.Error, SourceChanged) as exc:
                        if once:
                            raise
                        structlog.get_logger().warning(
                            "archive_follower_retry", error_type=type(exc).__name__
                        )
                    else:
                        structlog.get_logger().info(
                            "archive_follower_polled", rows=result.rows, events=result.events
                        )
                    if once:
                        return
                    await asyncio.sleep(1)
        finally:
            session.cluster.shutdown()

    asyncio.run(run())


@verify_app.command("doc")
def verify_doc(doc_id: str, as_json: bool = typer.Option(False, "--json")) -> None:
    """Verify one document's chain; exits 1 when any check fails."""
    from .store.session import connect
    from .store.timeline import TimelineStore
    from .verify import verify_events

    s = Settings()
    session = connect(s.cassandra_hosts, username=s.cassandra_username, password=s.cassandra_password)
    try:
        events = asyncio.run(TimelineStore(session, s.keyspace).timeline(doc_id))
    finally:
        session.cluster.shutdown()
    report = verify_events(doc_id, events, FsBlobStore(s.blob_dir))
    if as_json:
        typer.echo(report.model_dump_json())
    else:
        typer.echo(f"{doc_id}: {'ok' if report.ok else 'FAILED'} ({report.events} events)")
        for c in report.checks:
            if not c.ok:
                typer.echo(f"  {c.name} seq={c.seq} {c.detail or ''}")
    raise typer.Exit(0 if report.ok else 1)
