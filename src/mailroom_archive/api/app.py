"""FastAPI app. `create_app(settings)` wires Cassandra; tests pass `deps` with fakes instead."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from fastapi import Depends, FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from ..blobs import FsBlobStore
from ..settings import Settings
from ..spool import Spool
from ..store.arrivals import ArrivalStore
from ..store.session import StoreUnavailable, connect, run
from ..store.timeline import TimelineStore
from ..writer.pool import ShardPool
from . import routes_events, routes_read
from .auth import require_read
from .metrics import Metrics

_READY = SimpleStatement("SELECT now() FROM system.local", consistency_level=ConsistencyLevel.QUORUM)


@dataclass
class Deps:
    store: Any
    pool: Any
    arrivals: Any | None
    blobs: Any
    session: Any | None = None
    ready: Callable[[], Awaitable[bool]] | None = None


def cassandra_deps(settings: Settings) -> Deps:
    session = connect(
        settings.cassandra_hosts, username=settings.cassandra_username, password=settings.cassandra_password
    )

    async def ready() -> bool:
        try:
            await run(session, _READY)
            return True
        except StoreUnavailable:
            return False

    settings.data_dir.mkdir(parents=True, exist_ok=True)
    blobs = FsBlobStore(settings.blob_dir)
    store = TimelineStore(session, settings.keyspace)
    pool = ShardPool(
        store,
        Spool(settings.spool_path),
        shards=settings.shards,
        queue_size=settings.queue_size,
        blobs=blobs,
        health=ready,
    )
    return Deps(
        store=store,
        pool=pool,
        arrivals=ArrivalStore(session, settings.keyspace, blobs),
        blobs=blobs,
        session=session,
        ready=ready,
    )


def create_app(settings: Settings, *, deps: Deps | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.deps is None:
            app.state.deps = cassandra_deps(settings)
            app.state.metrics = Metrics(app.state.deps)
        await app.state.deps.pool.start()
        try:
            yield
        finally:
            await app.state.deps.pool.stop()
            if app.state.deps.session is not None:
                app.state.deps.session.cluster.shutdown()

    app = FastAPI(title="mailroom-archive", lifespan=lifespan)
    app.state.settings, app.state.deps = settings, deps
    app.state.metrics = Metrics(deps) if deps is not None else None
    app.include_router(routes_events.router)
    app.include_router(routes_read.router)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @app.get("/readyz", dependencies=[Depends(require_read)])
    async def readyz(request: Request) -> Response:
        ready = request.app.state.deps.ready
        ok = True if ready is None else await ready()
        return Response(json.dumps({"ok": ok}), status_code=200 if ok else 503, media_type="application/json")

    @app.get("/metrics", dependencies=[Depends(require_read)])
    async def metrics(request: Request) -> Response:
        return Response(generate_latest(request.app.state.metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return app
