"""Cassandra session and the one async bridge every store call goes through."""

from __future__ import annotations

import asyncio
from typing import Any

from cassandra import (
    ConsistencyLevel,
    CoordinationFailure,
    OperationTimedOut,
    ReadTimeout,
    Unavailable,
    WriteTimeout,
)
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, NoHostAvailable, Session
from cassandra.connection import ConnectionException  # noqa: E402
from cassandra.query import dict_factory

_DRIVER_OUTAGES = (
    NoHostAvailable,
    Unavailable,
    ReadTimeout,
    WriteTimeout,
    CoordinationFailure,  # ReadFailure, WriteFailure
    OperationTimedOut,
    ConnectionException,
)


class StoreUnavailable(Exception):
    """Cassandra could not take or serve the request; callers spool or retry."""


def connect(hosts: list[str], *, username: str | None = None, password: str | None = None) -> Session:
    profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.QUORUM, row_factory=dict_factory, request_timeout=10
    )
    auth = PlainTextAuthProvider(username, password) if username else None
    cluster = Cluster(
        hosts, protocol_version=5, auth_provider=auth, execution_profiles={EXEC_PROFILE_DEFAULT: profile}
    )
    try:
        return cluster.connect()
    except _DRIVER_OUTAGES as e:
        cluster.shutdown()
        raise StoreUnavailable(str(e)) from e


async def run(session: Session, stmt: Any, params: tuple | dict = ()) -> list[dict]:
    """Execute without blocking the event loop; follows paging; maps outages to StoreUnavailable."""
    loop = asyncio.get_running_loop()
    done: asyncio.Future[list[dict]] = loop.create_future()
    rows: list[dict] = []
    try:
        rf = session.execute_async(stmt, params or None)
    except _DRIVER_OUTAGES as e:
        raise StoreUnavailable(str(e)) from e

    def ok(page: list[dict] | None) -> None:
        try:
            rows.extend(page or ())  # writes return no rows
            if rf.has_more_pages:
                rf.start_fetching_next_page()
                return
        except Exception as e:  # never leave the awaiting coroutine hanging
            err(e)
            return
        _deliver(lambda: done.done() or done.set_result(rows))

    def err(exc: BaseException) -> None:
        _deliver(lambda: done.done() or done.set_exception(exc))

    def _deliver(fn) -> None:
        try:
            loop.call_soon_threadsafe(fn)
        except RuntimeError:  # loop already closed (shutdown); nobody is waiting
            pass

    rf.add_callbacks(ok, err)
    try:
        return await done
    except _DRIVER_OUTAGES as e:
        raise StoreUnavailable(str(e)) from e


def quorum_probe(session: Session, keyspace: str):
    """Readiness: a QUORUM range read on the keyspace. system.local is node-local, so it
    would answer even with every other replica down."""
    from cassandra.query import SimpleStatement

    stmt = SimpleStatement(
        f"SELECT name FROM {keyspace}.cursors LIMIT 1", consistency_level=ConsistencyLevel.QUORUM
    )

    async def ready() -> bool:
        try:
            await run(session, stmt)
            return True
        except StoreUnavailable:
            return False

    return ready
