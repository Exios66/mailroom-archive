"""Cassandra session and the one async bridge every store call goes through."""

from __future__ import annotations

import asyncio
from typing import Any

from cassandra import ConsistencyLevel, OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, NoHostAvailable, Session
from cassandra.query import dict_factory

_DRIVER_OUTAGES = (NoHostAvailable, Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut)


class StoreUnavailable(Exception):
    """Cassandra could not take or serve the request; callers spool or retry."""


def connect(hosts: list[str], *, username: str | None = None, password: str | None = None) -> Session:
    profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, row_factory=dict_factory,
                               request_timeout=10)
    auth = PlainTextAuthProvider(username, password) if username else None
    cluster = Cluster(hosts, protocol_version=5, auth_provider=auth,
                      execution_profiles={EXEC_PROFILE_DEFAULT: profile})
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

    def ok(page: list[dict]) -> None:
        rows.extend(page)
        if rf.has_more_pages:
            rf.start_fetching_next_page()
        else:
            loop.call_soon_threadsafe(lambda: done.done() or done.set_result(rows))

    def err(exc: BaseException) -> None:
        loop.call_soon_threadsafe(lambda: done.done() or done.set_exception(exc))

    rf.add_callbacks(ok, err)
    try:
        return await done
    except _DRIVER_OUTAGES as e:
        raise StoreUnavailable(str(e)) from e
