"""Fixtures shared by the integration and fault suites: one fresh keyspace per session."""

import os
import secrets

import pytest

from mailroom_archive.store.schema import apply_schema
from mailroom_archive.store.session import connect
from mailroom_archive.store.timeline import TimelineStore

PROFILE = os.environ.get("ARCHIVE_PROFILE", "single")


@pytest.fixture(scope="session")
def ks_session():
    session = connect(os.environ["CASSANDRA_HOSTS"].split(","))
    yield session
    session.cluster.shutdown()


@pytest.fixture(scope="session")
def ks(ks_session):
    name = "it_" + secrets.token_hex(4)
    apply_schema(ks_session, name, PROFILE)
    yield name
    ks_session.execute(f"DROP KEYSPACE IF EXISTS {name}", timeout=60)


@pytest.fixture(scope="session")
def store(ks_session, ks):
    return TimelineStore(ks_session, ks)
