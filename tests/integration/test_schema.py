import pytest

from mailroom_archive.store.schema import apply_schema

pytestmark = pytest.mark.integration


def test_apply_schema_creates_all_tables(ks_session, ks):
    names = apply_schema(ks_session, ks, "single")
    assert set(names) == {"doc_timeline", "arrivals", "sightings_by_sha", "blobs", "events_by_time",
                          "trace_io", "pending_rows", "docs_by_matter", "idempotency", "seals",
                          "cursors", "sim_runs"}


def test_apply_schema_is_idempotent(ks_session, ks):
    apply_schema(ks_session, ks, "single")
    apply_schema(ks_session, ks, "single")
