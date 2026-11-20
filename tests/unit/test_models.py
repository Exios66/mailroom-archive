import pytest
from pydantic import ValidationError

from mailroom_archive.core.ids import event_id_us, new_arrival_id, new_event_id
from mailroom_archive.core.kinds import CLIENT_KINDS, Kind
from mailroom_archive.core.models import EventIn


def mk(**kw):
    base = dict(doc_id="d", kind="arrived", actor="a", source="s", source_ref="r", occurred_us=0, payload={})
    return EventIn(**{**base, **kw})


def test_doc_id_length_bounds():
    with pytest.raises(ValidationError):
        mk(doc_id="")
    with pytest.raises(ValidationError):
        mk(doc_id="x" * 129)
    assert mk(doc_id="fixture-doc-a").doc_id == "fixture-doc-a"


def test_unknown_kind_rejected():
    with pytest.raises(ValidationError):
        mk(kind="teleported")


def test_negative_time_rejected():
    with pytest.raises(ValidationError):
        mk(occurred_us=-1)


def test_kinds_closed_set_and_client_subset():
    assert {k.value for k in Kind} == {
        "arrived", "stage_changed", "node_visit", "trace_io", "audit_row", "audit_attempt",
        "audit_addendum", "pending_row", "correction", "conflict", "annotation", "relation", "retention"}
    assert Kind.conflict not in CLIENT_KINDS and Kind.retention not in CLIENT_KINDS
    assert len(CLIENT_KINDS) == 11


def test_event_id_is_time_ordered():
    a, b = new_event_id(), new_event_id()
    assert a.version == 1 and event_id_us(a) <= event_id_us(b)


def test_arrival_id_is_uuid7_string():
    a = new_arrival_id()
    assert isinstance(a, str) and a[14] == "7"
