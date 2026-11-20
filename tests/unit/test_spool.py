from mailroom_archive.core.models import EventIn
from mailroom_archive.spool import Spool


def ev(doc_id, kind="annotation", ref="r"):
    return EventIn(
        doc_id=doc_id,
        kind=kind,
        actor="a",
        source="s",
        source_ref=ref,
        occurred_us=1_763_650_000_000_000,
        payload={"note": "Société — §12"},
    )


def test_append_dedups_and_orders(tmp_path):
    sp = Spool(tmp_path / "s.db")
    assert sp.append(3, ev("d", ref="a")) and not sp.append(3, ev("d", ref="a"))
    sp.append(3, ev("d", ref="b"))
    assert [e.source_ref for _, e in sp.peek(3)] == ["a", "b"]
    assert sp.peek(3)[0][1] == ev("d", ref="a")


def test_survives_reopen(tmp_path):
    Spool(tmp_path / "s.db").append(1, ev("d", ref="a"))
    assert Spool(tmp_path / "s.db").has_pending(1)


def test_ack_and_stats(tmp_path):
    sp = Spool(tmp_path / "s.db")
    sp.append(1, ev("d", ref="a"))
    sp.append(2, ev("e", ref="b"))
    assert sp.pending_shards() == {1, 2} and sp.stats().events == 2 and sp.stats().oldest_us > 0
    sp.ack([i for i, _ in sp.peek(1)])
    assert sp.pending_shards() == {2} and not sp.has_pending(1)


def test_spool_under_tempdir_is_flagged_ephemeral(tmp_path):
    assert Spool(tmp_path / "s.db").ephemeral
