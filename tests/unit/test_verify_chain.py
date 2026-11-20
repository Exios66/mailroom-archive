import pytest
from factories import stored

from mailroom_archive.blobs import FsBlobStore
from mailroom_archive.verify import verify_events


def chain(n, spill_seq=None, blobs=None):
    evs, prev = [], b""
    for seq in range(1, n + 1):
        e = stored(doc_id="d", seq=seq, payload={"x": seq, "note": "Générale"}, prev_hash=prev)
        if seq == spill_seq:
            info = blobs.put(e.payload)
            e = e.model_copy(update={"payload": b"", "payload_ref": "payload/" + info.sha256.hex()})
        evs.append(e)
        prev = e.event_hash
    return evs


def setattr_copy(evs, i, **changes):
    evs[i] = evs[i].model_copy(update=changes)


def blob_path(root, payload_ref):
    h = payload_ref.split("/")[1]
    return root / h[:2] / h[2:4] / h


def test_clean_chain_passes():
    rep = verify_events("d", chain(5), None)
    assert rep.ok and rep.events == 5
    assert {c.name for c in rep.checks} == {
        "seq_contiguous",
        "prev_link",
        "payload_hash",
        "event_hash",
        "payload_available",
    }
    assert all(c.ok for c in rep.checks)


def test_empty_timeline_is_not_ok():
    rep = verify_events("d", [], None)
    assert not rep.ok and rep.events == 0


@pytest.mark.parametrize(
    "tamper,check",
    [
        (lambda evs: evs.pop(2), "seq_contiguous"),
        (lambda evs: setattr_copy(evs, 3, payload=b'{"x":2}'), "payload_hash"),
        (lambda evs: setattr_copy(evs, 3, kind="annotation", actor="someone-else"), "event_hash"),
        (lambda evs: setattr_copy(evs, 4, prev_hash=b"\x00" * 32), "prev_link"),
    ],
)
def test_tamper_detected(tamper, check):
    evs = chain(5)
    tamper(evs)
    rep = verify_events("d", evs, None)
    assert not rep.ok and any(c.name == check and not c.ok for c in rep.checks)


def test_failure_names_the_seq():
    evs = chain(5)
    setattr_copy(evs, 3, payload=b'{"x":2}')
    bad = [c for c in verify_events("d", evs, None).checks if not c.ok]
    assert [(c.name, c.seq) for c in bad] == [("payload_hash", 4)]


def test_spilled_payload_checked_against_blob(tmp_path):
    blobs = FsBlobStore(tmp_path)
    evs = chain(3, spill_seq=2, blobs=blobs)
    assert verify_events("d", evs, blobs).ok
    blob_path(tmp_path, evs[1].payload_ref).unlink()
    rep = verify_events("d", evs, blobs)
    assert not rep.ok and any(c.name == "payload_available" and c.seq == 2 and not c.ok for c in rep.checks)


def test_report_is_json_serializable():
    assert '"ok":true' in verify_events("d", chain(2), None).model_dump_json()
