import hashlib

from mailroom_archive.core.hashing import HASH_VERSION, event_hash, sha256


def test_sha256_is_raw_digest():
    assert sha256(b"x") == hashlib.sha256(b"x").digest() and len(sha256(b"")) == 32


def test_event_hash_matches_documented_form():
    assert HASH_VERSION == "a1"
    ph = bytes.fromhex("11" * 32)
    got = event_hash(doc_id="doc-1", seq=1, kind="arrived", occurred_us=1763650000000000,
                     actor="probe", source="inbox", source_ref="inbox:a", payload_sha256=ph, prev_hash=b"")
    expected = ('{"actor":"probe","doc_id":"doc-1","hash_version":"a1","kind":"arrived",'
                '"occurred_us":1763650000000000,"payload_sha256":"' + "11" * 32 + '",'
                '"prev_hash":"","seq":1,"source":"inbox","source_ref":"inbox:a"}')
    assert got == hashlib.sha256(expected.encode()).digest()


def test_event_hash_changes_with_any_field():
    base = dict(doc_id="d", seq=2, kind="correction", occurred_us=1, actor="a", source="s",
                source_ref="r", payload_sha256=b"\x00" * 32, prev_hash=b"\x01" * 32)
    h = event_hash(**base)
    for k, v in [("seq", 3), ("kind", "annotation"), ("occurred_us", 2), ("prev_hash", b"\x02" * 32),
                 ("doc_id", "e"), ("actor", "b"), ("source", "t"), ("source_ref", "q"),
                 ("payload_sha256", b"\x03" * 32)]:
        assert event_hash(**{**base, k: v}) != h, k
