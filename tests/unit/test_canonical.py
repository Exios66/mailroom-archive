import json

import pytest

from mailroom_archive.core.canonical import canonical_bytes


def test_canonical_bytes_sorted_compact():
    assert canonical_bytes({"b": 1, "a": [2, {"d": 3, "c": 4}]}) == b'{"a":[2,{"c":4,"d":3}],"b":1}'


def test_canonical_bytes_unicode_stable():
    s = {"name": "Société Générale — §12", "f": "📎 claim.pdf"}
    out = canonical_bytes(s)
    assert out == '{"f":"📎 claim.pdf","name":"Société Générale — §12"}'.encode()
    assert canonical_bytes(json.loads(out)) == out


def test_float_canonical_round_trip():
    for v in (0.1, 1e-7, 123456.789, -0.0):
        out = canonical_bytes({"v": v})
        assert canonical_bytes(json.loads(out)) == out


def test_canonical_rejects_nan():
    with pytest.raises(ValueError):
        canonical_bytes({"v": float("nan")})
