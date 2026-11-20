import json

import pytest

from mailroom_archive.core.models import EventIn
from mailroom_archive.core.state import Head, plan_state


def ev(kind, payload=None, source_ref="r", **kw):
    return EventIn(doc_id="d", kind=kind, actor="a", source="s", source_ref=source_ref,
                   occurred_us=1_763_650_000_000_000, payload=payload or {}, **kw)


def head(**fields):
    return Head.empty().replace(seq=5, **fields)


def test_stage_changed_sets_stage_and_type():
    payload = {"from_stage": "inbox", "to_stage": "classified", "doc_type": "contract"}
    upd, extra = plan_state(Head.empty(), ev("stage_changed", payload), 1)
    assert upd.fields["stage"] == "classified" and upd.fields["doc_type"] == "contract" and extra == []
    assert "stage_at" in upd.fields


def test_archive_correction_overrides_and_records_seq():
    upd, _ = plan_state(head(doc_type="contract"), ev("correction", {
        "origin": "archive", "field_path": "doc_type", "new_value": "merger_agreement",
        "reason": "r", "reviewer": "jb", "corrects_seq": 3}), 7)
    assert upd.fields["doc_type"] == "merger_agreement" and upd.fields["overrides"] == {"doc_type": 7}


def test_pipeline_disagreeing_with_override_keeps_archive_value_and_emits_conflict():
    h = head(doc_type="merger_agreement", overrides={"doc_type": 7})
    upd, extra = plan_state(h, ev("stage_changed", {"to_stage": "archived", "doc_type": "contract"},
                                  source_ref="documents:d:9"), 8)
    assert "doc_type" not in upd.fields and upd.fields["stage"] == "archived"
    assert [e.kind for e in extra] == ["conflict"]
    assert extra[0].payload == {"field_path": "doc_type", "override_seq": 7,
                                "archive_value": "merger_agreement", "pipeline_value": "contract",
                                "pipeline_source_ref": "documents:d:9"}
    assert extra[0].source == "writer" and extra[0].source_ref == "conflict:documents:d:9:doc_type"


def test_pipeline_agreeing_with_override_is_not_a_conflict():
    h = head(doc_type="contract", overrides={"doc_type": 7})
    _, extra = plan_state(h, ev("stage_changed", {"to_stage": "archived", "doc_type": "contract"}), 8)
    assert extra == []


def test_release_removes_override():
    upd, _ = plan_state(head(doc_type="x", overrides={"doc_type": 7}), ev("correction", {
        "origin": "archive", "field_path": "doc_type", "release": True, "reason": "r", "reviewer": "jb"}), 9)
    assert upd.fields["overrides"] == {} and "doc_type" not in upd.fields


def test_extracted_field_override_survives_audit_row():
    h = head(extracted_data={"policy_no": "A1"}, overrides={"extracted_data.policy_no": 4})
    extracted = {"policy_no": "B2", "amount": 10}
    row = {"entry_hash": "ab" * 32, "detail": {"extraction": {"extracted_data": extracted}}}
    upd, extra = plan_state(h, ev("audit_row", row), 5)
    assert json.loads(upd.fields["extracted_data"]) == {"policy_no": "A1", "amount": 10}
    assert upd.fields["audit_entry_hash"] == bytes.fromhex("ab" * 32)
    assert extra[0].payload["field_path"] == "extracted_data.policy_no"


def test_archive_correction_of_nested_extracted_field():
    h = head(extracted_data={"insured": {"name": "Acme"}})
    upd, _ = plan_state(h, ev("correction", {"origin": "archive", "field_path": "extracted_data.insured.name",
                                             "new_value": "Acme Ltd", "reason": "r", "reviewer": "jb"}), 6)
    assert json.loads(upd.fields["extracted_data"]) == {"insured": {"name": "Acme Ltd"}}
    assert upd.fields["overrides"] == {"extracted_data.insured.name": 6}


def test_correction_of_unknown_field_rejected():
    with pytest.raises(ValueError):
        plan_state(Head.empty(), ev("correction", {"origin": "archive", "field_path": "stage",
                                                   "new_value": "x", "reason": "r", "reviewer": "jb"}), 1)


def test_arrived_sets_content_and_created_once():
    upd, _ = plan_state(Head.empty(), ev("arrived", {"content_sha256": "cd" * 32, "channel": "upload",
                                                     "original_filename": "a.pdf"}, matter_id="M1"), 1)
    assert upd.fields["content_sha256"] == bytes.fromhex("cd" * 32) and upd.fields["channel"] == "upload"
    assert upd.fields["matter_id"] == "M1" and "created_at" in upd.fields
    upd2, _ = plan_state(head(), ev("arrived", {"content_sha256": "cd" * 32, "channel": "upload"}), 6)
    assert "created_at" not in upd2.fields


def test_annotation_changes_no_state():
    upd, extra = plan_state(head(), ev("annotation", {"note": "n"}), 6)
    assert upd.fields == {} and extra == []
