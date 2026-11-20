"""Current-state rules (spec §6.5a), free of I/O.

Every event may change the document's static columns. Archive corrections override
pipeline values; a later pipeline value that disagrees keeps the override and produces
a `conflict` event instead.
"""

from __future__ import annotations

import copy
import dataclasses
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .canonical import canonical_bytes
from .kinds import Kind
from .models import EventIn

CORRECTABLE_TOP = frozenset({"doc_type", "doc_subclass", "matter_id"})
EXTRACTED = "extracted_data"


@dataclass(frozen=True)
class Head:
    seq: int
    hash: bytes
    matter_id: str | None = None
    doc_type: str | None = None
    doc_subclass: str | None = None
    stage: str | None = None
    stage_us: int | None = None
    content_sha256: bytes | None = None
    channel: str | None = None
    original_filename: str | None = None
    extracted_data: dict | None = None
    overrides: dict[str, int] = field(default_factory=dict)
    audit_entry_hash: bytes | None = None

    @classmethod
    def empty(cls) -> Head:
        return cls(seq=0, hash=b"")

    def replace(self, **changes: Any) -> Head:
        return dataclasses.replace(self, **changes)


@dataclass(frozen=True)
class StateUpdate:
    fields: dict[str, Any]


def _ts(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


def _check_path(path: str) -> None:
    if path in CORRECTABLE_TOP:
        return
    if path.startswith(EXTRACTED + ".") and all(path.split(".")[1:]):
        return
    raise ValueError(f"field_path not correctable: {path!r}")


def _get(d: dict | None, keys: list[str]) -> Any:
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def _set(d: dict, keys: list[str], value: Any) -> None:
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = value


class _Planner:
    def __init__(self, head: Head, ev: EventIn, seq: int):
        self.head, self.ev, self.seq = head, ev, seq
        self.fields: dict[str, Any] = {}
        self.extra: list[EventIn] = []
        self.overrides = dict(head.overrides)
        self.extracted: dict | None = None  # working copy when touched

    # current value as of this plan (pending updates win over the head)
    def current(self, path: str) -> Any:
        if path in CORRECTABLE_TOP:
            return self.fields.get(path, getattr(self.head, path))
        data = self.extracted if self.extracted is not None else self.head.extracted_data
        return _get(data, path.split(".")[1:])

    def set_value(self, path: str, value: Any) -> None:
        if path in CORRECTABLE_TOP:
            self.fields[path] = value
            return
        if self.extracted is None:
            self.extracted = copy.deepcopy(self.head.extracted_data) or {}
        _set(self.extracted, path.split(".")[1:], value)

    def pipeline_value(self, path: str, value: Any) -> None:
        """A value reported by the pipeline: applied unless an archive override holds the field."""
        if path not in self.overrides:
            if value != self.current(path):
                self.set_value(path, value)
            return
        archive_value = self.current(path)
        if value != archive_value:
            self.extra.append(EventIn(
                doc_id=self.ev.doc_id, kind=Kind.conflict, actor="archive", source="writer",
                source_ref=f"conflict:{self.ev.source_ref}:{path}", occurred_us=self.ev.occurred_us,
                payload={"field_path": path, "override_seq": self.overrides[path],
                         "archive_value": archive_value, "pipeline_value": value,
                         "pipeline_source_ref": self.ev.source_ref},
                matter_id=None, sim_run_id=self.ev.sim_run_id))

    def finish(self) -> tuple[StateUpdate, list[EventIn]]:
        if self.extracted is not None:
            self.fields["extracted_data"] = canonical_bytes(self.extracted)
        if self.overrides != self.head.overrides:
            self.fields["overrides"] = self.overrides
        return StateUpdate(self.fields), self.extra


def plan_state(head: Head, ev: EventIn, seq: int) -> tuple[StateUpdate, list[EventIn]]:
    p = _Planner(head, ev, seq)
    pl = ev.payload

    if head.seq == 0:
        p.fields["created_at"] = _ts(ev.occurred_us)
        if ev.sim_run_id:
            p.fields["sim_run_id"] = ev.sim_run_id
    if ev.matter_id is not None:
        p.pipeline_value("matter_id", ev.matter_id)

    if ev.kind == Kind.stage_changed:
        p.fields["stage"] = pl["to_stage"]
        p.fields["stage_at"] = _ts(ev.occurred_us)
        for f in ("doc_type", "doc_subclass"):
            if pl.get(f) is not None:
                p.pipeline_value(f, pl[f])

    elif ev.kind == Kind.arrived:
        if pl.get("content_sha256"):
            p.fields["content_sha256"] = bytes.fromhex(pl["content_sha256"])
        for f in ("channel", "original_filename"):
            if pl.get(f) is not None:
                p.fields[f] = pl[f]

    elif ev.kind == Kind.audit_row:
        if pl.get("entry_hash"):
            p.fields["audit_entry_hash"] = bytes.fromhex(pl["entry_hash"])
        extracted = _get(pl, ["detail", "extraction", "extracted_data"])
        if isinstance(extracted, dict):
            # overridden paths are checked first so the new dict cannot hide a conflict
            for path in [o for o in p.overrides if o.startswith(EXTRACTED + ".")]:
                new = _get(extracted, path.split(".")[1:])
                if new is not None:
                    p.pipeline_value(path, new)
            merged = copy.deepcopy(extracted)
            for path in [o for o in p.overrides if o.startswith(EXTRACTED + ".")]:
                kept = p.current(path)
                if kept is not None:
                    _set(merged, path.split(".")[1:], kept)
            if merged != head.extracted_data:
                p.extracted = merged

    elif ev.kind == Kind.correction:
        path = pl["field_path"]
        _check_path(path)
        if pl.get("origin") == "archive":
            if pl.get("release"):
                p.overrides.pop(path, None)
            else:
                p.set_value(path, pl["new_value"])
                p.overrides[path] = seq
        else:
            p.pipeline_value(path, pl["new_value"])

    return p.finish()
