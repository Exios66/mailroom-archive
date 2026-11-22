"""Deterministic mapping; upstream judge scores and verdicts are never recomputed."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from ..core.canonical import canonical_bytes
from ..core.hashing import sha256
from ..core.kinds import Kind
from ..core.models import EventIn
from .sqlite import DOCUMENTS, Snapshot, Stream

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def timestamp_us(value: str) -> int:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta = dt - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def row_events(snapshot: Snapshot, stream: Stream, row: dict[str, Any], source: str) -> Iterator[EventIn]:
    def event(kind: Kind, ref: str, payload: dict, stamp: str | None = None) -> EventIn:
        return EventIn(
            doc_id=row["doc_id"],
            kind=kind,
            actor=row.get("actor") or "follower",
            source=source,
            source_ref=ref,
            occurred_us=timestamp_us(stamp or row.get("timestamp") or row["updated_at"]),
            payload=payload,
            matter_id=row.get("matter_id"),
        )

    if stream == DOCUMENTS:
        # One observed snapshot per revision, including its raw catalog fields.
        # Hash also distinguishes updates sharing a timestamp; replay is stable.
        revision = sha256(canonical_bytes(row)).hex()
        yield event(
            Kind.stage_changed,
            f"documents:{row['doc_id']}:{revision}",
            {
                "to_stage": row["stage"],
                "observed": True,
                **{
                    k: row[k]
                    for k in (
                        "doc_type",
                        "doc_subclass",
                        "classification_confidence",
                        "extraction_confidence",
                        "escalation_reason",
                    )
                    if k in row
                },
                "catalog_snapshot": row,
            },
        )
        return

    is_addendum = stream.name == "audit_addenda"
    identity = row["addendum_id" if is_addendum else "entry_id"]
    prefix = "audit_addenda" if is_addendum else "audit_legacy" if stream.legacy else "audit_log"
    ref = f"{prefix}:{identity}"
    payload = {**row, "legacy": True} if stream.legacy else row
    yield event(Kind.audit_addendum if is_addendum else Kind.audit_row, ref, payload)
    if stream.legacy:
        return
    for attempt in snapshot.attempts(identity):
        if attempt["doc_id"] != row["doc_id"]:
            raise ValueError("attempt parent and document disagree")
        yield event(
            Kind.audit_attempt,
            f"audit_attempts:{attempt['attempt_id']}",
            attempt,
            attempt.get("ended_at") or attempt.get("started_at"),
        )
    for i, visit in enumerate((row.get("detail") or {}).get("trace") or []):
        # Prompt/completion stay in the verbatim parent; node_visit is metadata.
        metadata = {k: v for k, v in visit.items() if k not in {"prompt", "completion"}}
        yield event(
            Kind.node_visit, f"{ref}:trace:{i}", metadata, visit.get("ended_at") or visit.get("started_at")
        )
