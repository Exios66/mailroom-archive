"""Wire and storage models."""

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from .kinds import Kind


class EventIn(BaseModel):
    doc_id: str = Field(min_length=1, max_length=128)
    kind: Kind
    actor: str
    source: str
    source_ref: str
    occurred_us: int = Field(ge=0)
    payload: dict[str, Any]
    matter_id: str | None = None
    sim_run_id: str | None = None


class StoredEvent(BaseModel):
    doc_id: str
    seq: int
    event_id: UUID
    kind: Kind
    actor: str
    source: str
    source_ref: str
    occurred_us: int
    payload: bytes
    payload_ref: str | None = None
    payload_sha256: bytes
    prev_hash: bytes
    event_hash: bytes
    corrects_seq: int | None = None


class WriteResult(BaseModel):
    doc_id: str
    status: Literal["written", "duplicate", "spooled"]
    seq: int | None = None
    event_hash: str | None = None
