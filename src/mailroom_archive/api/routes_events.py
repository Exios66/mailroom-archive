"""Write routes: events, corrections, annotations, arrivals, blob upload (spec §7.1)."""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from pydantic import BaseModel, ValidationError

from ..blobs import HashMismatch
from ..core.kinds import WRITER_ONLY, Kind
from ..core.models import EventIn, WriteResult
from ..core.state import Head, check_path, plan_state
from ..writer.pool import Backpressure
from .auth import require_write

log = structlog.get_logger()
router = APIRouter(dependencies=[Depends(require_write)])

MAX_EVENTS = 1000
MAX_BYTES = 4 * 1024 * 1024


def _now_us() -> int:
    return time.time_ns() // 1000


def _no_nan(token: str) -> None:
    raise ValueError(f"{token} is not valid JSON")


def _loads(data: bytes) -> Any:
    return json.loads(data, parse_constant=_no_nan)  # NaN/Infinity could not be hashed canonically


def _event(**fields: Any) -> EventIn:
    try:
        ev = EventIn(**fields)
    except ValidationError as e:
        raise HTTPException(422, e.errors(include_url=False, include_context=False)) from e
    _check_payloads([ev])
    return ev


def _check_payloads(events: list[EventIn]) -> None:
    """Reject, before anything is written, any payload its kind's state rule cannot apply."""
    for i, ev in enumerate(events):
        try:
            plan_state(Head.empty(), ev, 1)
        except (KeyError, ValueError, TypeError) as e:
            raise HTTPException(422, f"event {i}: {ev.kind} payload rejected: {e!r}") from e


def _parse_events(body: bytes, content_type: str) -> list[EventIn]:
    try:
        if "ndjson" in content_type:
            raw = [_loads(line) for line in body.splitlines() if line.strip()]
        else:
            raw = _loads(body)
    except (ValueError, UnicodeDecodeError) as e:
        raise HTTPException(422, f"body is not JSON or NDJSON: {e}") from e
    if not isinstance(raw, list):
        raise HTTPException(422, "body must be a JSON array of events")
    if len(raw) > MAX_EVENTS:
        raise HTTPException(413, f"at most {MAX_EVENTS} events per request")
    try:
        events = [EventIn.model_validate(r) for r in raw]
    except ValidationError as e:
        raise HTTPException(422, e.errors(include_url=False, include_context=False)) from e
    if bad := [i for i, e in enumerate(events) if e.kind in WRITER_ONLY]:
        raise HTTPException(422, f"kinds {sorted(WRITER_ONLY)} are writer-only (events {bad})")
    _check_payloads(events)
    return events


async def submit(request: Request, events: list[EventIn]) -> list[WriteResult]:
    st = request.app.state
    try:
        results = await st.deps.pool.submit_batch(events)
    except Backpressure as e:
        raise HTTPException(
            429, "shard queue full", headers={"Retry-After": str(math.ceil(e.retry_after))}
        ) from e
    for ev, r in zip(events, results, strict=True):
        st.metrics.ingested.labels(str(ev.kind), ev.source, r.status).inc()
        if r.status == "duplicate":
            st.metrics.duplicates.labels("event").inc()
    log.info(
        "archive_batch_written",
        events=len(events),
        written=sum(r.status == "written" for r in results),
        spooled=sum(r.status == "spooled" for r in results),
    )
    return results


@router.post("/v1/events", response_model=list[WriteResult])
async def post_events(request: Request) -> list[WriteResult]:
    with request.app.state.metrics.latency.time():
        body = await request.body()
        if len(body) > MAX_BYTES:
            raise HTTPException(413, f"at most {MAX_BYTES} bytes per request")
        return await submit(request, _parse_events(body, request.headers.get("content-type", "")))


def _api_ref(doc_id: str, idempotency_key: str | None) -> str:
    # scoped by document, so one client key reused on two documents names two events
    return f"api:{doc_id}:{idempotency_key or uuid.uuid4()}"


class CorrectionIn(BaseModel):
    field_path: str
    new_value: Any = None
    release: bool = False
    reason: str
    reviewer: str
    corrects_seq: int | None = None


@router.post("/v1/docs/{doc_id}/corrections", response_model=WriteResult)
async def post_correction(
    doc_id: str, body: CorrectionIn, request: Request, idempotency_key: str | None = Header(default=None)
) -> WriteResult:
    try:
        check_path(body.field_path)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    payload = {"origin": "archive", **body.model_dump(exclude={"corrects_seq", "new_value"})}
    if not body.release:
        payload["new_value"] = body.new_value  # null is a real value: it clears the field
    if body.corrects_seq is not None:
        payload["corrects_seq"] = body.corrects_seq
    ev = _event(
        doc_id=doc_id,
        kind=Kind.correction,
        actor=body.reviewer,
        source="api",
        source_ref=_api_ref(doc_id, idempotency_key),
        occurred_us=_now_us(),
        payload=payload,
    )
    return (await submit(request, [ev]))[0]


class AnnotationIn(BaseModel):
    note: str
    author: str
    tags: list[str] = []


@router.post("/v1/docs/{doc_id}/annotations", response_model=WriteResult)
async def post_annotation(
    doc_id: str, body: AnnotationIn, request: Request, idempotency_key: str | None = Header(default=None)
) -> WriteResult:
    ev = _event(
        doc_id=doc_id,
        kind=Kind.annotation,
        actor=body.author,
        source="api",
        source_ref=_api_ref(doc_id, idempotency_key),
        occurred_us=_now_us(),
        payload=body.model_dump(),
    )
    return (await submit(request, [ev]))[0]


class ArrivalMeta(BaseModel):
    channel: str
    content_sha256: str | None = None  # required when no file is sent
    size_bytes: int | None = None
    mime: str | None = None
    original_filename: str | None = None
    intake_meta: dict = {}
    arrived_us: int | None = None
    sim_run_id: str | None = None


@router.post("/v1/arrivals")
async def post_arrival(request: Request) -> dict:
    st = request.app.state
    if st.deps.arrivals is None:
        raise HTTPException(503, "arrivals store not configured")
    content: bytes | None = None
    if request.headers.get("content-type", "").startswith("multipart/"):
        form = await request.form()
        upload = form.get("file")
        if upload is None or isinstance(upload, str):
            raise HTTPException(422, "multipart arrival needs a file part")
        content = await upload.read()
        try:
            meta = ArrivalMeta.model_validate_json(str(form.get("meta") or "{}"))
        except ValidationError as e:
            raise HTTPException(422, e.errors(include_url=False, include_context=False)) from e
        meta.original_filename = meta.original_filename or upload.filename
        meta.mime = meta.mime or upload.content_type
    else:
        try:
            meta = ArrivalMeta.model_validate_json(await request.body())
        except ValidationError as e:
            raise HTTPException(422, e.errors(include_url=False, include_context=False)) from e
    if content is not None:
        sha = hashlib.sha256(content).digest()
        if meta.content_sha256 and bytes.fromhex(meta.content_sha256) != sha:
            raise HTTPException(422, "content_sha256 does not match the file")
        size = len(content)
        st.metrics.blob_bytes.inc(size)
    elif meta.content_sha256:
        sha, size = bytes.fromhex(meta.content_sha256), meta.size_bytes or 0
    else:
        raise HTTPException(422, "send a file or content_sha256")
    a = await st.deps.arrivals.record(
        content=content,
        content_sha256=sha,
        size_bytes=size,
        mime=meta.mime,
        channel=meta.channel,
        original_filename=meta.original_filename,
        intake_meta=meta.intake_meta,
        arrived_us=meta.arrived_us or _now_us(),
        sim_run_id=meta.sim_run_id,
    )
    if a.duplicate_of:
        st.metrics.duplicates.labels("content").inc()
    return {"arrival_id": a.arrival_id, "duplicate_of": a.duplicate_of}


@router.put("/v1/blobs/{sha256}")
async def put_blob(sha256: str, request: Request) -> dict:
    try:
        expected = bytes.fromhex(sha256)
    except ValueError as e:
        raise HTTPException(422, "sha256 must be hex") from e
    data = await request.body()
    try:
        info = request.app.state.deps.blobs.put(data, expected)
    except HashMismatch as e:
        raise HTTPException(422, str(e)) from e
    request.app.state.metrics.blob_bytes.inc(len(data))
    return {"sha256": info.sha256.hex(), "size_bytes": info.size_bytes, "storage_uri": info.storage_uri}


def blob_response(request: Request, sha256: str) -> Response:
    try:
        digest = bytes.fromhex(sha256)
    except ValueError as e:
        raise HTTPException(422, "sha256 must be hex") from e
    blobs = request.app.state.deps.blobs
    if not blobs.exists(digest):
        raise HTTPException(404, "no such blob")
    return Response(blobs.get(digest), media_type="application/octet-stream")
