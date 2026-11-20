"""Read routes: current state, timeline, content, matters, verify, blob download (spec §7.1)."""

from __future__ import annotations

import json
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from ..core.kinds import Kind
from ..core.models import StoredEvent
from ..core.state import CORRECTABLE_TOP, Head
from ..verify import VerifyReport, verify_events
from .auth import require_read
from .routes_events import blob_response

log = structlog.get_logger()
router = APIRouter(dependencies=[Depends(require_read)])


def _hex(b: bytes | None) -> str | None:
    return b.hex() if b else None


def event_json(e: StoredEvent) -> dict[str, Any]:
    return {
        "seq": e.seq,
        "event_id": str(e.event_id),
        "kind": str(e.kind),
        "actor": e.actor,
        "source": e.source,
        "source_ref": e.source_ref,
        "occurred_us": e.occurred_us,
        "payload": json.loads(e.payload) if e.payload else None,
        "payload_ref": e.payload_ref,
        "payload_sha256": _hex(e.payload_sha256),
        "prev_hash": _hex(e.prev_hash),
        "event_hash": _hex(e.event_hash),
        "corrects_seq": e.corrects_seq,
    }


def _get_path(h: Head, path: str) -> Any:
    if path in CORRECTABLE_TOP:
        return getattr(h, path)
    d: Any = h.extracted_data
    for k in path.split(".")[1:]:
        d = d.get(k) if isinstance(d, dict) else None
    return d


async def doc_state(store: Any, h: Head, doc_id: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "doc_id": doc_id,
        "head_seq": h.seq,
        "head_hash": _hex(h.hash),
        "matter_id": h.matter_id,
        "doc_type": h.doc_type,
        "doc_subclass": h.doc_subclass,
        "stage": h.stage,
        "stage_us": h.stage_us,
        "content_sha256": _hex(h.content_sha256),
        "channel": h.channel,
        "original_filename": h.original_filename,
        "extracted_data": h.extracted_data,
        "audit_entry_hash": _hex(h.audit_entry_hash),
        "overrides": {},
    }
    if not h.overrides:
        return out
    # the latest conflict per overridden field carries the value the pipeline wants
    latest: dict[str, Any] = {}
    for e in await store.timeline(doc_id, after_seq=min(h.overrides.values())):
        if e.kind == Kind.conflict and e.payload:
            p = json.loads(e.payload)
            latest[p["field_path"]] = p["pipeline_value"]
    for path, seq in h.overrides.items():
        view = {"value": _get_path(h, path), "overridden_by": seq, "pipeline_value": latest.get(path)}
        out["overrides"][path] = view
        if path in CORRECTABLE_TOP:
            out[path] = view
    return out


async def _head_or_404(request: Request, doc_id: str) -> Head:
    h = await request.app.state.deps.store.get_head(doc_id)
    if h.seq == 0:
        raise HTTPException(404, "no such document")
    return h


@router.get("/v1/docs/{doc_id}")
async def get_doc(doc_id: str, request: Request) -> dict:
    return await doc_state(request.app.state.deps.store, await _head_or_404(request, doc_id), doc_id)


@router.get("/v1/docs/{doc_id}/timeline")
async def get_timeline(
    doc_id: str, request: Request, after_seq: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=1000)
) -> list[dict]:
    events = await request.app.state.deps.store.timeline(doc_id, after_seq=after_seq, limit=limit)
    return [event_json(e) for e in events]


@router.get("/v1/verify/doc/{doc_id}", response_model=VerifyReport)
async def verify_doc(doc_id: str, request: Request) -> VerifyReport:
    st = request.app.state
    report = verify_events(doc_id, await st.deps.store.timeline(doc_id), st.deps.blobs)
    if not report.ok:
        st.metrics.verify_failures.inc()
        log.warning(
            "archive_verify_failed",
            doc_id=doc_id,
            failed=[(c.name, c.seq) for c in report.checks if not c.ok],
        )
    return report


@router.get("/v1/content/{sha256}")
async def get_content(sha256: str, request: Request) -> dict:
    arrivals = request.app.state.deps.arrivals
    if arrivals is None:
        raise HTTPException(503, "arrivals store not configured")
    try:
        digest = bytes.fromhex(sha256)
    except ValueError as e:
        raise HTTPException(422, "sha256 must be hex") from e
    row = await arrivals.blob_row(digest)
    sightings = await arrivals.sightings(digest)
    if row is None and not sightings:
        raise HTTPException(404, "content never seen")
    out = []
    for s in sightings:
        a = await arrivals.get(s["arrival_id"])
        out.append(
            {
                "arrival_id": s["arrival_id"],
                "seen_at": str(s["seen_at"]),
                "channel": s["channel"],
                "original_filename": s["original_filename"],
                "doc_id": a["doc_id"] if a else None,
            }
        )
    blob = (
        None
        if row is None
        else {
            "size_bytes": row["size_bytes"],
            "mime": row["mime"],
            "storage_uri": row["storage_uri"],
            "first_seen": row["first_seen"].isoformat() if row["first_seen"] else None,
        }
    )
    return {"content_sha256": sha256, "blob": blob, "sightings": out}


@router.get("/v1/matters/{matter_id}/docs")
async def get_matter_docs(
    matter_id: str, request: Request, year: int = Query(...), limit: int = Query(500, ge=1, le=1000)
) -> list[dict]:
    store = request.app.state.deps.store
    out = []
    for r in await store.matter_docs(matter_id, year, limit):
        h = await store.get_head(r["doc_id"])
        out.append(
            {
                "doc_id": r["doc_id"],
                "created_at": str(r.get("created_at")),
                "doc_type": h.doc_type,
                "stage": h.stage,
                "head_seq": h.seq,
            }
        )
    return out


@router.get("/v1/blobs/{sha256}")
async def get_blob(sha256: str, request: Request) -> Response:
    return blob_response(request, sha256)
