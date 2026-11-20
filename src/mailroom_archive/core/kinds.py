"""The closed set of event kinds (spec §6.2)."""

from enum import StrEnum


class Kind(StrEnum):
    arrived = "arrived"
    stage_changed = "stage_changed"
    node_visit = "node_visit"
    trace_io = "trace_io"
    audit_row = "audit_row"
    audit_attempt = "audit_attempt"
    audit_addendum = "audit_addendum"
    pending_row = "pending_row"
    correction = "correction"
    conflict = "conflict"
    annotation = "annotation"
    relation = "relation"
    retention = "retention"


# Kinds only the archive itself writes.
WRITER_ONLY = frozenset({Kind.conflict, Kind.retention})
CLIENT_KINDS = frozenset(Kind) - WRITER_ONLY
