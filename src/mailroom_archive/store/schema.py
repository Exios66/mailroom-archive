"""Render and apply schema/*.cql. Placeholders: {ks}, {rf}, {timeline_compaction}."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from cassandra.cluster import Session

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schema"
TIMELINE_COMPACTION = "{'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L4'}"
_TABLE = re.compile(r"CREATE TABLE IF NOT EXISTS \S+?\.(\w+)")
_KS = re.compile(r"^[a-z][a-z0-9_]{0,47}$")

Profile = Literal["single", "small", "full"]


def render(keyspace: str, profile: Profile) -> list[str]:
    if not _KS.match(keyspace):
        raise ValueError(f"bad keyspace name: {keyspace!r}")
    values = {
        "{ks}": keyspace,
        "{rf}": "1" if profile == "single" else "3",
        "{timeline_compaction}": TIMELINE_COMPACTION,
    }
    out: list[str] = []
    for path in sorted(SCHEMA_DIR.glob("*.cql")):
        text = re.sub(r"--[^\n]*", "", path.read_text())
        for key, val in values.items():
            text = text.replace(key, val)
        out += [s.strip() for s in text.split(";") if s.strip()]
    return out


def apply_schema(session: Session, keyspace: str, profile: Profile) -> list[str]:
    tables = []
    for stmt in render(keyspace, profile):
        session.execute(stmt, timeout=60)
        if m := _TABLE.search(stmt):
            tables.append(m.group(1))
    return tables
