"""Identifiers: timeuuids for events (ingest time), UUIDv7 strings for arrivals."""

import uuid

import uuid6

# 100-ns intervals between the UUID epoch (1582-10-15) and the Unix epoch.
_UUID_EPOCH_OFFSET = 0x01B21DD213814000


def new_event_id() -> uuid.UUID:
    return uuid.uuid1()


def event_id_us(event_id: uuid.UUID) -> int:
    return (event_id.time - _UUID_EPOCH_OFFSET) // 10


def new_arrival_id() -> str:
    return str(uuid6.uuid7())
