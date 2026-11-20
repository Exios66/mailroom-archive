"""Stable shard assignment (spec §7.2): writer shards and feed shards."""

import xxhash

WRITER_SHARDS = 64
FEED_SHARDS = 16


def shard_of(doc_id: str, n: int = WRITER_SHARDS) -> int:
    return xxhash.xxh64_intdigest(doc_id.encode("utf-8")) % n


def feed_shard(doc_id: str) -> int:
    return shard_of(doc_id, FEED_SHARDS)
