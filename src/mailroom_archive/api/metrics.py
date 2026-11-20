"""Prometheus metrics (spec §11 names). One registry per app, so tests never collide."""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Histogram
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector


class _PoolCollector(Collector):
    """Values the pool and spool already keep, read at scrape time."""

    def __init__(self, deps: Any):
        self.deps = deps

    def collect(self) -> Iterable:
        pool = self.deps.pool
        depth = GaugeMetricFamily("archive_shard_queue_depth", "Events waiting per shard", labels=["shard"])
        for i, q in enumerate(getattr(pool, "queues", [])):
            depth.add_metric([str(i)], q.qsize())
        yield depth
        yield CounterMetricFamily(
            "archive_write_retries", "Cassandra write retries", value=getattr(pool, "retries", 0)
        )
        yield CounterMetricFamily(
            "archive_conflicts",
            "Pipeline values that disagreed with an override",
            value=getattr(pool, "conflicts", 0),
        )
        spool = getattr(pool, "spool", None)
        stats = spool.stats() if spool is not None else None
        yield GaugeMetricFamily(
            "archive_spool_events", "Events waiting in the spool", value=stats.events if stats else 0
        )
        oldest = (time.time() - stats.oldest_us / 1e6) if stats and stats.oldest_us else 0
        yield GaugeMetricFamily(
            "archive_spool_oldest_seconds", "Age of the oldest spooled event", value=oldest
        )
        session = getattr(self.deps, "session", None)
        up = sum(1 for h in session.cluster.metadata.all_hosts() if h.is_up) if session is not None else 0
        yield GaugeMetricFamily("archive_cassandra_hosts_up", "Cassandra hosts the driver sees up", value=up)


class Metrics:
    def __init__(self, deps: Any):
        self.registry = CollectorRegistry()
        r = self.registry
        self.ingested = Counter(
            "archive_events_ingested", "Events accepted", ["kind", "source", "status"], registry=r
        )
        self.latency = Histogram("archive_ingest_latency_seconds", "POST /v1/events latency", registry=r)
        self.blob_bytes = Counter("archive_blob_bytes", "Bytes written to the blob store", registry=r)
        self.duplicates = Counter("archive_duplicates", "Duplicates seen", ["type"], registry=r)
        self.verify_failures = Counter("archive_verify_failures", "Documents that failed verify", registry=r)
        r.register(_PoolCollector(deps))
