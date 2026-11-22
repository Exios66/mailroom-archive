"""Durable producer checkpoints in the already-established cursors table."""

from datetime import UTC, datetime

from .session import run


class CursorStore:
    def __init__(self, session, keyspace: str):
        self.session = session
        self._get = session.prepare(f"SELECT position FROM {keyspace}.cursors WHERE name = ?")
        self._put = session.prepare(
            f"INSERT INTO {keyspace}.cursors (name, position, updated_at) VALUES (?, ?, ?)"
        )

    async def get(self, name: str) -> str | None:
        rows = await run(self.session, self._get, (name,))
        return rows[0]["position"] if rows else None

    async def put(self, name: str, position: str) -> None:
        await run(self.session, self._put, (name, position, datetime.now(UTC)))
