import pytest

from mailroom_archive.store.cursors import CursorStore

pytestmark = pytest.mark.integration


async def test_checkpoint_survives_store_recreation(ks_session, ks):
    cursors = CursorStore(ks_session, ks)
    name = "follower:integration:audit_addenda"
    assert await cursors.get(name) is None
    await cursors.put(name, '["2026-11-20 12:00:00.123456", "addendum-1"]')
    recreated = CursorStore(ks_session, ks)
    assert await recreated.get(name) == '["2026-11-20 12:00:00.123456", "addendum-1"]'
    await recreated.put(name, '["2026-11-20 12:00:01.123456", "addendum-2"]')
    assert await cursors.get(name) == '["2026-11-20 12:00:01.123456", "addendum-2"]'
    assert await cursors.get("follower:other-pipeline:audit_addenda") is None
