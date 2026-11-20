import os

import pytest

if not os.environ.get("CASSANDRA_HOSTS") or os.environ.get("ARCHIVE_PROFILE") != "small":
    pytest.skip("fault tests need CASSANDRA_HOSTS and ARCHIVE_PROFILE=small", allow_module_level=True)

from cassandra_fixtures import ks, ks_session, store  # noqa: E402, F401
