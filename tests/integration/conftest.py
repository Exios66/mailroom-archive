import os

import pytest

if not os.environ.get("CASSANDRA_HOSTS"):
    pytest.skip("CASSANDRA_HOSTS not set", allow_module_level=True)

from cassandra_fixtures import ks, ks_session, store  # noqa: E402, F401
