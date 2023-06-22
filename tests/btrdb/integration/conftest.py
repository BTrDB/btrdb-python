import os
import pytest
import btrdb

@pytest.fixture
def conn():
    if os.getenv("BTRDB_INTEGRATION_TEST_PROFILE") == None:
        pytest.skip("skipping because BTRDB_INTEGRATION_TEST_PROFILE is not set")
    conn = btrdb.connect(profile=os.getenv("BTRDB_INTEGRATION_TEST_PROFILE"))
    return conn

def _delete_collection(conn, col):
    streams = conn.streams_in_collection(col)
    for stream in streams:
        stream.obliterate()

@pytest.fixture
def tmp_collection(request, conn):
    col = "btrdb_python_integration_tests/" + request.node.name + "/tmp"
    _delete_collection(conn, col)
    yield col
    _delete_collection(conn, col)
