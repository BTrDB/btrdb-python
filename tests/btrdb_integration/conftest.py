import os

import pyarrow as pa
import pytest

import btrdb


@pytest.fixture
def single_stream_values_arrow_schema():
    schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("value", pa.float64(), nullable=False),
        ]
    )
    return schema


@pytest.fixture
def single_stream_windows_all_stats_arrow_schema():
    schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("min", pa.float64(), nullable=False),
            pa.field("mean", pa.float64(), nullable=False),
            pa.field("max", pa.float64(), nullable=False),
            pa.field("count", pa.uint64(), nullable=False),
            pa.field("stddev", pa.float64(), nullable=False),
        ]
    )
    return schema


def single_stream_windows_mean_stddev_count_stats_arrow_schema():
    schema = (
        pa.schema(
            [
                pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
                pa.field("mean", pa.float64(), nullable=False),
                pa.field("count", pa.uint64(), nullable=False),
                pa.field("stddev", pa.float64(), nullable=False),
            ]
        ),
    )
    return schema


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
