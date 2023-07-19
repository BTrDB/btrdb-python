import logging
from uuid import uuid4 as new_uuid

import numpy as np
import pandas as pd
import pytest

import btrdb.utils.timez
from btrdb.utils.timez import currently_as_ns, ns_delta

try:
    import pyarrow as pa
except ImportError:
    pa = None


def test_stream_methods(conn, tmp_collection):
    s1 = conn.create(
        new_uuid(),
        tmp_collection,
        tags={"name": "s1", "unit": "foo"},
        annotations={"test": "bar"},
    )


def test_grpc_insert_and_values(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    data = []
    duration = 100
    for i in range(duration):
        data.append([t + i, i])
    s.insert(data)
    fetched_data = s.values(start=t, end=t + duration)
    assert len(fetched_data) == len(data)
    for i, (p, _) in enumerate(fetched_data):
        assert p.time == data[i][0]
        assert p.value == data[i][1]


def test_arrow_insert_and_values(
    conn, tmp_collection, single_stream_values_arrow_schema
):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = 100
    for i in range(duration):
        times.append(t + i)
        values.append(i)
    data = pa.Table.from_arrays(
        [
            pa.array(times),
            pa.array(values),
        ],
        schema=single_stream_values_arrow_schema,
    )
    s.arrow_insert(data)
    fetched_data = s.arrow_values(start=t, end=t + duration)
    assert data == fetched_data


@pytest.mark.parametrize(
    "merge_policy,duplicates_expected",
    [("never", True), ("equal", True), ("retain", False), ("replace", False)],
)
def test_arrow_insert_duplicate_timestamps(
    conn,
    tmp_collection,
    single_stream_values_arrow_schema,
    merge_policy,
    duplicates_expected,
):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    t1 = [100, 105, 110, 110, 120]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    s1.insert(list(zip(t1, d1)), merge=merge_policy)
    df = s1.arrow_values(start=100, end=121).to_pandas().set_index("time")
    assert any(df.index.duplicated().tolist()) == duplicates_expected


def test_arrow_values_table_schema(
    conn, tmp_collection, single_stream_values_arrow_schema
):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = 100
    for i in range(duration):
        times.append(t + i)
        values.append(i)
    data = pa.Table.from_arrays(
        [
            pa.array(times),
            pa.array(values),
        ],
        schema=single_stream_values_arrow_schema,
    )
    s.arrow_insert(data)
    fetched_data = s.arrow_values(start=t, end=t + duration)
    assert single_stream_values_arrow_schema.equals(fetched_data.schema)


def test_arrow_values_table_schema(
    conn, tmp_collection, single_stream_values_arrow_schema
):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = 100
    for i in range(duration):
        times.append(t + i)
        values.append(i)
    data = pa.Table.from_arrays(
        [
            pa.array(times),
            pa.array(values),
        ],
        schema=single_stream_values_arrow_schema,
    )
    s.arrow_insert(data)
    fetched_data = s.arrow_values(start=t, end=t + duration)
    assert single_stream_values_arrow_schema.equals(fetched_data.schema)


def test_arrow_windows(
    conn,
    tmp_collection,
    single_stream_windows_all_stats_arrow_schema,
    single_stream_values_arrow_schema,
):
    rng = np.random.default_rng(seed=7)
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = btrdb.utils.timez.ns_delta(minutes=1)
    nvals = 100
    for i in range(nvals):
        times.append(t + i * duration)
        values.append(i + rng.random())
    data = pa.Table.from_arrays(
        [
            pa.array(times),
            pa.array(values),
        ],
        schema=single_stream_values_arrow_schema,
    )
    # group these 5 at a time to go with the 5 min windows from below
    mean_dat = np.mean(np.asarray(values).reshape(-1, 5), axis=1)
    min_dat = np.min(np.asarray(values).reshape(-1, 5), axis=1)
    max_dat = np.max(np.asarray(values).reshape(-1, 5), axis=1)
    count_dat = [np.asarray(values).reshape(-1, 5).shape[1]] * int(
        np.asarray(values).shape[0] / 5
    )
    stddev_dat = np.std(np.asarray(values).reshape(-1, 5), axis=1)
    time_dat = [t for i, t in enumerate(times) if i % 5 == 0]
    window_table = pa.Table.from_arrays(
        [time_dat, min_dat, mean_dat, max_dat, count_dat, stddev_dat],
        schema=single_stream_windows_all_stats_arrow_schema,
    )
    s.arrow_insert(data)
    # 5 min windows
    fetched_data = s.arrow_windows(
        start=t, end=t + duration * nvals + 1, width=duration * 5
    )
    fetched_df = fetched_data.to_pandas()
    fetched_df["time"] = fetched_df["time"].astype(int)
    window_df = window_table.to_pandas()
    window_df["time"] = window_df["time"].astype(int)
    assert np.allclose(fetched_df.values, window_df.values, rtol=0, atol=1e-9)


@pytest.mark.xfail
def test_aligned_windows_no_flush(
    conn,
    tmp_collection,
    single_stream_windows_all_stats_arrow_schema,
    single_stream_values_arrow_schema,
):
    rng = np.random.default_rng(seed=7)
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = btrdb.utils.timez.ns_delta(minutes=1)
    nvals = 100
    for i in range(nvals):
        times.append(t + i * duration)
        values.append(i + rng.random())
    data = pa.Table.from_arrays(
        [
            pa.array(times),
            pa.array(values),
        ],
        schema=single_stream_values_arrow_schema,
    )
    s.arrow_insert(data)
    fetched_data = s.arrow_aligned_windows(
        start=t,
        end=t + duration * nvals,
        pointwidth=btrdb.utils.general.pointwidth.from_nanoseconds(duration * 6),
    )
    fetched_df = fetched_data.to_pandas()
    assert fetched_df["stddev"].isna().sum() == 0


def test_arrow_aligned_windows_vs_aligned_windows(
    conn,
    tmp_collection,
    single_stream_windows_all_stats_arrow_schema,
    single_stream_values_arrow_schema,
):
    rng = np.random.default_rng(seed=7)
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = btrdb.utils.timez.ns_delta(minutes=1)
    nvals = 100
    for i in range(nvals):
        times.append(t + i * duration)
        values.append(i + rng.random())
    data = pa.Table.from_arrays(
        [
            pa.array(times),
            pa.array(values),
        ],
        schema=single_stream_values_arrow_schema,
    )
    s.arrow_insert(data)
    s.flush()
    fetched_data = s.arrow_aligned_windows(
        start=t,
        end=t + duration * nvals,
        pointwidth=btrdb.utils.general.pointwidth.from_nanoseconds(duration * 6),
    )
    fetched_df = fetched_data.to_pandas()
    fetched_df["time"] = fetched_df["time"].astype(int)
    fetched_df["count"] = fetched_df["count"].astype(int)

    other_method = s.aligned_windows(
        start=t,
        end=t + duration * nvals,
        pointwidth=btrdb.utils.general.pointwidth.from_nanoseconds(duration * 6),
    )
    other_method_data = []
    for statpt, ver in other_method:
        tmp = dict()
        tmp["time"] = statpt.time
        tmp["min"] = statpt.min
        tmp["mean"] = statpt.mean
        tmp["max"] = statpt.max
        tmp["count"] = statpt.count
        tmp["stddev"] = statpt.stddev
        other_method_data.append(tmp)
    other_method_df = pd.DataFrame(other_method_data)
    assert fetched_data.schema.equals(single_stream_windows_all_stats_arrow_schema)
    assert fetched_df.equals(other_method_df)


def compare_arrow_windows_vs_windows(
    conn,
    tmp_collection,
    single_stream_windows_all_stats_arrow_schema,
    single_stream_values_arrow_schema,
):
    rng = np.random.default_rng(seed=7)
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = btrdb.utils.timez.ns_delta(minutes=1)
    nvals = 100
    for i in range(nvals):
        times.append(t + i * duration)
        values.append(i + rng.random())
    data = pa.Table.from_arrays(
        [
            pa.array(times),
            pa.array(values),
        ],
        schema=single_stream_values_arrow_schema,
    )
    s.arrow_insert(data)
    fetched_data = s.arrow_windows(
        start=t,
        end=t + duration * nvals,
        width=duration * 5,
    )
    fetched_df = fetched_data.to_pandas()
    fetched_df["time"] = fetched_df["time"].astype(int)
    fetched_df["count"] = fetched_df["count"].astype(int)

    other_method = s.windows(start=t, end=t + duration * nvals, width=duration * 5)
    other_method_data = []
    for statpt, ver in other_method:
        tmp = dict()
        tmp["time"] = statpt.time
        tmp["min"] = statpt.min
        tmp["mean"] = statpt.mean
        tmp["max"] = statpt.max
        tmp["count"] = statpt.count
        tmp["stddev"] = statpt.stddev
        other_method_data.append(tmp)
    other_method_df = pd.DataFrame(other_method_data)
    assert fetched_data.schema.equals(single_stream_windows_all_stats_arrow_schema)
    assert fetched_df.equals(other_method_df)


def test_arrow_empty_values(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    data = s.arrow_values(start=100, end=1000)
    assert len(data["time"]) == 0
    assert len(data["value"]) == 0


def test_arrow_empty_values_schema(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    data = s.arrow_values(start=100, end=1000)
    schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("value", pa.float64(), nullable=False),
        ]
    )
    assert schema.equals(data.schema)


@pytest.mark.xfail
def test_stream_annotation_update(conn, tmp_collection):
    # XXX marked as expected failure until someone has time to investigate.
    s = conn.create(
        new_uuid(), tmp_collection, tags={"name": "s"}, annotations={"foo": "bar"}
    )
    annotations1, version1 = s.annotations()
    assert version1 == 0
    assert annotations1["foo"] == "bar"
    s.update(annotations={"foo": "baz"})
    annotations2, version2 = s.annotations()
    assert version2 > version1
    assert annotations2["foo"] == "baz"
    s.update(annotations={}, replace=True)
    annotations3, _ = s.annotations()
    assert len(annotations3) == 0
