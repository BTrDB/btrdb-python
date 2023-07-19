import logging
from math import isnan, nan
from uuid import uuid4 as new_uuid

import numpy as np
import pytest
from numpy import testing as np_test

import btrdb
import btrdb.stream
from btrdb.utils.timez import currently_as_ns

try:
    import pyarrow as pa
except ImportError:
    pa = None
try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import polars as pl
    from polars import testing as pl_test
except ImportError:
    pl = None


def test_streamset_values(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.values()
    assert len(values) == 2
    assert len(values[0]) == len(t1)
    assert len(values[1]) == len(t2)
    assert [(p.time, p.value) for p in values[0]] == list(zip(t1, d1))
    assert [(p.time, p.value) for p in values[1]] == list(zip(t2, d2))


def test_streamset_arrow_values(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_col1 = [0.0, np.NaN, 1.0, np.NaN, 2.0, np.NaN, 3.0, np.NaN, 4.0]
    expected_col2 = [np.NaN, 5.0, np.NaN, 6.0, 7.0, 8.0, np.NaN, 9.0, np.NaN]
    expected_schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(str(s1.uuid), pa.float64(), nullable=False),
            pa.field(str(s2.uuid), pa.float64(), nullable=False),
        ]
    )
    values = ss.arrow_values()
    times = [t.value for t in values["time"]]
    col1 = [np.NaN if isnan(v.as_py()) else v.as_py() for v in values[str(s1.uuid)]]
    col2 = [np.NaN if isnan(v.as_py()) else v.as_py() for v in values[str(s2.uuid)]]
    assert times == expected_times
    assert col1 == expected_col1
    assert col2 == expected_col2
    assert expected_schema.equals(values.schema)


@pytest.mark.parametrize(
    "name_callable",
    [(None), (lambda s: str(s.uuid)), (lambda s: s.name + "/" + s.collection)],
    ids=["empty", "uu_as_str", "name_collection"],
)
def test_streamset_arrow_windows_vs_windows(conn, tmp_collection, name_callable):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    s3 = conn.create(new_uuid(), tmp_collection, tags={"name": "s3"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    d3 = [1.0, 9.0, 44.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    s3.insert(list(zip(t2, d3)))
    ss = (
        btrdb.stream.StreamSet([s1, s2, s3])
        .filter(start=100, end=121)
        .windows(width=btrdb.utils.timez.ns_delta(nanoseconds=10))
    )
    values_arrow = ss.arrow_to_dataframe(name_callable=name_callable)
    values_prev = ss.to_dataframe(name_callable=name_callable).convert_dtypes(
        dtype_backend="pyarrow"
    )
    values_prev.index = pd.DatetimeIndex(values_prev.index, tz="UTC")
    col_map = {old_col: old_col + "/mean" for old_col in values_prev.columns}
    values_prev = values_prev.rename(columns=col_map)
    (values_arrow)
    (values_prev)
    assert values_arrow.equals(values_prev)


def test_streamset_arrow_windows_vs_windows_agg_all(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    s3 = conn.create(new_uuid(), tmp_collection, tags={"name": "s3"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    d3 = [1.0, 9.0, 44.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    s3.insert(list(zip(t2, d3)))
    ss = (
        btrdb.stream.StreamSet([s1, s2, s3])
        .filter(start=100, end=121)
        .windows(width=btrdb.utils.timez.ns_delta(nanoseconds=10))
    )
    values_arrow = ss.arrow_to_dataframe(name_callable=None, agg=["all"])
    values_prev = ss.to_dataframe(name_callable=None, agg="all")
    values_prev = values_prev.apply(lambda x: x.astype(str(x.dtype) + "[pyarrow]"))
    values_prev = values_prev.apply(
        lambda x: x.astype("uint64[pyarrow]") if "count" in x.name else x
    )
    values_prev.index = pd.DatetimeIndex(values_prev.index, tz="UTC")
    values_prev = values_prev.convert_dtypes(dtype_backend="pyarrow")
    new_cols = ["/".join(old_col) for old_col in values_prev.columns]
    values_prev.columns = new_cols
    assert values_arrow.equals(values_prev)

    with pytest.raises(
        AttributeError,
        match="cannot provide name_callable when using 'all' as aggregate at this time",
    ):
        values_prev = ss.to_dataframe(name_callable=lambda x: str(x.uuid), agg="all")
    other_arrow_df = ss.arrow_to_dataframe(
        agg=["all", "mean", "trash"], name_callable=lambda x: str(x.uuid)
    )
    assert (
        len(other_arrow_df.filter(regex="[min,mean,max,count,stddev]").columns) == 3 * 5
    )


@pytest.mark.parametrize(
    "name_callable",
    [(None), (lambda s: str(s.uuid)), (lambda s: s.name + "/" + s.collection)],
    ids=["empty", "uu_as_str", "name_collection"],
)
def test_streamset_arrow_aligned_windows_vs_aligned_windows(
    conn, tmp_collection, name_callable
):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    s3 = conn.create(new_uuid(), tmp_collection, tags={"name": "s3"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    d3 = [1.0, 9.0, 44.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    s3.insert(list(zip(t2, d3)))
    ss = (
        btrdb.stream.StreamSet([s1, s2, s3])
        .filter(start=100, end=121)
        .windows(width=btrdb.utils.general.pointwidth.from_nanoseconds(10))
    )
    values_arrow = ss.arrow_to_dataframe(name_callable=name_callable)
    values_prev = ss.to_dataframe(
        name_callable=name_callable
    )  # .convert_dtypes(dtype_backend='pyarrow')
    values_prev = values_prev.apply(lambda x: x.astype(str(x.dtype) + "[pyarrow]"))
    values_prev = values_prev.apply(
        lambda x: x.astype("uint64[pyarrow]") if "count" in x.name else x
    )
    values_prev.index = pd.DatetimeIndex(values_prev.index, tz="UTC")
    col_map = {old_col: old_col + "/mean" for old_col in values_prev.columns}
    values_prev = values_prev.rename(columns=col_map)
    assert values_arrow.equals(values_prev)


def test_streamset_empty_arrow_values(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    ss = btrdb.stream.StreamSet([s]).filter(start=100, end=200)
    values = ss.arrow_values()
    expected_schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(str(s.uuid), pa.float64(), nullable=False),
        ]
    )
    assert [t.value for t in values["time"]] == []
    assert [v for v in values[str(s.uuid)]] == []
    assert expected_schema.equals(values.schema)


def test_streamset_to_dataframe(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.to_dataframe()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_col1 = [0.0, np.NaN, 1.0, np.NaN, 2.0, np.NaN, 3.0, np.NaN, 4.0]
    expected_col2 = [np.NaN, 5.0, np.NaN, 6.0, 7.0, 8.0, np.NaN, 9.0, np.NaN]
    expected_dat = {
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    expected_df = pd.DataFrame(expected_dat, index=expected_times)
    assert values.equals(expected_df)


def test_arrow_streamset_to_dataframe(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.arrow_to_dataframe()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_times = [
        pa.scalar(v, type=pa.timestamp("ns", tz="UTC")).as_py() for v in expected_times
    ]
    expected_col1 = pa.array(
        [0.0, np.NaN, 1.0, np.NaN, 2.0, np.NaN, 3.0, np.NaN, 4.0], mask=[False] * 9
    )
    expected_col2 = pa.array(
        [np.NaN, 5.0, np.NaN, 6.0, 7.0, 8.0, np.NaN, 9.0, np.NaN], mask=[False] * 9
    )
    expected_dat = {
        "time": expected_times,
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    schema = pa.schema(
        fields=[
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(tmp_collection + "/s1", type=pa.float64(), nullable=False),
            pa.field(tmp_collection + "/s2", type=pa.float64(), nullable=False),
        ]
    )
    expected_table = pa.Table.from_pydict(expected_dat, schema=schema)
    expected_df = expected_table.to_pandas(
        timestamp_as_object=False, types_mapper=pd.ArrowDtype
    )
    expected_df.set_index("time", inplace=True)
    expected_df.index = pd.DatetimeIndex(expected_df.index, tz="UTC")
    np_test.assert_array_equal(
        values.values.astype(float), expected_df.values.astype(float)
    )


def test_arrow_streamset_to_polars(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.arrow_to_polars()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_times = [
        pa.scalar(v, type=pa.timestamp("ns", tz="UTC")).as_py() for v in expected_times
    ]
    expected_col1 = [0.0, np.NaN, 1.0, np.NaN, 2.0, np.NaN, 3.0, np.NaN, 4.0]
    expected_col2 = [np.NaN, 5.0, np.NaN, 6.0, 7.0, 8.0, np.NaN, 9.0, np.NaN]
    expected_dat = {
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    expected_df = pd.DataFrame(
        expected_dat, index=pd.DatetimeIndex(expected_times)
    ).reset_index(names="time")
    expected_df_pl = pl.from_pandas(expected_df, nan_to_null=False)
    pl_test.assert_frame_equal(values, expected_df_pl)


@pytest.mark.parametrize(
    "name_callable",
    [(None), (lambda s: str(s.uuid)), (lambda s: s.name + "/" + s.collection)],
    ids=["empty", "uu_as_str", "name_collection"],
)
def test_streamset_arrow_polars_vs_old_to_polars(conn, tmp_collection, name_callable):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values_arrow = ss.arrow_to_polars()
    values_non_arrow = ss.to_polars()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_times = [
        pa.scalar(v, type=pa.timestamp("ns", tz="UTC")).as_py() for v in expected_times
    ]
    expected_col1 = [0.0, np.NaN, 1.0, np.NaN, 2.0, np.NaN, 3.0, np.NaN, 4.0]
    expected_col2 = [np.NaN, 5.0, np.NaN, 6.0, 7.0, 8.0, np.NaN, 9.0, np.NaN]
    expected_dat = {
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    expected_df = pd.DataFrame(
        expected_dat, index=pd.DatetimeIndex(expected_times, tz="UTC")
    ).reset_index(names="time")
    expected_df_pl = pl.from_pandas(expected_df, nan_to_null=False)
    pl_test.assert_frame_equal(values_arrow, expected_df_pl)
    pl_test.assert_frame_equal(values_non_arrow, expected_df_pl)
    pl_test.assert_frame_equal(values_non_arrow, values_arrow)


@pytest.mark.parametrize(
    "name_callable",
    [(None), (lambda s: str(s.uuid)), (lambda s: s.name + "/" + s.collection)],
    ids=["empty", "uu_as_str", "name_collection"],
)
def test_streamset_windows_arrow_polars_vs_old_to_polars(
    conn, tmp_collection, name_callable
):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    s3 = conn.create(new_uuid(), tmp_collection, tags={"name": "s3"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    d3 = [1.0, 9.0, 44.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    s3.insert(list(zip(t2, d3)))
    ss = (
        btrdb.stream.StreamSet([s1, s2, s3])
        .filter(start=100, end=121)
        .windows(width=btrdb.utils.timez.ns_delta(nanoseconds=10))
    )
    values_arrow_pl = ss.arrow_to_polars(name_callable=name_callable)
    values_non_arrow_pl = ss.to_polars(name_callable=name_callable)
    new_names = {
        old_col: str(old_col) + "/" + "mean"
        for old_col in values_non_arrow_pl.select(pl.col(pl.Float64)).columns
    }
    new_names["time"] = "time"
    values_non_arrow_pl = values_non_arrow_pl.rename(mapping=new_names)
    assert values_arrow_pl.frame_equal(values_non_arrow_pl)


def test_streamset_windows_aggregates_filter(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    s3 = conn.create(new_uuid(), tmp_collection, tags={"name": "s3"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    d3 = [1.0, 9.0, 44.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    s3.insert(list(zip(t2, d3)))
    ss = (
        btrdb.stream.StreamSet([s1, s2, s3])
        .filter(start=100, end=121)
        .windows(width=btrdb.utils.timez.ns_delta(nanoseconds=10))
    )
    values_arrow_df = ss.arrow_to_dataframe(agg=["mean", "stddev"])
    values_non_arrow_df = ss.to_dataframe(agg="all")
    values_non_arrow_df.index = pd.DatetimeIndex(values_non_arrow_df.index, tz="UTC")
    values_non_arrow_df = values_non_arrow_df.apply(
        lambda x: x.astype(str(x.dtype) + "[pyarrow]")
    )
    values_non_arrow_df = values_non_arrow_df.apply(
        lambda x: x.astype("uint64[pyarrow]") if "count" in x.name else x
    )
    new_cols = ["/".join(old_col) for old_col in values_non_arrow_df.columns]
    values_non_arrow_df.columns = new_cols
    cols_to_drop = [
        col
        for col in values_non_arrow_df.columns
        if ("count" in col) or ("min" in col) or ("max" in col)
    ]
    values_non_arrow_df.drop(columns=cols_to_drop, inplace=True)
    assert values_arrow_df.equals(values_non_arrow_df)


def test_timesnap_backward_extends_range(conn, tmp_collection):
    sec = 10**9
    tv1 = [
        [int(0.5 * sec), 0.5],
        [2 * sec, 2.0],
    ]
    tv2 = [
        [int(0.5 * sec) - 1, 0.5],
        [2 * sec, 2.0],
    ]
    tv3 = [
        [1 * sec, 1.0],
        [2 * sec, 2.0],
    ]
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    s3 = conn.create(new_uuid(), tmp_collection, tags={"name": "s3"})
    s1.insert(tv1)
    s2.insert(tv2)
    s3.insert(tv3)
    ss = btrdb.stream.StreamSet([s1, s2, s3]).filter(
        start=1 * sec, end=3 * sec, sampling_frequency=1
    )
    values = ss.arrow_values()
    assert [1 * sec, 2 * sec] == [t.value for t in values["time"]]
    assert [0.5, 2.0] == [v.as_py() for v in values[str(s1.uuid)]]
    assert [np.NaN, 2.0] == [
        np.NaN if isnan(v.as_py()) else v.as_py() for v in values[str(s2.uuid)]
    ]
    assert [1.0, 2.0] == [v.as_py() for v in values[str(s3.uuid)]]


def test_timesnap_forward_restricts_range(conn, tmp_collection):
    sec = 10**9
    tv = [
        [1 * sec, 1.0],
        [2 * sec, 2.0],
        [int(2.75 * sec), 2.75],
    ]
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    s.insert(tv)
    ss = btrdb.stream.StreamSet([s]).filter(start=1 * sec, sampling_frequency=1)
    values = ss.filter(end=int(3.0 * sec)).arrow_values()
    assert [1 * sec, 2 * sec] == [t.value for t in values["time"]]
    assert [1.0, 2.0] == [v.as_py() for v in values[str(s.uuid)]]
    # Same result if skipping past end instead of to end.
    assert values == ss.filter(end=int(2.9 * sec)).arrow_values()
