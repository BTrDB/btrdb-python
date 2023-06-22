import btrdb
import btrdb.stream
import pytest
import logging
from math import nan, isnan
from uuid import uuid4 as new_uuid
from btrdb.utils.timez import currently_as_ns

try:
    import pyarrow as pa
except:
    pa = None
try:
    import pandas as pd
except:
    pd = None

def test_streamset_values(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name":"s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name":"s2"})
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
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name":"s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name":"s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_col1 = [0.0, None, 1.0, None, 2.0, None, 3.0, None, 4.0]
    expected_col2 = [None, 5.0, None, 6.0, 7.0, 8.0, None, 9.0, None]
    values = ss.arrow_values()
    times = [t.value for t in values['time']]
    col1 = [None if isnan(v) else v for v in values[tmp_collection + '/s1']]
    col2 = [None if isnan(v) else v for v in values[tmp_collection + '/s2']]
    assert times == expected_times
    assert col1 == expected_col1
    assert col2 == expected_col2

def test_streamset_to_dataframe(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name":"s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name":"s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.to_dataframe()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_col1 = [0.0, None, 1.0, None, 2.0, None, 3.0, None, 4.0]
    expected_col2 = [None, 5.0, None, 6.0, 7.0, 8.0, None, 9.0, None]
    times = [t for t in values.index]
    col1 = [None if isnan(v) else v for v in values[tmp_collection + '/s1']]
    col2 = [None if isnan(v) else v for v in values[tmp_collection + '/s2']]
    assert times == expected_times
    assert col1 == expected_col1
    assert col2 == expected_col2
