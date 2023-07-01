import pytest
import logging

from uuid import uuid4 as new_uuid
from btrdb.utils.timez import currently_as_ns

try:
    import pyarrow as pa
except:
    pa = None

def test_grpc_insert_and_values(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name":"s"})
    t = currently_as_ns()
    data = []
    duration=100
    for i in range(duration):
        data.append([t+i, i])
    s.insert(data)
    fetched_data = s.values(start=t, end=t+duration)
    assert len(fetched_data) == len(data)
    for (i, (p, _)) in enumerate(fetched_data):
        assert p.time == data[i][0]
        assert p.value == data[i][1]

def test_arrow_insert_and_values(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name":"s"})
    t = currently_as_ns()
    times = []
    values = []
    duration = 100
    for i in range(duration):
        times.append(t+i)
        values.append(i)
    schema = pa.schema([
        pa.field('time', pa.timestamp('ns', tz='UTC'), nullable=False),
        pa.field('value', pa.float64(), nullable=False),
    ])
    data = pa.Table.from_arrays([
        pa.array(times),
        pa.array(values),
    ], schema=schema)
    s.arrow_insert(data)
    fetched_data = s.arrow_values(start=t, end=t+duration)
    assert data == fetched_data

def test_arrow_empty_values(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name":"s"})
    data = s.arrow_values(start=100, end=1000)
    assert len(data['time']) == 0
    assert len(data['value']) == 0

@pytest.mark.xfail
def test_stream_annotation_update(conn, tmp_collection):
    # XXX marked as expected failure until someone has time to investigate.
    s = conn.create(new_uuid(), tmp_collection, tags={"name":"s"}, annotations={"foo":"bar"})
    annotations1, version1 = s.annotations()
    assert version1 == 0
    assert annotations1["foo"] == "bar"
    s.update(annotations={"foo":"baz"})
    annotations2, version2 = s.annotations()
    assert version2 > version1
    assert annotations2["foo"] == "baz"
    s.update(annotations={}, replace=True)
    annotations3, _ = s.annotations()
    assert len(annotations3) == 0
