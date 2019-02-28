# tests.test_transformers
# Testing package for the btrdb transformers module
#
# Author:   PingThings
# Created:  Wed Jan 02 19:26:20 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_transformers.py [] allen@pingthings.io $

"""
Testing package for the btrdb stream module
"""

##########################################################################
## Imports
##########################################################################

import os
from io import StringIO, BytesIO

import pytest
from unittest.mock import Mock, PropertyMock
import numpy as np
from pandas import Series, DataFrame, Index

from btrdb.stream import Stream, StreamSet
from btrdb.point import RawPoint
from btrdb.transformers import *

##########################################################################
## Transformer Tests
##########################################################################

@pytest.fixture
def streamset():
    # list of mock streams
    streams = []
    for idx in range(4):
        stream = Mock(Stream)
        type(stream).collection = PropertyMock(return_value="test")
        type(stream).name = PropertyMock(return_value="stream{}".format(idx))
        streams.append(stream)

    rows = [
        (None, RawPoint(1500000000000000000, 1.0), RawPoint(1500000000000000000, 1.0), RawPoint(1500000000000000000, 1.0)),
        (RawPoint(1500000000100000000, 2.0), None, RawPoint(1500000000100000000, 2.0), RawPoint(1500000000100000000, 2.0)),
        (None, RawPoint(1500000000200000000, 3.0), None, RawPoint(1500000000200000000, 3.0)),
        (RawPoint(1500000000300000000, 4.0), None, RawPoint(1500000000300000000, 4.0), RawPoint(1500000000300000000, 4.0)),
        (None, RawPoint(1500000000400000000, 5.0), RawPoint(1500000000400000000, 5.0), RawPoint(1500000000400000000, 5.0)),
        (RawPoint(1500000000500000000, 6.0), None, None, RawPoint(1500000000500000000, 6.0)),
        (None, RawPoint(1500000000600000000, 7.0), RawPoint(1500000000600000000, 7.0), RawPoint(1500000000600000000, 7.0)),
        (RawPoint(1500000000700000000, 8.0), None, RawPoint(1500000000700000000, 8.0), RawPoint(1500000000700000000, 8.0)),
        (None, RawPoint(1500000000800000000, 9.0), RawPoint(1500000000800000000, 9.0), RawPoint(1500000000800000000, 9.0)),
        (RawPoint(1500000000900000000, 10.0), None, RawPoint(1500000000900000000, 10.0), RawPoint(1500000000900000000, 10.0) ),
    ]

    values = [
        [RawPoint(1500000000100000000, 2.0),RawPoint(1500000000300000000, 4.0),RawPoint(1500000000500000000, 6.0),RawPoint(1500000000700000000, 8.0),RawPoint(1500000000900000000, 10.0)],
        [RawPoint(1500000000000000000, 1.0),RawPoint(1500000000200000000, 3.0),RawPoint(1500000000400000000, 5.0),RawPoint(1500000000600000000, 7.0),RawPoint(1500000000800000000, 9.0)],
        [RawPoint(1500000000000000000, 1.0),RawPoint(1500000000100000000, 2.0),RawPoint(1500000000300000000, 4.0),RawPoint(1500000000400000000, 5.0),RawPoint(1500000000600000000, 7.0),RawPoint(1500000000700000000, 8.0),RawPoint(1500000000800000000, 9.0),RawPoint(1500000000900000000, 10.0)],
        [RawPoint(1500000000000000000, 1.0),RawPoint(1500000000100000000, 2.0),RawPoint(1500000000200000000, 3.0),RawPoint(1500000000300000000, 4.0),RawPoint(1500000000400000000, 5.0),RawPoint(1500000000500000000, 6.0),RawPoint(1500000000600000000, 7.0),RawPoint(1500000000700000000, 8.0),RawPoint(1500000000800000000, 9.0),RawPoint(1500000000900000000, 10.0)]
    ]

    obj = StreamSet(streams)
    obj.rows = Mock(return_value=rows)
    obj.values = Mock(return_value=values)
    return obj


expected = {
    "to_dict": [
        {'time': 1500000000000000000, 'test/stream0': None, 'test/stream1': 1.0, 'test/stream2': 1.0, 'test/stream3': 1.0},
        {'time': 1500000000100000000, 'test/stream0': 2.0, 'test/stream1': None, 'test/stream2': 2.0, 'test/stream3': 2.0},
        {'time': 1500000000200000000, 'test/stream0': None, 'test/stream1': 3.0, 'test/stream2': None, 'test/stream3': 3.0},
        {'time': 1500000000300000000, 'test/stream0': 4.0, 'test/stream1': None, 'test/stream2': 4.0, 'test/stream3': 4.0},
        {'time': 1500000000400000000, 'test/stream0': None, 'test/stream1': 5.0, 'test/stream2': 5.0, 'test/stream3': 5.0},
        {'time': 1500000000500000000, 'test/stream0': 6.0, 'test/stream1': None, 'test/stream2': None, 'test/stream3': 6.0},
        {'time': 1500000000600000000, 'test/stream0': None, 'test/stream1': 7.0, 'test/stream2': 7.0, 'test/stream3': 7.0},
        {'time': 1500000000700000000, 'test/stream0': 8.0, 'test/stream1': None, 'test/stream2': 8.0, 'test/stream3': 8.0},
        {'time': 1500000000800000000, 'test/stream0': None, 'test/stream1': 9.0, 'test/stream2': 9.0, 'test/stream3': 9.0},
        {'time': 1500000000900000000, 'test/stream0': 10.0, 'test/stream1': None, 'test/stream2': 10.0, 'test/stream3': 10.0}
    ],
    "to_array": [
        np.array([RawPoint(1500000000100000000, 2.0),
            RawPoint(1500000000300000000, 4.0),
            RawPoint(1500000000500000000, 6.0),
            RawPoint(1500000000700000000, 8.0),
            RawPoint(1500000000900000000, 10.0)]),
        np.array([RawPoint(1500000000000000000, 1.0),
            RawPoint(1500000000200000000, 3.0),
            RawPoint(1500000000400000000, 5.0),
            RawPoint(1500000000600000000, 7.0),
            RawPoint(1500000000800000000, 9.0)]),
        np.array([RawPoint(1500000000000000000, 1.0),
            RawPoint(1500000000100000000, 2.0),
            RawPoint(1500000000300000000, 4.0),
            RawPoint(1500000000400000000, 5.0),
            RawPoint(1500000000600000000, 7.0),
            RawPoint(1500000000700000000, 8.0),
            RawPoint(1500000000800000000, 9.0),
            RawPoint(1500000000900000000, 10.0)]),
        np.array([RawPoint(1500000000000000000, 1.0),
            RawPoint(1500000000100000000, 2.0),
            RawPoint(1500000000200000000, 3.0),
            RawPoint(1500000000300000000, 4.0),
            RawPoint(1500000000400000000, 5.0),
            RawPoint(1500000000500000000, 6.0),
            RawPoint(1500000000600000000, 7.0),
            RawPoint(1500000000700000000, 8.0),
            RawPoint(1500000000800000000, 9.0),
            RawPoint(1500000000900000000, 10.0)])
    ],
    "table": """               time    test/stream0    test/stream1    test/stream2    test/stream3
-------------------  --------------  --------------  --------------  --------------
1500000000000000000                               1               1               1
1500000000100000000               2                               2               2
1500000000200000000                               3                               3
1500000000300000000               4                               4               4
1500000000400000000                               5               5               5
1500000000500000000               6                                               6
1500000000600000000                               7               7               7
1500000000700000000               8                               8               8
1500000000800000000                               9               9               9
1500000000900000000              10                              10              10""",
    "series": None,
    "dataframe": None,
    "csv": """time,test/stream0,test/stream1,test/stream2,test/stream3
1500000000000000000,,1.0,1.0,1.0
1500000000100000000,2.0,,2.0,2.0
1500000000200000000,,3.0,,3.0
1500000000300000000,4.0,,4.0,4.0
1500000000400000000,,5.0,5.0,5.0
1500000000500000000,6.0,,,6.0
1500000000600000000,,7.0,7.0,7.0
1500000000700000000,8.0,,8.0,8.0
1500000000800000000,,9.0,9.0,9.0
1500000000900000000,10.0,,10.0,10.0
""",

}


class TestTransformers(object):

    def test_to_dict(self, streamset):
        assert to_dict(streamset) == expected["to_dict"]

    def test_to_array(self, streamset):
        result = to_array(streamset)
        for idx in range(4):
            assert (result[idx] == expected["to_array"][idx]).all()

    def test_to_series(self, streamset):
        """
        Asserts to_series produces correct results including series name
        """
        expected = []
        expected_names = [s.collection + "/" + s.name for s in streamset]
        for idx, points in enumerate(streamset.values()):
            values, times = list(zip(*[(p.value, p.time) for p in points]))
            times = Index(times, dtype='datetime64[ns]')
            expected.append(Series(values, times, name=expected_names[idx]))

        result = to_series(streamset)
        for idx in range(4):
            assert (result[idx] == expected[idx]).all()     # verify data
            assert result[idx].name == expected_names[idx]  # verify name

    def test_to_series_index_type(self, streamset):
        """
        assert default index type is 'datetime64[ns]'
        """
        result = to_series(streamset)
        for series in result:
            assert series.index.dtype.name == 'datetime64[ns]'

    def test_to_series_index_as_int(self, streamset):
        """
        assert datetime64_index: False produces 'int64' index
        """
        result = to_series(streamset, False)
        for series in result:
            assert series.index.dtype.name == 'int64'

    def test_to_dataframe(self, streamset):
        columns = ["time", "test/stream0", "test/stream1", "test/stream2", "test/stream3"]
        df = DataFrame(expected["to_dict"], columns=columns)
        df.set_index("time", inplace=True)
        assert to_dataframe(streamset).equals(df)

    def test_to_csv_as_path(self, streamset, tmpdir):
        path = os.path.join(tmpdir.dirname, "to_csv_test.csv")
        streamset.to_dict = Mock(return_value=expected["to_dict"])
        to_csv(streamset, path)
        with open(path, "r") as f:
            content = f.read()
        assert content == expected["csv"]
        os.remove(path)

    def test_to_csv_as_stringio(self, streamset):
        # NOTE: There are newline issues with StringIO and csv doesnt allow
        #       BytesIO object.
        string_obj = StringIO()
        streamset.to_dict = Mock(return_value=expected["to_dict"])
        to_csv(streamset, string_obj)
        string_obj.seek(0)
        reader = csv.DictReader(string_obj)

        # convert back to dicts for assert due to newline issues with CSV
        result = [dict(row) for row in reader]
        for item in result:
            for k,v in item.items():
                if v is '':
                    item[k] = None
                elif "." in v:
                    item[k] = float(v)
                else:
                    item[k] = int(v)
        assert result == expected["to_dict"]

    def test_to_table(self, streamset):
        assert to_table(streamset) == expected["table"]
