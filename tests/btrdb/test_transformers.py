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
from btrdb.point import RawPoint, StatPoint
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


@pytest.fixture
def statpoint_streamset():
    rows = [
        [StatPoint(1500000000100000000, 0, 2.0, 2.5, 10, 1),StatPoint(1500000000100000000, 0, 4.0, 4.5, 11, 1),StatPoint(1500000000100000000, 0, 6.0, 6.5, 10, 1),StatPoint(1500000000100000000, 0, 8.0, 8.5, 11, 2)],
        [StatPoint(1500000000300000000, 0, 3.0, 3.5, 11, 1),StatPoint(1500000000300000000, 0, 5.0, 5.5, 10, 1),StatPoint(1500000000300000000, 0, 7.0, 7.5, 10, 1),StatPoint(1500000000300000000, 0, 9.0, 9.5, 11, 2)],
        [StatPoint(1500000000500000000, 0, 4.0, 4.5, 10, 1),StatPoint(1500000000500000000, 0, 6.0, 6.5, 11, 1),StatPoint(1500000000500000000, 0, 8.0, 8.5, 10, 1),StatPoint(1500000000500000000, 0, 10.0, 10.5, 11, 2)],
        [StatPoint(1500000000700000000, 0, 5.0, 5.5, 11, 1),StatPoint(1500000000700000000, 0, 7.0, 7.5, 10, 1),StatPoint(1500000000700000000, 0, 9.0, 9.5, 10, 1),StatPoint(1500000000700000000, 0, 11.0, 11.5, 11, 2)],
    ]
    values = [
        [StatPoint(1500000000100000000, 0, 2.0, 2.5, 10, 1),StatPoint(1500000000300000000, 0, 4.0, 4.5, 11, 1),StatPoint(1500000000500000000, 0, 6.0, 6.5, 10, 1),StatPoint(1500000000700000000, 0, 8.0, 8.5, 11, 2)],
        [StatPoint(1500000000100000000, 1, 3.0, 3.5, 11, 1),StatPoint(1500000000300000000, 1, 5.0, 5.5, 10, 1),StatPoint(1500000000500000000, 1, 7.0, 7.5, 10, 1),StatPoint(1500000000700000000, 1, 9.0, 9.5, 11, 2)],
        [StatPoint(1500000000100000000, 2, 4.0, 4.5, 10, 1),StatPoint(1500000000300000000, 2, 6.0, 6.5, 11, 1),StatPoint(1500000000500000000, 2, 8.0, 8.5, 10, 1),StatPoint(1500000000700000000, 2, 10.0, 10.5, 11, 2)],
        [StatPoint(1500000000100000000, 3, 5.0, 5.5, 11, 1),StatPoint(1500000000300000000, 3, 7.0, 7.5, 10, 1),StatPoint(1500000000500000000, 3, 9.0, 9.5, 10, 1),StatPoint(1500000000700000000, 3, 11.0, 11.5, 11, 2)],
    ]
    streams = []
    for idx in range(4):
        stream = Mock(Stream)
        type(stream).collection = PropertyMock(return_value="test")
        type(stream).name = PropertyMock(return_value="stream{}".format(idx))
        streams.append(stream)

    obj = StreamSet(streams)
    obj.rows = Mock(return_value=rows)
    obj.values = Mock(return_value=values)
    obj.pointwidth = 20
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
        np.array([2.0, 4.0, 6.0, 8.0, 10.0]),
        np.array([1.0, 3.0, 5.0, 7.0, 9.0]),
        np.array([1.0, 2.0, 4.0, 5.0, 7.0, 8.0, 9.0, 10.0]),
        np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
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

    ##########################################################################
    ## to_dict Tests
    ##########################################################################

    def test_to_dict(self, streamset):
        assert to_dict(streamset) == expected["to_dict"]

    def test_to_dict_statpoint(self, statpoint_streamset):
        expected = [
            {'time': 1500000000100000000, 'test/stream0': 2.0, 'test/stream1': 4.0, 'test/stream2': 6.0, 'test/stream3': 8.0},
            {'time': 1500000000300000000, 'test/stream0': 3.0, 'test/stream1': 5.0, 'test/stream2': 7.0, 'test/stream3': 9.0},
            {'time': 1500000000500000000, 'test/stream0': 4.0, 'test/stream1': 6.0, 'test/stream2': 8.0, 'test/stream3': 10.0},
            {'time': 1500000000700000000, 'test/stream0': 5.0, 'test/stream1': 7.0, 'test/stream2': 9.0, 'test/stream3': 11.0},
        ]
        assert to_dict(statpoint_streamset) == expected

    def test_to_dict_statpoint_agg_count(self, statpoint_streamset):
        expected = [
            {'time': 1500000000100000000, 'test/stream0': 10, 'test/stream1': 11, 'test/stream2': 10, 'test/stream3': 11},
            {'time': 1500000000300000000, 'test/stream0': 11, 'test/stream1': 10, 'test/stream2': 10, 'test/stream3': 11},
            {'time': 1500000000500000000, 'test/stream0': 10, 'test/stream1': 11, 'test/stream2': 10, 'test/stream3': 11},
            {'time': 1500000000700000000, 'test/stream0': 11, 'test/stream1': 10, 'test/stream2': 10, 'test/stream3': 11},
        ]
        assert to_dict(statpoint_streamset, agg='count') == expected

    def test_to_dict_statpoint_agg_all(self, statpoint_streamset):
        expected = [
            OrderedDict([('time', 1500000000100000000), ('test/stream0-min', 0), ('test/stream0-mean', 2.0), ('test/stream0-max', 2.5), ('test/stream0-count', 10), ('test/stream0-stddev', 1), ('test/stream1-min', 0), ('test/stream1-mean', 4.0), ('test/stream1-max', 4.5), ('test/stream1-count', 11), ('test/stream1-stddev', 1), ('test/stream2-min', 0), ('test/stream2-mean', 6.0), ('test/stream2-max', 6.5), ('test/stream2-count', 10), ('test/stream2-stddev', 1), ('test/stream3-min', 0), ('test/stream3-mean', 8.0), ('test/stream3-max', 8.5), ('test/stream3-count', 11), ('test/stream3-stddev', 2)]),
            OrderedDict([('time', 1500000000300000000), ('test/stream0-min', 0), ('test/stream0-mean', 3.0), ('test/stream0-max', 3.5), ('test/stream0-count', 11), ('test/stream0-stddev', 1), ('test/stream1-min', 0), ('test/stream1-mean', 5.0), ('test/stream1-max', 5.5), ('test/stream1-count', 10), ('test/stream1-stddev', 1), ('test/stream2-min', 0), ('test/stream2-mean', 7.0), ('test/stream2-max', 7.5), ('test/stream2-count', 10), ('test/stream2-stddev', 1), ('test/stream3-min', 0), ('test/stream3-mean', 9.0), ('test/stream3-max', 9.5), ('test/stream3-count', 11), ('test/stream3-stddev', 2)]),
            OrderedDict([('time', 1500000000500000000), ('test/stream0-min', 0), ('test/stream0-mean', 4.0), ('test/stream0-max', 4.5), ('test/stream0-count', 10), ('test/stream0-stddev', 1), ('test/stream1-min', 0), ('test/stream1-mean', 6.0), ('test/stream1-max', 6.5), ('test/stream1-count', 11), ('test/stream1-stddev', 1), ('test/stream2-min', 0), ('test/stream2-mean', 8.0), ('test/stream2-max', 8.5), ('test/stream2-count', 10), ('test/stream2-stddev', 1), ('test/stream3-min', 0), ('test/stream3-mean', 10.0), ('test/stream3-max', 10.5), ('test/stream3-count', 11), ('test/stream3-stddev', 2)]),
            OrderedDict([('time', 1500000000700000000), ('test/stream0-min', 0), ('test/stream0-mean', 5.0), ('test/stream0-max', 5.5), ('test/stream0-count', 11), ('test/stream0-stddev', 1), ('test/stream1-min', 0), ('test/stream1-mean', 7.0), ('test/stream1-max', 7.5), ('test/stream1-count', 10), ('test/stream1-stddev', 1), ('test/stream2-min', 0), ('test/stream2-mean', 9.0), ('test/stream2-max', 9.5), ('test/stream2-count', 10), ('test/stream2-stddev', 1), ('test/stream3-min', 0), ('test/stream3-mean', 11.0), ('test/stream3-max', 11.5), ('test/stream3-count', 11), ('test/stream3-stddev', 2)])
        ]
        assert to_dict(statpoint_streamset, agg='all') == expected

    ##########################################################################
    ## to_array Tests
    ##########################################################################

    def test_to_array(self, streamset):
        """
        asserts to_array of rawpoint returns multidimensional ndarry of values
        """
        result = to_array(streamset)
        for idx in range(4):
            assert (result[idx] == expected["to_array"][idx]).all()

    def test_to_array_statpoint(self, statpoint_streamset):
        """
        asserts to_array of statpoint returns ndarry of requested values
        """
        result = to_array(statpoint_streamset)
        assert result.__class__ == np.ndarray
        assert (result[0] == np.array([2.0, 4.0, 6.0, 8.0])).all()
        assert (result[1] == np.array([3.0, 5.0, 7.0, 9.0])).all()
        assert (result[2] == np.array([4.0, 6.0, 8.0, 10.0])).all()
        assert (result[3] == np.array([5.0, 7.0, 9.0, 11.0])).all()

        result = to_array(statpoint_streamset, agg="min")
        assert result.__class__ == np.ndarray
        assert (result[0] == np.array([0.0, 0.0, 0.0, 0.0,])).all()
        assert (result[1] == np.array([1.0, 1.0, 1.0, 1.0, ])).all()
        assert (result[2] == np.array([2.0, 2.0, 2.0, 2.0, ])).all()
        assert (result[3] == np.array([3.0, 3.0, 3.0, 3.0, ])).all()

    ##########################################################################
    ## to_series Tests
    ##########################################################################

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

    def test_to_series_name_lambda(self, streamset):
        """
        assert to_dateframe uses name lambda
        """
        result = streamset.to_series(name_callable=lambda s: s.name)
        assert [s.name for s in result] == ['stream0', 'stream1', 'stream2', 'stream3']

    def test_to_series_ignores_rawpoint_with_agg(self, streamset):
        """
        assert to_series ignores agg if RawPoints are used
        """
        result = streamset.to_series(agg="mean")
        assert len(result) == 4

    def test_to_series_statpoint(self, statpoint_streamset):
        """
        Asserts to_series produces correct results with statpoints
        """
        result = to_series(statpoint_streamset)
        values = [2.0, 4.0, 6.0, 8.0]
        index = [1500000000100000000, 1500000000300000000, 1500000000500000000, 1500000000700000000]
        assert (result[0] == Series(values, index=index)).all()

        values = [3.0, 5.0, 7.0, 9.0]
        index = [1500000000100000000, 1500000000300000000, 1500000000500000000, 1500000000700000000]
        assert (result[1] == Series(values, index=index)).all()

        result = to_series(statpoint_streamset, agg="max")
        values = [2.5, 4.5, 6.5, 8.5]
        index = [1500000000100000000, 1500000000300000000, 1500000000500000000, 1500000000700000000]
        assert (result[0] == Series(values, index=index)).all()

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

    def test_to_series_index_as_int(self, streamset):
        """
        assert datetime64_index: False produces 'int64' index
        """
        result = to_series(streamset, False)
        for series in result:
            assert series.index.dtype.name == 'int64'

    def test_to_series_raises_on_agg_all(self, statpoint_streamset):
        """
        asserts to_series raises error if using "all" as agg.
        """
        with pytest.raises(AttributeError):
            statpoint_streamset.to_series(agg="all")

    ##########################################################################
    ## to_dataframe Tests
    ##########################################################################

    def test_to_dataframe(self, streamset):
        """
        assert to_dateframe works on RawPoints
        """
        columns = ["time", "test/stream0", "test/stream1", "test/stream2", "test/stream3"]
        df = DataFrame(expected["to_dict"], columns=columns)
        df.set_index("time", inplace=True)
        assert to_dataframe(streamset).equals(df)

    def test_to_dataframe_column_issues_warning(self, statpoint_streamset):
        """
        assert to_dateframe with column argument issues warning
        """
        columns = ["test/cats", "test/dogs", "test/horses", "test/fishes"]
        with pytest.deprecated_call():
            statpoint_streamset.to_dataframe(columns=columns)

    def test_to_dataframe_column(self, statpoint_streamset):
        """
        assert to_dateframe with column argument actually renames columns
        """
        columns = ["test/cats", "test/dogs", "test/horses", "test/fishes"]
        with pytest.deprecated_call():
            df = statpoint_streamset.to_dataframe(columns=columns)

        assert df.columns.tolist() == columns

    def test_to_dataframe_multindex(self, statpoint_streamset):
        """
        assert to_dateframe agg=all creates columned multiindex
        """
        df = statpoint_streamset.to_dataframe(agg="all")
        assert len(df.columns.levels) == 3
        assert list(df.columns.levels[0]) == ['test']
        assert list(df.columns.levels[1]) == ['stream0', 'stream1', 'stream2', 'stream3']
        assert list(df.columns.levels[2]) == ['count', 'max', 'mean', 'min', 'stddev']

    def test_to_dataframe_name_lambda(self, streamset):
        """
        assert to_dateframe uses name lambda
        """
        columns = ["stream0", "stream1", "stream2", "stream3"]
        df = streamset.to_dataframe(name_callable=lambda s: s.name)

        assert len(columns) == len(df.columns)
        for idx in range(len(columns)):
            assert columns[idx] == df.columns[idx]

    def test_to_dataframe_rawpoint_with_agg(self, streamset):
        """
        assert to_dateframe ignores agg if RawPoint
        """
        df = streamset.to_dataframe(agg="all")
        assert df.shape == (10,4)


    def test_to_dataframe_statpoints(self, statpoint_streamset):
        """
        assert to_dateframe works on StatPoints
        """
        df = statpoint_streamset.to_dataframe()

        assert df["test/stream0"].tolist() == [2.0, 3.0, 4.0, 5.0]
        assert df["test/stream1"].tolist() == [4.0, 5.0, 6.0, 7.0]
        assert df["test/stream2"].tolist() == [6.0, 7.0, 8.0, 9.0]
        assert df["test/stream3"].tolist() == [8.0, 9.0, 10.0, 11.0]

    def test_to_dataframe_agg(self, statpoint_streamset):
        """
        assert to_dateframe respects agg argument for StatPoints
        """
        df = statpoint_streamset.to_dataframe(agg="count")
        assert df["test/stream0"].tolist() == [10, 11, 10, 11]

        df = statpoint_streamset.to_dataframe(agg="min")
        assert df["test/stream3"].tolist() == [0, 0, 0, 0]

        df = statpoint_streamset.to_dataframe(agg="mean")
        assert df["test/stream0"].tolist() == [2, 3, 4, 5]

        df = statpoint_streamset.to_dataframe(agg="max")
        assert df["test/stream0"].tolist() == [2.5, 3.5, 4.5, 5.5]

        df = statpoint_streamset.to_dataframe(agg="stddev")
        assert df["test/stream0"].tolist() == [1, 1, 1, 1]

    ##########################################################################
    ## to_csv Tests
    ##########################################################################

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

    def test_to_csv_raises_on_agg_all(self, statpoint_streamset):
        """
        asserts to_csv raises error if using "all" as agg.
        """
        with pytest.raises(AttributeError):
            statpoint_streamset.to_csv("tmp.txt", agg="all")

    ##########################################################################
    ## to_table Tests
    ##########################################################################

    def test_to_table(self, streamset):
        """
        asserts to_table returns formatted ASCII table
        """
        assert to_table(streamset) == expected["table"]

    def test_to_table(self, statpoint_streamset):
        """
        asserts to_table handles statpoints with requested key
        """
        expected = """               time    test/stream0    test/stream1    test/stream2    test/stream3
-------------------  --------------  --------------  --------------  --------------
1500000000100000000               2               4               6               8
1500000000300000000               3               5               7               9
1500000000500000000               4               6               8              10
1500000000700000000               5               7               9              11"""
        assert to_table(statpoint_streamset) == expected

        expected = """               time    test/stream0    test/stream1    test/stream2    test/stream3
-------------------  --------------  --------------  --------------  --------------
1500000000100000000             2.5             4.5             6.5             8.5
1500000000300000000             3.5             5.5             7.5             9.5
1500000000500000000             4.5             6.5             8.5            10.5
1500000000700000000             5.5             7.5             9.5            11.5"""
        assert to_table(statpoint_streamset, agg="max") == expected

    def test_to_table_name_callable(self, statpoint_streamset):
        """
        asserts to_table handles name_callable
        """
        expected = """               time    stream0    stream1    stream2    stream3
-------------------  ---------  ---------  ---------  ---------
1500000000100000000          2          4          6          8
1500000000300000000          3          5          7          9
1500000000500000000          4          6          8         10
1500000000700000000          5          7          9         11"""
        assert to_table(statpoint_streamset, name_callable=lambda s: s.name) == expected

    def test_to_table_raises_on_agg_all(self, statpoint_streamset):
        """
        asserts to_table raises error if using "all" as agg.
        """
        with pytest.raises(AttributeError):
            to_table(statpoint_streamset, agg="all")