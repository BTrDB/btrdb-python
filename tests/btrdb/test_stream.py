# tests.test_stream
# Testing package for the btrdb stream module
#
# Author:   PingThings
# Created:  Wed Jan 02 19:26:20 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_stream.py [] allen@pingthings.io $

"""
Testing package for the btrdb stream module
"""

##########################################################################
## Imports
##########################################################################

import uuid
import pytest
from unittest.mock import Mock, PropertyMock

from btrdb.stream import Stream, StreamSet, StreamFilter
from btrdb.point import RawPoint


##########################################################################
## Fixtures
##########################################################################

@pytest.fixture
def stream1():
    uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
    stream = Mock(Stream)
    stream.version = Mock(return_value=11)
    stream.uuid = Mock(return_value=uu)
    stream.nearest = Mock(return_value=(RawPoint(time=10, value=1), 11))
    return stream


@pytest.fixture
def stream2():
    uu = uuid.UUID('17dbe387-89ea-42b6-864b-f505cdb483f5')
    stream = Mock(Stream)
    stream.version = Mock(return_value=22)
    stream.uuid = Mock(return_value=uu)
    stream.nearest = Mock(return_value=(RawPoint(time=20, value=1), 22))
    return stream


##########################################################################
## Stream Tests
##########################################################################

class TestStream(object):

    def test_create(self):
        """
        Assert we can create the object
        """
        Stream(None, "FAKE")

##########################################################################
## StreamSet Tests
##########################################################################

class TestStreamSet(object):

    def test_create(self):
        """
        Assert we can create the object
        """
        StreamSet([])

    ##########################################################################
    ## allow_window tests
    ##########################################################################

    def test_allow_window(self):
        """
        Assert allow_window returns False if window already requested
        """
        streams = StreamSet([1,2,3])
        assert streams.allow_window == True

        streams.windows(30, 4)
        assert streams.allow_window == False


        streams = StreamSet([1,2,3])
        streams.aligned_windows(30)
        assert streams.allow_window == False


    ##########################################################################
    ## _latest_versions tests
    ##########################################################################

    def test_latest_versions(self, stream1, stream2):
        """
        Assert _latest_versions returns correct values
        """
        streams = StreamSet([stream1, stream2])
        expected = {
            stream1.uuid(): stream1.version(),
            stream2.uuid(): stream2.version()
        }
        assert streams._latest_versions() == expected


    ##########################################################################
    ## pin_versions tests
    ##########################################################################

    def test_pin_versions_returns_self(self, stream1):
        """
        Assert pin_versions returns self
        """
        streams = StreamSet([stream1])
        expected = {
            stream1.uuid(): stream1.version()
        }
        result =  streams.pin_versions(expected)
        assert streams is result


    def test_pin_versions_with_argument(self):
        """
        Assert pin_versions uses supplied version numbers
        """
        streams = StreamSet([1,2])
        expected = [3,4]
        assert streams.pin_versions(expected) == streams
        assert streams._pinned_versions == expected


    def test_pin_versions_with_argument(self):
        """
        Assert pin_versions uses supplied version numbers
        """
        streams = StreamSet([1,2])
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        expected = {
            uu: 42
        }
        assert streams.pin_versions(expected) == streams
        assert streams._pinned_versions == expected


    def test_pin_versions_no_argument(self, stream1, stream2):
        """
        Assert pin_versions uses latest version numbers
        """
        streams = StreamSet([stream1, stream2])
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        expected = {
            uu: 42
        }

        assert streams.pin_versions(expected) == streams
        assert streams._pinned_versions == expected


    def test_pin_versions_raise_on_non_dict(self):
        """
        Assert pin_versions raises if versions argument is not dict
        """
        streams = StreamSet([1])
        expected = "INVALID DATA"

        with pytest.raises(TypeError) as e:
            streams.pin_versions(expected) == streams
        assert "dict" in str(e).lower()


    def test_pin_versions_raise_on_non_uuid_key(self):
        """
        Assert pin_versions raises if versions argument dict does not have UUID
        for keys
        """
        streams = StreamSet([1])
        expected = {"uuid": 42}

        with pytest.raises(TypeError) as e:
            streams.pin_versions(expected) == streams
        assert "uuid" in str(e).lower()


    ##########################################################################
    ## versions tests
    ##########################################################################

    def test_versions_no_pin(self, stream1, stream2):
        """
        Assert versions returns correctly if pin_versions not called
        """
        streams = StreamSet([stream1, stream2])
        expected = {
            stream1.uuid(): stream1.version(),
            stream2.uuid(): stream2.version()
        }

        assert streams.versions() == expected


    def test_versions_with_pin(self, stream1, stream2):
        """
        Assert versions returns correctly if pin_versions called
        """
        uu1 = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        s1 = Mock(Stream)
        s1.version = Mock(return_value=11)
        s1.uuid = Mock(return_value=uu1)

        uu2 = uuid.UUID('17dbe387-89ea-42b6-864b-f505cdb483f5')
        s2 = Mock(Stream)
        s2.version = Mock(return_value=22)
        s2.uuid = Mock(return_value=uu2)

        streams = StreamSet([stream1, stream2])
        expected = {
            stream1.uuid(): 88,
            stream2.uuid(): 99
        }
        streams.pin_versions(expected)
        assert streams.versions() == expected


    ##########################################################################
    ## earliest/latest tests
    ##########################################################################

    def test_earliest(self, stream1, stream2):
        """
        Assert earliest returns correct time code
        """
        streams = StreamSet([stream1, stream2])
        assert streams.earliest() == 10


    def test_latest(self, stream1, stream2):
        """
        Assert latest returns correct time code
        """
        streams = StreamSet([stream1, stream2])
        assert streams.latest() == 20


    ##########################################################################
    ## filter tests
    ##########################################################################

    def test_filter(self, stream1):
        """
        Assert filter creates and stores a StreamFilter object
        """
        streams = StreamSet([stream1])
        start, end = 1, 100
        streams = streams.filter(start=start, end=end)

        assert len(streams.filters) == 1
        assert streams.filters[0].start == start
        assert streams.filters[0].end == end
        assert isinstance(streams.filters[0], StreamFilter)


    def test_filter_raises(self, stream1):
        """
        Assert filter raises ValueError
        """
        streams = StreamSet([stream1])

        with pytest.raises(ValueError) as exc:
            streams = streams.filter(start=None, end=None)
        assert "must be supplied" in str(exc).lower()


    def test_filter_returns_new_instance(self, stream1):
        """
        Assert filter returns new instance
        """
        streams = StreamSet([stream1])
        start, end = 1, 100
        other = streams.filter(start=start, end=end)

        assert other is not streams
        assert isinstance(other, streams.__class__)

    ##########################################################################
    ## clone tests
    ##########################################################################

    def test_clone_returns_new_object(self, stream1, stream2):
        """
        Assert that clone returns a different object
        """
        streams = StreamSet([stream1, stream2])
        clone = streams.clone()

        assert id(clone) != id(streams)
        assert clone._streams is streams._streams


    ##########################################################################
    ## windows tests
    ##########################################################################

    def test_windows_raises_valueerror(self, stream1):
        """
        Assert that raises ValueError if arguments not castable to int
        """
        streams = StreamSet([stream1])
        with pytest.raises(ValueError) as exc:
            streams.windows("invalid", 42)
        assert "literal" in str(exc).lower()

        with pytest.raises(ValueError) as exc:
            streams.windows(42, "invalid")
        assert "literal" in str(exc).lower()


    def test_windows_raises_if_not_allowed(self, stream1):
        """
        Assert windows raises Exception if not allowed
        """
        streams = StreamSet([stream1])
        streams.windows(8, 22)

        with pytest.raises(Exception) as exc:
            streams.windows(10, 20)
        assert "window operation is already requested" in str(exc).lower()


    def test_windows_returns_self(self, stream1):
        """
        Assert windows returns self
        """
        streams = StreamSet([stream1])
        result = streams.windows(10, 20)
        assert result is streams


    def test_windows(self, stream1):
        """
        Assert windows stores values
        """
        streams = StreamSet([stream1])
        result = streams.windows(10, 20)

        # assert stores values
        assert streams.width == 10
        assert streams.depth == 20


    ##########################################################################
    ## aligned_windows tests
    ##########################################################################

    def test_aligned_windows_raises_valueerror(self, stream1):
        """
        Assert aligned_windows raises ValueError if argument not castable to int
        """
        streams = StreamSet([stream1])
        with pytest.raises(ValueError) as exc:
            streams.aligned_windows("invalid")
        assert "literal" in str(exc).lower()


    def test_aligned_windows_raises_if_not_allowed(self, stream1):
        """
        Assert that aligned_windows raises Exception if not allowed
        """
        streams = StreamSet([stream1])
        streams.windows(8, 22)

        with pytest.raises(Exception) as exc:
            streams.aligned_windows(20)
        assert "window operation is already requested" in str(exc).lower()


    def test_aligned_windows(self, stream1):
        """
        Assert aligned_windows stores objects
        """
        streams = StreamSet([stream1])
        result = streams.aligned_windows(20)
        assert streams.pointwidth == 20


    def test_aligned_windows_returns_self(self, stream1):
        """
        Assert aligned_windows returns self
        """
        streams = StreamSet([stream1])
        result = streams.aligned_windows(20)
        assert result is streams


    ##########################################################################
    ## rows tests
    ##########################################################################

    def test_rows(self, stream1, stream2):
        """
        Assert rows returns correct values
        """
        stream1.values = Mock(return_value=iter([
            (RawPoint(time=1, value=1), 1), (RawPoint(time=2, value=2), 1),
            (RawPoint(time=3, value=3), 1), (RawPoint(time=4, value=4), 1),
        ]))
        stream2.values = Mock(return_value=iter([
            (RawPoint(time=1, value=1), 2), (RawPoint(time=3, value=3), 2)
        ]))

        streams = StreamSet([stream1, stream2])
        rows = streams.rows()

        assert next(rows) == (RawPoint(time=1, value=1), RawPoint(time=1, value=1))
        assert next(rows) == (RawPoint(time=2, value=2), None)
        assert next(rows) == (RawPoint(time=3, value=3), RawPoint(time=3, value=3))
        assert next(rows) == (RawPoint(time=4, value=4), None)


    ##########################################################################
    ## _params_from_filters tests
    ##########################################################################

    def test_params_from_filters_latest_value_wins(self, stream1):
        """
        Assert _params_from_filters returns latest setting
        """
        streams = StreamSet([stream1])
        assert streams.filters == []

        streams = streams.filter(start=1)
        assert streams._params_from_filters() == {"start": 1}

        streams = streams.filter(start=2)
        assert streams._params_from_filters() == {"start": 2}


    def test_params_from_filters_works(self, stream1):
        """
        Assert _params_from_filters returns correct values
        """
        streams = StreamSet([stream1])
        assert streams.filters == []

        streams = streams.filter(start=1, end=2)
        assert streams._params_from_filters() == {"start": 1, "end": 2}

        streams = streams.filter(start=9, end=10)
        assert streams._params_from_filters() == {"start": 9, "end": 10}


    ##########################################################################
    ## values tests
    ##########################################################################

    def test_values(self, stream1, stream2):
        """
        Assert values returns correct data
        """
        stream1_values = [
            (RawPoint(time=1, value=1), 1), (RawPoint(time=2, value=2), 1),
            (RawPoint(time=3, value=3), 1), (RawPoint(time=4, value=4), 1),
        ]
        stream1.values = Mock(return_value=iter(stream1_values))

        stream2_values = [
            (RawPoint(time=1, value=1), 2), (RawPoint(time=3, value=3), 2)
        ]
        stream2.values = Mock(return_value=iter(stream2_values))

        streams = StreamSet([stream1, stream2])
        assert streams.values == [
            [t[0] for t in stream1_values],
            [t[0] for t in stream2_values],
        ]


##########################################################################
## StreamFilter Tests
##########################################################################

class TestStreamFilter(object):

    def test_create(self):
        """
        Assert we can create the object
        """
        StreamFilter(0,1)

    def test_start_larger_or_equal(self):
        """
        Assert we raise ValueError if start is greater than/equal to end
        """
        with pytest.raises(ValueError):
            StreamFilter(1,1)

    def test_start_valid(self):
        """
        Assert we can use any value castable to an int for start argument
        """
        sf = StreamFilter(start="100")
        assert sf.start == 100

        sf = StreamFilter(start=100.0)
        assert sf.start == 100

        sf = StreamFilter(start=100)
        assert sf.start == 100

    def test_start_invalid(self):
        """
        Assert we raise ValueError if start is greater than/equal to end
        """
        with pytest.raises(ValueError):
            StreamFilter(start="foo")
