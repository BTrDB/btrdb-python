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
import pytz
import datetime
import pytest
from unittest.mock import Mock, PropertyMock
from freezegun import freeze_time

from btrdb.stream import Stream, StreamSet, StreamFilter, INSERT_BATCH_SIZE
from btrdb.point import RawPoint, StatPoint
from btrdb.exceptions import BTrDBError, InvalidOperation
from btrdb.grpcinterface import btrdb_pb2

RawPointProto =  btrdb_pb2.RawPoint
StatPointProto =  btrdb_pb2.StatPoint


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

from btrdb.conn import BTrDB
from btrdb.endpoint import Endpoint

class TestStream(object):

    def test_create(self):
        """
        Assert we can create the object
        """
        Stream(None, "FAKE")


    def test_refresh_metadata(self):
        """
        Assert refresh_metadata calls Endpoint.streamInfo
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, {}, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        stream.refresh_metadata()
        stream._btrdb.ep.streamInfo.assert_called_once_with(uu, False, True)


    ##########################################################################
    ## update tests
    ##########################################################################

    def test_update_arguments(self):
        """
        Assert update raises errors on invalid arguments
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        # no arguments
        with pytest.raises(ValueError) as exc:
            stream.update()
        assert "must supply" in str(exc)

        # tags not dict
        with pytest.raises(TypeError) as exc:
            stream.update(tags=[])
        assert "tags must be of type dict" in str(exc)

        # annotations not dict
        with pytest.raises(TypeError) as exc:
            stream.update(annotations=[])
        assert "annotations must be of type dict" in str(exc)

        # collection not string
        with pytest.raises(TypeError) as exc:
            stream.update(collection=42)
        assert "collection must be of type string" in str(exc)


    def test_update_tags(self):
        """
        Assert update calls correct Endpoint methods for tags update
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, {}, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        tags = {"cat": "dog"}

        stream.update(tags=tags)
        stream._btrdb.ep.setStreamTags.assert_called_once_with(uu=uu, expected=42,
            tags=tags, collection="koala")
        stream._btrdb.ep.setStreamAnnotations.assert_not_called()


    def test_update_collection(self):
        """
        Assert update calls correct Endpoint methods for collection update
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, {}, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        collection = "giraffe"

        stream.update(collection=collection)
        stream._btrdb.ep.setStreamTags.assert_called_once_with(uu=uu, expected=42,
            tags=stream.tags(), collection=collection)
        stream._btrdb.ep.setStreamAnnotations.assert_not_called()


    def test_update_annotations(self):
        """
        Assert update calls correct Endpoint methods for annotations update
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, {}, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        annotations = {"owner": "rabbit"}

        stream.refresh_metadata()
        stream.update(annotations=annotations)
        stream._btrdb.ep.setStreamAnnotations.assert_called_once_with(uu=uu, expected=42,
            changes=annotations)
        stream._btrdb.ep.setStreamTags.assert_not_called()


    ##########################################################################
    ## exists tests
    ##########################################################################

    def test_exists_cached_value(self):
        """
        Assert exists first uses cached value
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu, known_to_exist=True)
        stream.refresh_metadata = Mock()

        assert stream.exists()
        stream.refresh_metadata.assert_not_called()


    def test_exists(self):
        """
        Assert exists refreshes data if value is unknown
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)
        stream.refresh_metadata = Mock(return_value=True)

        assert stream.exists()
        stream.refresh_metadata.assert_called_once()


    def test_exists_returns_false_on_404(self):
        """
        Assert exists returns False on 404 error
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)
        stream.refresh_metadata = Mock(side_effect=BTrDBError(code=404, msg="hello", mash=""))

        assert stream.exists() == False
        stream.refresh_metadata.assert_called_once()


    def test_exists_passes_other_errors(self):
        """
        Assert exists does not keep non 404 errors trapped
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)
        stream.refresh_metadata = Mock(side_effect=ValueError())

        with pytest.raises(ValueError):
            stream.exists()
        stream.refresh_metadata.assert_called_once()


    ##########################################################################
    ## tag/annotation tests
    ##########################################################################

    def test_tags_returns_copy(self):
        """
        Assert tags returns a copy of the tags dict
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        token = {"cat": "dog"}
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu, tags=token)

        assert stream.tags() is not token
        assert stream.tags() == token


    def test_tags_returns_cached_values(self):
        """
        Assert tags returns a copy of the tags dict
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        token = {"cat": "dog"}
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu, tags=token,
            property_version=42)
        stream.refresh_metadata = Mock()

        assert stream.tags(refresh=False) == token
        stream.refresh_metadata.assert_not_called()


    def test_tags_forces_refresh_if_requested(self):
        """
        Assert tags calls refresh_metadata if requested even though a
        cached copy is available
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        token = {"cat": "dog"}
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu, tags=token)
        stream.refresh_metadata = Mock()

        stream.tags(refresh=True)
        stream.refresh_metadata.assert_called_once()


    def test_annotations_returns_copy_of_value(self):
        """
        Assert annotations returns a copy of the annotations dict
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        token = {"cat": "dog"}
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu, annotations=token,
            property_version=42)
        stream.refresh_metadata = Mock()

        assert stream.annotations(refresh=False)[0] == token
        assert stream.annotations(refresh=False)[0] is not token
        assert stream.annotations(refresh=False)[1] == 42


    def test_annotations_returns_cached_values(self):
        """
        Assert annotations returns a copy of the annotations dict
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        token = {"cat": "dog"}
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu, annotations=token,
            property_version=42)
        stream.refresh_metadata = Mock()

        assert stream.annotations(refresh=False)[0] == token
        stream.refresh_metadata.assert_not_called()


    def test_annotations_forces_refresh_if_requested(self):
        """
        Assert annotations calls refresh_metadata if requested even though a
        cached copy is available
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        token = {"cat": "dog"}
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu, annotations=token)
        stream.refresh_metadata = Mock()

        stream.annotations(refresh=True)
        stream.refresh_metadata.assert_called_once()


    ##########################################################################
    ## windowing tests
    ##########################################################################

    def test_windows(self):
        """
        Assert windows returns tuples of data from Endpoint.windows
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        windows = [
            [(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6), StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7)), 42],
            [(StatPointProto(time=3,min=4,mean=5,max=6,count=7,stddev=8), StatPointProto(time=4,min=5,mean=6,max=7,count=8,stddev=9)), 42],
        ]
        expected = (
            ((StatPoint(time=1,minv=2.0,meanv=3.0,maxv=4.0,count=5,stddev=6.0), 42), (StatPoint(time=2,minv=3.0,meanv=4.0,maxv=5.0,count=6,stddev=7.0), 42)),
            ((StatPoint(time=3,minv=4.0,meanv=5.0,maxv=6.0,count=7,stddev=8.0), 42), (StatPoint(time=4,minv=5.0,meanv=6.0,maxv=7.0,count=8,stddev=9.0), 42)),
        )
        endpoint.windows = Mock(return_value=windows)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        params = {"start": 100, "end": 500, "width": 2}

        result = stream.windows(**params)
        assert result == expected
        assert isinstance(result, tuple)
        assert isinstance(result[0], tuple)
        stream._btrdb.ep.windows.assert_called_once_with(
            uu, 100, 500, 2, 0, 0
        )


    def test_aligned_windows(self):
        """
        Assert windows returns tuples of data from Endpoint.alignedWindows
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        windows = [
            [(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6), StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7)), 42],
            [(StatPointProto(time=3,min=4,mean=5,max=6,count=7,stddev=8), StatPointProto(time=4,min=5,mean=6,max=7,count=8,stddev=9)), 42],
        ]
        expected = (
            ((StatPoint(time=1,minv=2.0,meanv=3.0,maxv=4.0,count=5,stddev=6.0), 42), (StatPoint(time=2,minv=3.0,meanv=4.0,maxv=5.0,count=6,stddev=7.0), 42)),
            ((StatPoint(time=3,minv=4.0,meanv=5.0,maxv=6.0,count=7,stddev=8.0), 42), (StatPoint(time=4,minv=5.0,meanv=6.0,maxv=7.0,count=8,stddev=9.0), 42)),
        )
        endpoint.alignedWindows = Mock(return_value=windows)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        params = {"start": 100, "end": 500, "pointwidth": 1}

        result = stream.aligned_windows(**params)
        assert result == expected
        assert isinstance(result, tuple)
        assert isinstance(result[0], tuple)
        stream._btrdb.ep.alignedWindows.assert_called_once_with(
            uu, 100, 500, 1, 0
        )


    ##########################################################################
    ## earliest/latest tests
    ##########################################################################

    def test_earliest(self):
        """
        Assert earliest calls Endpoint.nearest
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(return_value=(RawPointProto(time=100, value=1.0), 42))

        point, ver = stream.earliest()
        assert (point, ver) == (RawPoint(100, 1.0), 42)
        endpoint.nearest.assert_called_once_with(uu, 0, 0, False)


    def test_earliest_swallows_exception(self):
        """
        Assert earliest returns None when endpoint throws exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=Exception())

        assert stream.earliest() is None
        endpoint.nearest.assert_called_once_with(uu, 0, 0, False)


    def test_latest(self):
        """
        Assert latest calls Endpoint.nearest
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(return_value=(RawPointProto(time=100, value=1.0), 42))

        with freeze_time("2018-01-01 12:00:00", tz_offset=0):
            point, ver = stream.latest()

        assert (point, ver) == (RawPoint(100, 1.0), 42)
        endpoint.nearest.assert_called_once_with(uu, 1514826000000000000, 0, True)


    def test_latest_swallows_exception(self):
        """
        Assert latest returns None when endpoint throws exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=Exception())

        with freeze_time(datetime.datetime(2018,1,1,12, tzinfo=pytz.utc)):
            assert stream.latest() is None
        endpoint.nearest.assert_called_once_with(uu, 1514826000000000000, 0, True)


    ##########################################################################
    ## misc tests
    ##########################################################################

    def test_version(self):
        """
        Assert version calls and returns correct value from streamInfo
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("", 0, {}, {}, 42))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        assert stream.version() == 42
        stream._btrdb.ep.streamInfo.assert_called_once_with(uu, True, False)


    def test_insert(self):
        """
        Assert insert batches data to endpoint insert and returns version
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.insert = Mock(side_effect=[1,2])
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        data = list(zip(range(10000,16000), map(float, range(6000))))
        version = stream.insert(data)

        assert stream._btrdb.ep.insert.call_args_list[0][0][1] == data[:INSERT_BATCH_SIZE]
        assert stream._btrdb.ep.insert.call_args_list[1][0][1] == data[INSERT_BATCH_SIZE:]
        assert version == 2


    def test_nearest(self):
        """
        Assert nearest calls Endpoint.nearest with correct arguments
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.nearest = Mock(return_value=(RawPointProto(time=100, value=1.0), 42))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        point, version = stream.nearest(5, 10, False)
        stream._btrdb.ep.nearest.assert_called_once_with(uu, 5, 10, False)
        assert point == RawPoint(100, 1.0)
        assert version == 42


    def test_delete_range(self):
        """
        Assert delete_range calls Endpoint.deleteRange with correct arguments
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)

        stream.delete(5, 10)
        stream._btrdb.ep.deleteRange.assert_called_once_with(uu, 5, 10)


    def test_flush(self):
        """
        Assert flush calls Endpoint.flush with UUID
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)

        stream.flush()
        stream._btrdb.ep.flush.assert_called_once_with(uu)




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
            stream1.uuid: stream1.version(),
            stream2.uuid: stream2.version()
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
            stream1.uuid: stream1.version(),
            stream2.uuid: stream2.version()
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
        assert streams.latest() == (RawPoint(time=10, value=1), RawPoint(time=20, value=1))


    def test_latest(self, stream1, stream2):
        """
        Assert latest returns correct time code
        """
        streams = StreamSet([stream1, stream2])
        assert streams.latest() == (RawPoint(time=10, value=1), RawPoint(time=20, value=1))


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
        assert streams.values() == [
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
