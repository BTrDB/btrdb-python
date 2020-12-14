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

import re
import sys
import json
import uuid
import pytz
import pytest
import datetime
from unittest.mock import Mock, PropertyMock, patch, call

from btrdb.conn import BTrDB
from btrdb.endpoint import Endpoint
from btrdb import MINIMUM_TIME, MAXIMUM_TIME
from btrdb.stream import Stream, StreamSet, StreamFilter, INSERT_BATCH_SIZE
from btrdb.point import RawPoint, StatPoint
from btrdb.exceptions import BTrDBError, InvalidOperation
from btrdb.grpcinterface import btrdb_pb2

RawPointProto =  btrdb_pb2.RawPoint
StatPointProto =  btrdb_pb2.StatPoint
EST = pytz.timezone('America/New_York')


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
    type(stream).collection = PropertyMock(return_value="fruits/apple")
    type(stream).name = PropertyMock(return_value="gala")
    stream.tags = Mock(return_value={"name": "gala", "unit": "volts"})
    stream.annotations = Mock(return_value=({"owner": "ABC", "color": "red"}, 11))
    return stream


@pytest.fixture
def stream2():
    uu = uuid.UUID('17dbe387-89ea-42b6-864b-f505cdb483f5')
    stream = Mock(Stream)
    stream.version = Mock(return_value=22)
    stream.uuid = Mock(return_value=uu)
    stream.nearest = Mock(return_value=(RawPoint(time=20, value=1), 22))
    type(stream).collection = PropertyMock(return_value="fruits/orange")
    type(stream).name = PropertyMock(return_value="blood")
    stream.tags = Mock(return_value={"name": "blood", "unit": "amps"})
    stream.annotations = Mock(return_value=({"owner": "ABC", "color": "orange"}, 22))
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


    def test_repr_str(self):
        """
        Assert the repr and str output are correct
        """
        COLLECTION = "relay/foo"
        NAME = "LINE222VA-ANG"
        stream = Stream(None, "FAKE")
        stream._collection = COLLECTION
        stream._tags = {"name": NAME}

        expected = "<Stream collection={} name={}>".format(
            COLLECTION, NAME
        )
        assert stream.__repr__() == expected
        assert stream.__str__() == expected


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


    def test_refresh_metadata_deserializes_annotations(self):
        """
        Assert refresh_metadata deserializes annotation values
        """
        uu = uuid.uuid4()
        serialized = {
            'acronym': 'VPHM',
            'description': 'El Segundo PMU 42 Ean',
            'devacronym': 'PMU!EL_SEG_PMU_42',
            'enabled': 'true',
            'id': '76932ae4-09bc-472c-8dc6-64fea68d2797',
            'phase': 'A',
            'label': 'null',
            'frequency': '30',
            'control': '2019-11-07 13:21:23.000000-0500',
            "calibrate": '{"racf": 1.8, "pacf": 0.005}',
        }
        expected = {
            'acronym': 'VPHM',
            'description': 'El Segundo PMU 42 Ean',
            'devacronym': 'PMU!EL_SEG_PMU_42',
            'enabled': True,
            'id': '76932ae4-09bc-472c-8dc6-64fea68d2797',
            'phase': 'A',
            'label': None,
            'frequency': 30,
            'control': '2019-11-07 13:21:23.000000-0500',
            "calibrate": {"racf": 1.8, "pacf": 0.005},
        }

        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, serialized, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        stream.refresh_metadata()
        assert stream.annotations()[0] == expected


    def test_stream_name_property(self):
        """
        Assert name property comes from tags
        """
        name = "LINE222VA-ANG"
        stream = Stream(None, "FAKE_UUID")
        stream._tags = {"name": name}
        assert stream.name == name


    def test_stream_unit_property(self):
        """
        Assert unit property comes from tags
        """
        unit = "whales"
        stream = Stream(None, "FAKE_UUID")
        stream._tags = {"unit": unit}
        assert stream.unit == unit


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

        # Test realistic annotations with multiple types
        annotations = {
            "acronym": "VPHM",
            "description": "El Segundo PMU 42 Ean",
            "devacronym": "PMU!EL_SEG_PMU_42",
            "enabled": True,
            "id": uuid.UUID('76932ae4-09bc-472c-8dc6-64fea68d2797'),
            "phase": "A",
            "label": None,
            "frequency": 30,
            "control": EST.localize(datetime.datetime(2019, 11, 7, 13, 21, 23)),
            "calibrate": {"racf": 1.8, "pacf": 0.005},
        }

        stream.refresh_metadata()
        stream.update(annotations=annotations)
        stream._btrdb.ep.setStreamAnnotations.assert_called_once_with(
            uu=uu,
            expected=42,
            changes={
                'acronym': 'VPHM',
                'description': 'El Segundo PMU 42 Ean',
                'devacronym': 'PMU!EL_SEG_PMU_42',
                'enabled': 'true',
                'id': '76932ae4-09bc-472c-8dc6-64fea68d2797',
                'phase': 'A',
                'label': 'null',
                'frequency': '30',
                'control': '2019-11-07 13:21:23.000000-0500',
                "calibrate": '{"racf": 1.8, "pacf": 0.005}',
            },
            removals=[],
        )
        stream._btrdb.ep.setStreamTags.assert_not_called()


    def test_update_annotations_nested_conversions(self):
        """
        Assert update correctly encodes nested annotation data
        """
        annotations = {
            "num": 10,
            "float": 1.3,
            "string": "the quick brown fox is 10",
            "nested": {
                "num": 11,
                "float": 1.3,
                "string": "the quick brown fox is 11",
                "nested": {
                    "num": 12,
                    "float": 1.3,
                    "string": "the quick brown fox is 12",
                }
            }
        }
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, {"foo": "42 Cherry Hill"}, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        stream.refresh_metadata()
        stream.update(annotations=annotations)

        # spot check a nested value for Python 3.4 and 3.5 compatability
        changes = stream._btrdb.ep.setStreamAnnotations.call_args[1]['changes']
        assert changes['nested'].__class__ == str
        assert json.loads(changes['nested']) == annotations['nested']

        # check all args if Python > 3.5
        if sys.version_info[0] > 3.5:
            stream._btrdb.ep.setStreamAnnotations.assert_called_once_with(
                uu=uu,
                expected=42,
                changes={
                    'num': '10',
                    'float': '1.3',
                    'string': '"the quick brown fox is 10"',
                    'nested': '{"num": 11, "float": 1.3, "string": "the quick brown fox is 11", "nested": {"num": 12, "float": 1.3, "string": "the quick brown fox is 12"}}'
                }
            )

    def test_update_annotations_no_encoder(self):
        """
        Assert update annotations works with None as encoder argument
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, {}, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        annotations = {"foo": "this is a string", "bar": "3.14"}

        stream.refresh_metadata()
        stream.update(annotations=annotations, encoder=None)
        stream._btrdb.ep.setStreamAnnotations.assert_called_once_with(
            uu=uu,
            expected=42,
            changes=annotations,
            removals=[],
        )

        # TODO: mock json.dumps
        # assert mock_dumps.assert_not_called()

    def test_update_annotations_replace(self):
        """
        Assert that replace argument will add proper keys to removals array in
        endpoint call.
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.streamInfo = Mock(return_value=("koala", 42, {}, {"phase": "A", "source": "PJM"}, None))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        annotations = {"foo": "this is a string", "phase": "A", }

        stream.refresh_metadata()

        # remove one of the keys and add new ones
        stream.update(annotations=annotations, replace=True)
        stream._btrdb.ep.setStreamAnnotations.assert_called_once_with(
            uu=uu,
            expected=42,
            changes=annotations,
            removals=["source"],
        )

        # clear annotations
        stream.update(annotations={}, replace=True)
        stream._btrdb.ep.setStreamAnnotations.assert_called_with(
            uu=uu,
            expected=42,
            changes={},
            removals=["phase", "source"],
        )

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
        assert stream.refresh_metadata.call_count == 1


    def test_exists_returns_false_on_404(self):
        """
        Assert exists returns False on 404 error
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)
        stream.refresh_metadata = Mock(side_effect=BTrDBError(code=404, msg="hello", mash=""))

        assert stream.exists() == False
        assert stream.refresh_metadata.call_count == 1


    def test_exists_passes_other_errors(self):
        """
        Assert exists does not keep non 404 errors trapped
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)
        stream.refresh_metadata = Mock(side_effect=ValueError())

        with pytest.raises(ValueError):
            stream.exists()
        assert stream.refresh_metadata.call_count == 1


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
        assert stream.refresh_metadata.call_count == 1


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
        assert stream.refresh_metadata.call_count == 1


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
            (StatPoint(time=1,minv=2.0,meanv=3.0,maxv=4.0,count=5,stddev=6.0), 42), (StatPoint(time=2,minv=3.0,meanv=4.0,maxv=5.0,count=6,stddev=7.0), 42),
            (StatPoint(time=3,minv=4.0,meanv=5.0,maxv=6.0,count=7,stddev=8.0), 42), (StatPoint(time=4,minv=5.0,meanv=6.0,maxv=7.0,count=8,stddev=9.0), 42),
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
            (StatPoint(time=1,minv=2.0,meanv=3.0,maxv=4.0,count=5,stddev=6.0), 42), (StatPoint(time=2,minv=3.0,meanv=4.0,maxv=5.0,count=6,stddev=7.0), 42),
            (StatPoint(time=3,minv=4.0,meanv=5.0,maxv=6.0,count=7,stddev=8.0), 42), (StatPoint(time=4,minv=5.0,meanv=6.0,maxv=7.0,count=8,stddev=9.0), 42),
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


    def test_count(self):
        """
        Test that stream count method uses aligned windows
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        windows = [
            [(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6), StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7)), 42],
            [(StatPointProto(time=3,min=4,mean=5,max=6,count=7,stddev=8), StatPointProto(time=4,min=5,mean=6,max=7,count=8,stddev=9)), 42],
        ]
        endpoint.alignedWindows = Mock(return_value=windows)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        assert stream.count(precise=False, version=0) == 26
        stream._btrdb.ep.alignedWindows.assert_called_once_with(
            uu, MINIMUM_TIME, MAXIMUM_TIME, 60, 0
        )

        stream.count(10, 1000, 8, version=1200)
        stream._btrdb.ep.alignedWindows.assert_called_with(uu, 10, 1000, 8, 1200)


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
        endpoint.nearest.assert_called_once_with(uu, MINIMUM_TIME, 0, False)


    def test_earliest_swallows_exception(self):
        """
        Assert earliest returns None when endpoint throws exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(401,"empty",None))

        assert stream.earliest() is None
        endpoint.nearest.assert_called_once_with(uu, MINIMUM_TIME, 0, False)


    def test_earliest_passes_exception(self):
        """
        Assert earliest reraises non 401 exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(999,"empty",None))

        with pytest.raises(BTrDBError) as exc:
            stream.earliest()
        assert exc.value.code == 999
        endpoint.nearest.assert_called_once_with(uu, MINIMUM_TIME, 0, False)


    def test_latest(self):
        """
        Assert latest calls Endpoint.nearest
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(return_value=(RawPointProto(time=100, value=1.0), 42))
        point, ver = stream.latest()

        assert (point, ver) == (RawPoint(100, 1.0), 42)
        endpoint.nearest.assert_called_once_with(uu, MAXIMUM_TIME, 0, True)


    def test_latest_swallows_exception(self):
        """
        Assert latest returns None when endpoint throws exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(401,"empty",None))

        assert stream.latest() is None
        endpoint.nearest.assert_called_once_with(uu, MAXIMUM_TIME, 0, True)


    def test_latest_passes_exception(self):
        """
        Assert latest reraises non 401 exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(999,"empty",None))

        with pytest.raises(BTrDBError) as exc:
            stream.latest()
        assert exc.value.code == 999
        endpoint.nearest.assert_called_once_with(uu, MAXIMUM_TIME, 0, True)


    @patch("btrdb.stream.currently_as_ns")
    def test_currently(self, mocked):
        """
        Assert currently calls Endpoint.nearest
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(return_value=(RawPointProto(time=100, value=1.0), 42))
        ns_fake_time = 1514808000000000000
        mocked.return_value = ns_fake_time
        point, ver = stream.current()

        assert (point, ver) == (RawPoint(100, 1.0), 42)
        endpoint.nearest.assert_called_once_with(uu, ns_fake_time, 0, True)


    @patch("btrdb.stream.currently_as_ns")
    def test_currently_swallows_exception(self, mocked):
        """
        Assert currently returns None when endpoint throws exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(401,"empty",None))
        ns_fake_time = 1514808000000000000
        mocked.return_value = ns_fake_time

        assert stream.current() is None
        endpoint.nearest.assert_called_once_with(uu, ns_fake_time, 0, True)


    @patch("btrdb.stream.currently_as_ns")
    def test_currently_passes_exception(self, mocked):
        """
        Assert currently reraises non 401 exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(999,"empty",None))
        ns_fake_time = 1514808000000000000
        mocked.return_value = ns_fake_time

        with pytest.raises(BTrDBError) as exc:
            stream.current()
        assert exc.value.code == 999
        endpoint.nearest.assert_called_once_with(uu, ns_fake_time, 0, True)


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
        endpoint.insert = Mock(side_effect=[1,2,3])
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        data = list(zip(range(10000,120000), map(float, range(110000))))
        version = stream.insert(data)

        assert stream._btrdb.ep.insert.call_args_list[0][0][1] == data[:INSERT_BATCH_SIZE]
        assert stream._btrdb.ep.insert.call_args_list[1][0][1] == data[INSERT_BATCH_SIZE:2*INSERT_BATCH_SIZE]
        assert stream._btrdb.ep.insert.call_args_list[2][0][1] == data[2*INSERT_BATCH_SIZE:]
        assert version == 3


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


    def test_nearest_swallows_exception(self):
        """
        Assert nearest returns None when endpoint throws 401 exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(401,"empty",None))

        assert stream.nearest(0, 0, False) is None
        endpoint.nearest.assert_called_once_with(uu, 0, 0, False)


    def test_nearest_passes_exception(self):
        """
        Assert nearest reraises non 401 exception
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)
        endpoint.nearest = Mock(side_effect=BTrDBError(999,"empty",None))

        with pytest.raises(BTrDBError) as exc:
            stream.nearest(0, 0, False)
        assert exc.value.code == 999
        endpoint.nearest.assert_called_once_with(uu, 0, 0, False)


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


    def test_obliterate(self):
        """
        Assert obliterate calls Endpoint.obliterate with UUID
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        stream = Stream(btrdb=BTrDB(Mock(Endpoint)), uuid=uu)

        stream.obliterate()
        stream._btrdb.ep.obliterate.assert_called_once_with(uu)


    def test_obliterate_allows_error(self):
        """
        Assert obliterate raises error if stream not found. (does not swallow error)
        """
        uu = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        endpoint = Mock(Endpoint)
        endpoint.obliterate = Mock(side_effect=BTrDBError(code=404, msg="hello", mash=""))
        stream = Stream(btrdb=BTrDB(endpoint), uuid=uu)

        with pytest.raises(BTrDBError):
            stream.obliterate()
        stream._btrdb.ep.obliterate.assert_called_once_with(uu)




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
    ## builtin / magic methods tests
    ##########################################################################

    def test_repr(self):
        """
        Assert StreamSet instance repr output
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        expected = "<StreamSet(4 streams)>"
        assert streams.__repr__() == expected

        data = [1]
        streams = StreamSet(data)
        expected = "<StreamSet(1 stream)>"
        assert streams.__repr__() == expected


    def test_str(self):
        """
        Assert StreamSet instance str output
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        expected = "StreamSet with 4 streams"
        assert str(streams) == expected

        data = [1]
        streams = StreamSet(data)
        expected = "StreamSet with 1 stream"
        assert str(streams) == expected


    def test_subscriptable(self):
        """
        Assert StreamSet instance is subscriptable
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        for index, val in enumerate(data):
            assert streams[index] == val


    def test_len(self):
        """
        Assert StreamSet instance support len
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        assert len(streams) == len(data)


    def test_iter(self):
        """
        Assert StreamSet instance support iteration
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        for index, stream in enumerate(streams):
            assert data[index] == stream


    def test_indexing(self):
        """
        Assert StreamSet instance supports indexing
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)

        # verify index lookup
        assert streams[-1] == data[-1]
        for idx in range(len(streams)):
            assert data[idx] == streams[idx]

        # verify slicing works
        assert streams[:2] == data[:2]


    def test_mapping(self):
        """
        Assert StreamSet instance support key mapping
        """
        uuids = [uuid.uuid4() for _ in range(4)]
        data = [Stream(None, uu) for uu in uuids]
        streams = StreamSet(data)

        # verify lookup with UUID
        for uu in uuids:
            assert streams[uu].uuid == uu

        # verify lookup with str
        for uu in uuids:
            assert streams[str(uu)].uuid == uu

        # verify raises KeyError
        missing = uuid.uuid4()
        with pytest.raises(KeyError) as e:
            streams[missing]
        assert str(missing) in str(e)


    def test_contains(self):
        """
        Assert StreamSet instance supports contains
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        assert "dog" in streams


    def test_reverse(self):
        """
        Assert StreamSet instance supports reversal
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        assert list(reversed(streams)) == list(reversed(data))


    def test_to_list(self):
        """
        Assert StreamSet instance cast to list
        """
        data = [11,22,"dog","cat"]
        streams = StreamSet(data)
        assert list(streams) == data


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

    @patch("btrdb.stream.currently_as_ns")
    def test_current(self, mocked, stream1, stream2):
        """
        Assert current calls nearest with the current time
        """
        mocked.return_value=15
        streams = StreamSet([stream1, stream2])
        streams.current()
        stream1.nearest.assert_called_once_with(15, version=11, backward=True)
        stream2.nearest.assert_called_once_with(15, version=22, backward=True)

    @patch("btrdb.stream.currently_as_ns")
    def test_currently_out_of_range(self, mocked):
        """
        Assert currently raises an exception if it is not filtered
        """
        mocked.return_value=15
        streams = StreamSet([stream1, stream2])

        with pytest.raises(ValueError, match="current time is not included in filtered stream range"):
            streams.filter(start=20, end=30).current()

        with pytest.raises(ValueError, match="current time is not included in filtered stream range"):
            streams.filter(start=0, end=10).current()

    def test_count(self):
        """
        Test the stream set count method
        """
        uu1 = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        uu2 = uuid.UUID('4dadf38d-52a5-4b7a-ada9-a5d563f9538c')
        endpoint = Mock(Endpoint)
        windows = [
            [(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6), StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7)), 42],
            [(StatPointProto(time=3,min=4,mean=5,max=6,count=7,stddev=8), StatPointProto(time=4,min=5,mean=6,max=7,count=8,stddev=9)), 42],
        ]
        endpoint.alignedWindows = Mock(return_value=windows)
        streams = StreamSet([
            Stream(btrdb=BTrDB(endpoint), uuid=uu1),
            Stream(btrdb=BTrDB(endpoint), uuid=uu2),
        ])

        assert streams.count() == 52
        endpoint.alignedWindows.assert_any_call(uu1, MINIMUM_TIME, MAXIMUM_TIME, 60, 0)
        endpoint.alignedWindows.assert_any_call(uu2, MINIMUM_TIME, MAXIMUM_TIME, 60, 0)


    def test_count_filtered(self):
        """
        Test the stream set count method with filters
        """
        uu1 = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        uu2 = uuid.UUID('4dadf38d-52a5-4b7a-ada9-a5d563f9538c')
        endpoint = Mock(Endpoint)
        endpoint.alignedWindows = Mock(return_value=[])
        streams = StreamSet([
            Stream(btrdb=BTrDB(endpoint), uuid=uu1),
            Stream(btrdb=BTrDB(endpoint), uuid=uu2),
        ])
        windows = [
            [(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6), StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7)), 42],
            [(StatPointProto(time=3,min=4,mean=5,max=6,count=7,stddev=8), StatPointProto(time=4,min=5,mean=6,max=7,count=8,stddev=9)), 42],
        ]
        endpoint.alignedWindows = Mock(return_value=windows)

        streams = streams.filter(start=10, end=1000)
        streams.pin_versions({uu1: 42, uu2: 99})

        streams.count()
        endpoint.alignedWindows.assert_any_call(uu1, 10, 1000, 8, 42)
        endpoint.alignedWindows.assert_any_call(uu2, 10, 1000, 8, 99)



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


    def test_filter_returns_new_instance(self, stream1):
        """
        Assert filter returns new instance
        """
        streams = StreamSet([stream1])
        start, end = 1, 100
        other = streams.filter(start=start, end=end)

        assert other is not streams
        assert isinstance(other, streams.__class__)


    def test_filter_collection(self, stream1, stream2):
        """
        Assert filter collection works as intended
        """
        streams = StreamSet([stream1, stream2])

        # string arguments
        other = streams.filter(collection="fruits")
        assert other._streams == []
        other = streams.filter(collection="fruits/apple")
        assert other._streams == [stream1]
        other = streams.filter(collection="FRUITS/APPLE")
        assert other._streams == [stream1]
        other = streams.filter(collection="fruits*")
        assert other._streams == []

        # regex arguments
        other = streams.filter(collection=re.compile("fruits"))
        assert other._streams == [stream1, stream2]
        other = streams.filter(collection=re.compile("fruits.*"))
        assert other._streams == [stream1, stream2]

        type(stream1).collection = PropertyMock(return_value="foo/region-north")
        other = streams.filter(collection=re.compile("region-"))
        assert other._streams == [stream1]
        other = streams.filter(collection=re.compile("^region-"))
        assert other._streams == []
        other = streams.filter(collection=re.compile("foo/"))
        assert other._streams == [stream1]
        other = streams.filter(collection=re.compile("foo/z"))
        assert other._streams == []

        type(stream1).collection = PropertyMock(return_value="region.north/foo")
        other = streams.filter(collection=re.compile(r"region\."))
        assert other._streams == [stream1]


    def test_filter_name(self, stream1, stream2):
        """
        Assert filter name works as intended
        """
        streams = StreamSet([stream1, stream2])

        # string arguments
        other = streams.filter(name="blood")
        assert other._streams == [stream2]
        other = streams.filter(name="BLOOD")
        assert other._streams == [stream2]
        other = streams.filter(name="not_found")
        assert other._streams == []

        # regex arguments
        other = streams.filter(name=re.compile("blood"))
        assert other._streams == [stream2]
        other = streams.filter(name=re.compile("^blood$"))
        assert other._streams == [stream2]
        other = streams.filter(name=re.compile("oo"))
        assert other._streams == [stream2]
        other = streams.filter(name=re.compile("not_found"))
        assert other._streams == []

        type(stream1).name = PropertyMock(return_value="region-north")
        other = streams.filter(name=re.compile("region-"))
        assert other._streams == [stream1]
        other = streams.filter(name=re.compile(r"region\."))
        assert other._streams == []

        type(stream1).name = PropertyMock(return_value="region.north")
        other = streams.filter(name=re.compile(r"region\."))
        assert other._streams == [stream1]


    def test_filter_unit(self, stream1, stream2):
        """
        Assert filter unit works as intended
        """
        streams = StreamSet([stream1, stream2])

        # string arguments
        other = streams.filter(unit="volts")
        assert other._streams == [stream1]
        other = streams.filter(unit="VOLTS")
        assert other._streams == [stream1]
        other = streams.filter(unit="not_found")
        assert other._streams == []

        # regex arguments
        other = streams.filter(unit=re.compile("volts|amps"))
        assert other._streams == [stream1, stream2]
        other = streams.filter(unit=re.compile("volts"))
        assert other._streams == [stream1]
        other = streams.filter(unit=re.compile("v"))
        assert other._streams == [stream1]
        other = streams.filter(unit=re.compile("meters"))
        assert other._streams == []


    def test_filter_tags(self, stream1, stream2):
        """
        Assert filter annotations works as intended
        """
        streams = StreamSet([stream1, stream2])

        other = streams.filter(tags={"unit": "meters"})
        assert other._streams == []

        other = streams.filter(tags={"unit": "volts"})
        assert other._streams == [stream1]

        stream2.tags.return_value = {"name": "blood", "unit": "volts"}
        other = streams.filter(tags={"name": "blood", "unit": "volts"})
        assert other._streams == [stream2]
        other = streams.filter(tags={"unit": "volts"})
        assert other._streams == [stream1, stream2]


    def test_filter_annotations(self, stream1, stream2):
        """
        Assert filter annotations works as intended
        """
        streams = StreamSet([stream1, stream2])

        other = streams.filter(annotations={"owner": ""})
        assert other._streams == []

        other = streams.filter(annotations={"owner": "ABC"})
        assert other._streams == [stream1, stream2]

        other = streams.filter(annotations={"color": "red"})
        assert other._streams == [stream1]

        other = streams.filter(annotations={"owner": "ABC", "color": "red"})
        assert other._streams == [stream1]


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


    def test_windows_stores_values(self, stream1):
        """
        Assert windows stores values
        """
        streams = StreamSet([stream1])
        result = streams.windows(10, 20)

        # assert stores values
        assert streams.width == 10
        assert streams.depth == 20


    def test_windows_values_and_calls_to_endpoint(self):
        """
        assert windows result and endpoint calls are correct
        """
        endpoint = Mock(Endpoint)
        window1 = [[(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6),), 11]]
        window2 = [[(StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7),), 12]]
        endpoint.windows = Mock(side_effect=[ window1, window2 ])

        uu1 = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        uu2 = uuid.UUID('5d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        s1 = Stream(btrdb=BTrDB(endpoint), uuid=uu1)
        s2 = Stream(btrdb=BTrDB(endpoint), uuid=uu2)
        versions = {uu1: 11, uu2: 12}

        start, end, width, depth = 1, 100, 1000, 25
        streams = StreamSet([s1, s2])
        streams.pin_versions(versions)
        values = streams.filter(start=start, end=end).windows(width=width, depth=depth).values()

        # assert endpoint calls have correct arguments, version
        expected = [
            call(uu1, start, end, width, depth, versions[uu1]),
            call(uu2, start, end, width, depth, versions[uu2])
        ]
        assert endpoint.windows.call_args_list == expected

        # assert expected output
        expected = [
            [StatPoint(1, 2.0, 3.0, 4.0, 5, 6.0)],
            [StatPoint(2, 3.0, 4.0, 5.0, 6, 7.0)],
        ]
        assert values == expected


    def test_windows_rows_and_calls_to_endpoint(self):
        """
        assert windows rows result and endpoint calls are correct
        """
        endpoint = Mock(Endpoint)
        window1 = [[(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6),), 11]]
        window2 = [[(StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7),), 12]]
        endpoint.windows = Mock(side_effect=[ window1, window2 ])

        uu1 = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        uu2 = uuid.UUID('5d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        s1 = Stream(btrdb=BTrDB(endpoint), uuid=uu1)
        s2 = Stream(btrdb=BTrDB(endpoint), uuid=uu2)
        versions = {uu1: 11, uu2: 12}

        start, end, width, depth = 1, 100, 1000, 25
        streams = StreamSet([s1, s2])
        streams.pin_versions(versions)
        rows = streams.filter(start=start, end=end).windows(width=width, depth=depth).rows()

        # assert endpoint calls have correct arguments, version
        expected = [
            call(uu1, start, end, width, depth, versions[uu1]),
            call(uu2, start, end, width, depth, versions[uu2])
        ]
        assert endpoint.windows.call_args_list == expected

        # assert expected output
        expected = [
            (StatPoint(1, 2.0, 3.0, 4.0, 5, 6.0), None),
            (None, StatPoint(2, 3.0, 4.0, 5.0, 6, 7.0)),
        ]
        assert rows == expected


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


    def test_aligned_windows_values_and_calls_to_endpoint(self):
        """
        assert aligned_windows result and endpoint calls are correct
        """
        endpoint = Mock(Endpoint)
        window1 = [[(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6),), 11]]
        window2 = [[(StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7),), 12]]
        endpoint.alignedWindows = Mock(side_effect=[ window1, window2 ])

        uu1 = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        uu2 = uuid.UUID('5d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        s1 = Stream(btrdb=BTrDB(endpoint), uuid=uu1)
        s2 = Stream(btrdb=BTrDB(endpoint), uuid=uu2)
        versions = {uu1: 11, uu2: 12}

        start, end, pointwidth = 1, 100, 25
        streams = StreamSet([s1, s2])
        streams.pin_versions(versions)
        values = streams.filter(start=start, end=end).aligned_windows(pointwidth=pointwidth).values()

        # assert endpoint calls have correct arguments, version
        expected = [
            call(uu1, start, end, pointwidth, versions[uu1]),
            call(uu2, start, end, pointwidth, versions[uu2])
        ]
        assert endpoint.alignedWindows.call_args_list == expected

        # assert expected output
        expected = [
            [StatPoint(1, 2.0, 3.0, 4.0, 5, 6.0)],
            [StatPoint(2, 3.0, 4.0, 5.0, 6, 7.0)],
        ]
        assert values == expected


    def test_aligned_windows_rows_and_calls_to_endpoint(self):
        """
        assert aligned_windows rows result and endpoint calls are correct
        """
        endpoint = Mock(Endpoint)
        window1 = [[(StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6),), 11]]
        window2 = [[(StatPointProto(time=2,min=3,mean=4,max=5,count=6,stddev=7),), 12]]
        endpoint.alignedWindows = Mock(side_effect=[ window1, window2 ])

        uu1 = uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        uu2 = uuid.UUID('5d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        s1 = Stream(btrdb=BTrDB(endpoint), uuid=uu1)
        s2 = Stream(btrdb=BTrDB(endpoint), uuid=uu2)
        versions = {uu1: 11, uu2: 12}

        start, end, pointwidth = 1, 100, 25
        streams = StreamSet([s1, s2])
        streams.pin_versions(versions)
        rows = streams.filter(start=start, end=end).aligned_windows(pointwidth=pointwidth).rows()

        # assert endpoint calls have correct arguments, version
        expected = [
            call(uu1, start, end, pointwidth, versions[uu1]),
            call(uu2, start, end, pointwidth, versions[uu2])
        ]
        assert endpoint.alignedWindows.call_args_list == expected

        # assert expected output
        expected = [
            (StatPoint(1, 2.0, 3.0, 4.0, 5, 6.0), None),
            (None, StatPoint(2, 3.0, 4.0, 5.0, 6, 7.0)),
        ]
        assert rows == expected



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
        rows = iter(streams.rows())

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
