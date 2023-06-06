import concurrent
import uuid
from unittest.mock import Mock, PropertyMock, patch

import pytest

import btrdb
from btrdb.point import RawPoint
from btrdb.stream import Stream


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
    stream._btrdb = Mock()
    stream._btrdb._executor = concurrent.futures.ThreadPoolExecutor()
    stream._btrdb._ARROW_ENABLED = Mock(return_value=True)
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
    stream._btrdb = Mock()
    stream._btrdb._executor = Mock()
    stream._btrdb._ARROW_ENABLED = Mock(return_value=True)
    return stream

class TestArrowStreams(object):
    pass