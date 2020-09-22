# tests.test_conn
# Testing package for the btrdb connection module
#
# Author:   PingThings
# Created:  Wed Jan 02 19:26:20 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_conn.py [] allen@pingthings.io $

"""
Testing package for the btrdb connection module
"""

##########################################################################
## Imports
##########################################################################

import uuid as uuidlib
import pytest
from unittest.mock import Mock, PropertyMock, patch, call

from btrdb.conn import Connection, BTrDB
from btrdb.endpoint import Endpoint
from btrdb.grpcinterface import btrdb_pb2
from btrdb.exceptions import *

##########################################################################
## Connection Tests
##########################################################################

class TestConnection(object):

    def test_raises_err_invalid_address(self):
        """
        Assert ValueError is raised if address:port is invalidly formatted
        """
        address = "127.0.0.1::4410"
        with pytest.raises(ValueError) as exc:
            conn = Connection(address)
        assert "expecting address:port" in str(exc)


    def test_raises_err_for_apikey_insecure_port(self):
        """
        Assert error is raised if apikey used on insecure port
        """
        address = "127.0.0.1:4410"
        with pytest.raises(ValueError) as exc:
            conn = Connection(address, apikey="abcd")
        assert "cannot use an API key with an insecure" in str(exc)


##########################################################################
## BTrDB Tests
##########################################################################

class TestBTrDB(object):

    ##########################################################################
    ## .streams tests
    ##########################################################################

    def test_streams_raises_err_if_version_not_list(self):
        """
        Assert streams raises TypeError if versions is not list
        """
        db = BTrDB(None)
        with pytest.raises(TypeError) as exc:
            db.streams('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a', versions="2,2")

        assert "versions argument must be of type list" in str(exc)


    def test_streams_raises_err_if_version_argument_mismatch(self):
        """
        Assert streams raises ValueError if len(identifiers) doesnt match length of versions
        """
        db = BTrDB(None)
        with pytest.raises(ValueError) as exc:
            db.streams('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a', versions=[2,2])

        assert "versions does not match identifiers" in str(exc)


    def test_streams_stores_versions(self):
        """
        Assert streams correctly stores supplied version info
        """
        db = BTrDB(None)
        uuid1 = uuidlib.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        uuid2 = uuidlib.UUID('17dbe387-89ea-42b6-864b-f505cdb483f5')
        versions = [22,44]
        expected = dict(zip([uuid1, uuid2], versions))

        streams = db.streams(uuid1, uuid2, versions=versions)
        assert streams._pinned_versions == expected


    @patch('btrdb.conn.BTrDB.stream_from_uuid')
    def test_streams_recognizes_uuid(self, mock_func):
        """
        Assert streams recognizes uuid strings
        """
        db = BTrDB(None)
        uuid1 = uuidlib.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')
        mock_func.return_value = [1]
        db.streams(uuid1)

        mock_func.assert_called_once()
        assert mock_func.call_args[0][0] == uuid1


    @patch('btrdb.conn.BTrDB.stream_from_uuid')
    def test_streams_recognizes_uuid_string(self, mock_func):
        """
        Assert streams recognizes uuid strings
        """
        db = BTrDB(None)
        uuid1 = '0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a'
        mock_func.return_value = [1]
        db.streams(uuid1)

        mock_func.assert_called_once()
        assert mock_func.call_args[0][0] == uuid1


    @patch('btrdb.conn.BTrDB.streams_in_collection')
    def test_streams_handles_path(self, mock_func):
        """
        Assert streams calls streams_in_collection for collection/name paths
        """
        db = BTrDB(None)
        ident = "zoo/animal/dog"
        mock_func.return_value = [1]
        db.streams(ident, '0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a')

        mock_func.assert_called_once()
        assert mock_func.call_args[0][0] == 'zoo/animal'
        assert mock_func.call_args[1] == {
            'is_collection_prefix': False,
            'tags': {'name': 'dog'}
        }


    @patch('btrdb.conn.BTrDB.streams_in_collection')
    def test_streams_raises_err(self, mock_func):
        """
        Assert streams raises NotFound
        """
        db = BTrDB(None)
        ident = "zoo/animal/dog"

        mock_func.return_value = []
        with pytest.raises(NotFound) as exc:
            db.streams(ident)

        mock_func.return_value = [1,2]
        with pytest.raises(NotFound) as exc:
            db.streams(ident)

        # check that does not raise if one returned
        mock_func.return_value = [1]
        db.streams(ident)


    def test_streams_raises_valueerror(self):
        """
        Assert streams raises ValueError if not uuid, uuid str, or path
        """
        db = BTrDB(None)
        with pytest.raises(ValueError) as exc:
            db.streams(11)


    ##########################################################################
    ## other tests
    ##########################################################################

    def test_info(self):
        """
        Assert info method returns a dict
        """
        serialized = b'\x18\x05*\x055.0.02\x10\n\x0elocalhost:4410'
        info = btrdb_pb2.InfoResponse.FromString(serialized)

        endpoint = Mock(Endpoint)
        endpoint.info = Mock(return_value=info)
        conn = BTrDB(endpoint)

        truth = {
            "majorVersion": 5,
            "build": "5.0.0",
            "proxy": { "proxyEndpoints": ["localhost:4410"], },
        }
        info = conn.info()
        assert info == truth

        # verify RepeatedScalarContainer is converted to list
        assert info["proxy"]["proxyEndpoints"].__class__ == list

    def test_list_collections(self):
        """
        Assert list_collections method works
        """
        endpoint = Mock(Endpoint)
        endpoint.listCollections = Mock(side_effect=[iter([
            ['allen/automated'],
            ['allen/bindings']
        ])])
        conn = BTrDB(endpoint)

        truth = ['allen/automated', 'allen/bindings']
        assert conn.list_collections() == truth

    @patch('btrdb.conn.unpack_stream_descriptor')
    def test_streams_in_collections_args(self, mock_util):
        """
        Assert streams_in_collections correctly sends *collection, tags, annotations
        to the endpoint method
        """
        descriptor = Mock()
        type(descriptor).uuid = PropertyMock(return_value=uuidlib.uuid4().bytes)
        type(descriptor).collection = PropertyMock(return_value="fruit/apple")
        type(descriptor).propertyVersion = PropertyMock(return_value=22)
        mock_util.side_effect = [({"name": "gala"}, {}), ({"name": "fuji"}, {})]

        endpoint = Mock(Endpoint)
        endpoint.lookupStreams = Mock(side_effect=[[[descriptor]], [[descriptor]]])

        conn = BTrDB(endpoint)
        tags = {"unit": "volts"}
        annotations = {"size": "large"}
        streams = conn.streams_in_collection("a", "b", is_collection_prefix=False,
            tags=tags, annotations=annotations)

        assert streams[0].name == "gala"
        assert streams[1].name == "fuji"

        expected = [call('a', False, tags, annotations), call('b', False, tags, annotations)]
        assert endpoint.lookupStreams.call_args_list == expected

    def test_streams_in_collections_no_arg(self):
        """
        Assert streams_in_collections calls lookupStreams if collections not sent
        """
        endpoint = Mock(Endpoint)
        endpoint.lookupStreams.return_value = []

        conn = BTrDB(endpoint)
        annotations = {"size": "large"}
        streams = conn.streams_in_collection(annotations=annotations)
        assert endpoint.lookupStreams.called
