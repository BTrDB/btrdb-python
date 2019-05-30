# tests.test_base
# Testing package for the btrdb library.
#
# Author:   PingThings
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# For license information, see LICENSE.txt
# ID: test_base.py [] allen@pingthings.io $

"""
Testing package for the btrdb database library.
"""

##########################################################################
## Imports
##########################################################################

import os
import pytest
from unittest.mock import patch

from btrdb import connect, __version__, BTRDB_ENDPOINTS, BTRDB_API_KEY
from btrdb.exceptions import ConnectionError

##########################################################################
## Test Constants
##########################################################################

EXPECTED_VERSION = "5.4"


##########################################################################
## Initialization Tests
##########################################################################

class TestPackage(object):

    def test_version(self):
        """
        Assert that the test version matches the library version.
        """
        assert __version__ == EXPECTED_VERSION

class TestConnect(object):

    def setup(self):
        for env in ["BTRDB_ENDPOINTS", "BTRDB_PROFILE"]:
            try:
                del os.environ[env]
            except KeyError:
                pass

    @patch('btrdb.utils.credentials.load_credentials')
    def test_raises_err_with_no_inputs(self, mock_load_credentials):
        """
        Assert ConnectionError raised if no connection arg, ENV, profile
        """
        mock_load_credentials.return_value = {}
        msg = "Could not determine credentials to use."
        with pytest.raises(ConnectionError, match=msg):
            connect()

    def test_raises_err_if_both_profile_and_credentials(self):
        """
        Assert error is raised if both profile and credentials are sent
        """
        with pytest.raises(ValueError):
            connect("192.168.1.100:4410",None,"default")

    def test_raises_err_if_both_profile_and_credentials_in_env(self):
        """
        Assert error is raised if both profile and credentials in ENV
        """
        os.environ["BTRDB_ENDPOINTS"] = "192.168.1.100:4410"
        os.environ["BTRDB_PROFILE"] = "husky"
        msg = "Found both BTRDB_PROFILE and BTRDB_ENDPOINTS in ENV"

        with pytest.raises(ValueError, match=msg):
            connect()

    @patch('btrdb.utils.credentials.load_credentials')
    @patch('btrdb._connect')
    def test_uses_args_over_env(self, mock_connect, mock_load_credentials):
        """
        Assert uses profile arg over profile env
        """
        os.environ["BTRDB_PROFILE"] = "dog"
        mock_load_credentials.return_value = {
            "cat": {"name": "cat", "btrdb": {"endpoints": "a", "api_key": "b"}},
            "dog": {"name": "dog", "btrdb": {"endpoints": "c", "api_key": "d"}},
        }
        connect(profile="cat")
        mock_connect.assert_called_once_with("a", "b")

    @patch('btrdb.Connection')
    def test_connect_with_env(self, mock_conn):
        """
        Assert connect uses ENV variables
        """
        address = "127.0.0.1:4410"
        apikey = "abcd"
        os.environ[BTRDB_ENDPOINTS] = address

        btrdb = connect()
        mock_conn.assert_called_once_with(address, apikey=None)
        mock_conn.reset_mock()

        os.environ[BTRDB_API_KEY] = apikey
        btrdb = connect()
        mock_conn.assert_called_once_with(address, apikey=apikey)