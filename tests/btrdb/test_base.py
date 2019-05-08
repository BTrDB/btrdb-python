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

    def test_sanity(self):
        """
        Test that tests work by confirming 7-3 = 4
        """
        assert 7-3 == 4, "The world went wrong!!"

    def test_version(self):
        """
        Assert that the test version matches the library version.
        """
        assert __version__ == EXPECTED_VERSION

    def test_connect_raises_err(self):
        """
        Assert ConnectionError raised if no connection arg or ENV
        """
        with pytest.raises(ConnectionError):
            connect()

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
