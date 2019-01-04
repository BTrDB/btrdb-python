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

import pytest
from unittest.mock import Mock, PropertyMock

from btrdb.conn import Connection, BTrDB


##########################################################################
## Tests
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


class TestBTrDB(object):

    def test_streams_raises_err_if_version_argument_mismatch(self):
        """
        Assert streams raises ValueError if len(identifiers) doesnt match length of versions
        """
        db = BTrDB(None)
        with pytest.raises(ValueError) as exc:
            db.streams('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a', versions=[2,2])

        assert "versions does not match identifiers" in str(exc)
