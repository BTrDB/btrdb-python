# tests.utils.test_conversions
# Testing for btrdb convertion utilities
#
# Author:   PingThings
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# For license information, see LICENSE.txt
# ID: test_conversions.py [] allen@pingthings.io $

"""
Testing for btrdb convertion utilities
"""

##########################################################################
## Imports
##########################################################################

import uuid
import pytest

from btrdb.utils.conversion import to_uuid

##########################################################################
## Test Constants
##########################################################################

EXAMPLE_UUID_STR = "07d28a44-4991-492d-b9c5-2d8cec5aa6d4"
EXAMPLE_UUID_BYTES = EXAMPLE_UUID_STR.encode("ASCII")
EXAMPLE_UUID = uuid.UUID(EXAMPLE_UUID_STR)

##########################################################################
## Initialization Tests
##########################################################################

class TestToUUID(object):

    def test_from_bytes(self):
        """
        Assert that `to_uuid` converts from bytes
        """
        assert to_uuid(EXAMPLE_UUID_BYTES) == EXAMPLE_UUID

    def test_from_str(self):
        """
        Assert that `to_uuid` converts from str
        """
        assert to_uuid(EXAMPLE_UUID_STR) == EXAMPLE_UUID

    def test_from_uuid(self):
        """
        Assert that `to_uuid` returns passed UUID
        """
        assert to_uuid(EXAMPLE_UUID) == EXAMPLE_UUID

    def test_raises_on_bad_data(self):
        """
        Assert that `to_uuid` raises error with bad UUID string
        """
        with pytest.raises(ValueError):
            to_uuid("bad data!!!")

    def test_raises_on_incorrect_input_type(self):
        """
        Assert that `to_uuid` raises error on wrong input class
        """
        with pytest.raises(TypeError) as exc:
            to_uuid(3.0)

        assert "Cannot convert object to UUID" in str(exc)
        assert "float" in str(exc)
