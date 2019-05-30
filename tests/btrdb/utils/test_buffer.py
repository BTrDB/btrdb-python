# tests.test_collection
# Testing for the btrdb.collection module
#
# Author:   PingThings
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# For license information, see LICENSE.txt
# ID: test_collection.py [] allen@pingthings.io $

"""
Testing for the btrdb.collection module
"""

##########################################################################
## Imports
##########################################################################

import pytest
from btrdb.utils.buffer import PointBuffer
from btrdb.point import RawPoint

##########################################################################
## Test Constants
##########################################################################



##########################################################################
## Initialization Tests
##########################################################################

class TestPointBuffer(object):

    def test_earliest(self):
        """
        Assert earliest returns correct key
        """
        buffer = PointBuffer(3)
        buffer[2000][0] = "horse"
        buffer[1000][0] = "zebra"
        buffer[1000][1] = "leopard"
        buffer[1000][2] = "giraffe"
        buffer[3000][0] = "pig"

        assert buffer.earliest() == 1000

    def test_earliest_if_empty(self):
        """
        Assert earliest return None
        """
        buffer = PointBuffer(3)
        assert buffer.earliest() == None

    def test_next_key_ready(self):
        """
        Assert next_key_ready returns correct key
        """
        buffer = PointBuffer(3)
        buffer.add_point(0, RawPoint(time=1000, value="zebra"))
        buffer.add_point(1, RawPoint(time=1000, value="leopard"))
        buffer.add_point(2, RawPoint(time=1000, value="giraffe"))
        buffer.add_point(0, RawPoint(time=2000, value="horse"))
        buffer.add_point(0, RawPoint(time=3000, value="pig"))
        assert buffer.next_key_ready() == 1000

        buffer = PointBuffer(2)
        buffer.add_point(0, RawPoint(time=1000, value="horse"))
        buffer.add_point(0, RawPoint(time=2000, value="zebra"))
        buffer.add_point(0, RawPoint(time=3000, value="pig"))
        buffer.add_point(1, RawPoint(time=2000, value="leopard"))
        assert buffer.next_key_ready() == 1000

    def test_next_key_ready_if_none_are(self):
        """
        Assert next_key_ready returns None if nothing is ready
        """
        buffer = PointBuffer(3)
        assert buffer.next_key_ready() == None

        buffer.add_point(0, RawPoint(time=1000, value="leopard"))
        buffer.add_point(2, RawPoint(time=1000, value="giraffe"))
        buffer.add_point(0, RawPoint(time=2000, value="horse"))
        buffer.add_point(0, RawPoint(time=3000, value="pig"))
        assert buffer.next_key_ready() == None

    def test_deactivate(self):
        """
        Assert deactivate modifies active list correctly
        """
        buffer = PointBuffer(3)
        buffer.deactivate(1)
        assert buffer.active == [True, False, True]

    def test_is_ready(self):
        """
        Assert is_ready returns correct value
        """
        buffer = PointBuffer(2)
        buffer.add_point(0, RawPoint(time=1000, value="zebra"))
        buffer.add_point(1, RawPoint(time=1000, value="giraffe"))
        buffer.add_point(0, RawPoint(time=2000, value="horse"))

        assert buffer.is_ready(1000) == True
        assert buffer.is_ready(2000) == False

        buffer.deactivate(1)
        assert buffer.is_ready(2000) == True


    def test_next_key_ready_with_inactive(self):
        """
        Assert next_key_ready returns correct key with inactive stream
        """
        buffer = PointBuffer(3)
        buffer.deactivate(1)
        buffer.add_point(0, RawPoint(time=1000, value="leopard"))
        buffer.add_point(2, RawPoint(time=1000, value="leopard"))
        buffer.add_point(0, RawPoint(time=2000, value="leopard"))
        assert buffer.next_key_ready() == 1000


        # assert first key is 500 even though it was exhausted
        buffer = PointBuffer(3)
        buffer.add_point(1, RawPoint(time=500, value="leopard"))
        buffer.deactivate(1)
        buffer.add_point(0, RawPoint(time=1000, value="leopard"))
        buffer.add_point(2, RawPoint(time=1000, value="leopard"))
        assert buffer.next_key_ready() == 500
