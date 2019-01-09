# tests.test_point
# Testing package for the btrdb point module
#
# Author:   PingThings
# Created:  Wed Jan 02 19:26:20 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_point.py [] allen@pingthings.io $

"""
Testing package for the btrdb point module
"""

##########################################################################
## Imports
##########################################################################

import pytest

from btrdb.point import RawPoint, StatPoint
from btrdb.grpcinterface import btrdb_pb2


##########################################################################
## ProtoBuff Classes
##########################################################################

RawPointProto =  btrdb_pb2.RawPoint
StatPointProto =  btrdb_pb2.StatPoint

##########################################################################
## RawPoint Tests
##########################################################################

class TestRawPoint(object):

    def test_create(self):
        """
        Ensure we can create the object
        """
        point = RawPoint(1, 2)
        assert point.time == 1
        assert point.value == 2

    def test_from_proto(self):
        """
        Assert from_proto creates a correct object
        """
        proto = RawPointProto(time=1, value=10)
        point = RawPoint.from_proto(proto)
        assert point.time == proto.time
        assert point.value == proto.value

    def test_from_proto_creates_rawpoint(self):
        """
        Assert from_proto creates a new instance of RawPoint
        """
        proto = RawPointProto(time=1, value=10)
        point = RawPoint.from_proto(proto)
        assert isinstance(point, RawPoint)

    def test_from_proto_list(self):
        """
        Assert from_proto_list creates valid objects
        """
        protos = [RawPointProto(time=1, value=10), RawPointProto(time=2, value=20)]
        points = RawPoint.from_proto_list(protos)
        assert points[0].time == protos[0].time
        assert points[0].value == protos[0].value
        assert points[1].time == protos[1].time
        assert points[1].value == protos[1].value

    def test_from_proto_list_returns_list_of_rawpoint(self):
        """
        Assert from_proto_list returns list of RawPoint
        """
        protos = [RawPointProto(time=1, value=10), RawPointProto(time=2, value=20)]
        points = RawPoint.from_proto_list(protos)

        assert len(points) == 2
        for point in points:
            assert isinstance(point, RawPoint)

    def test_get_returns_correct_values(self):
        """
        Assert the __get__ returns correct values
        """
        point = RawPoint(1, 10)
        assert point[0] == 1
        assert point[1] == 10

    def test_get_raises_indexerror(self):
        """
        Assert the __get__ raises IndexError
        """
        point = RawPoint(1, 1)
        with pytest.raises(IndexError):
            point[2]
        with pytest.raises(IndexError):
            point[-1]

    def test_to_proto(self):
        """
        Assert the to_proto
        """
        point = RawPoint(1, 10)
        proto = RawPoint.to_proto(point)
        assert proto.time == 1
        assert proto.value == 10
        assert proto.__class__ == btrdb_pb2.RawPoint

    def test_to_proto_list(self):
        """
        Assert the to_proto
        """
        points = [RawPoint(1, 10), RawPoint(2, 20)]
        protos = RawPoint.to_proto_list(points)
        assert protos[0].time == 1
        assert protos[0].value == 10
        assert protos[0].__class__ == btrdb_pb2.RawPoint
        assert protos[1].time == 2
        assert protos[1].value == 20
        assert protos[1].__class__ == btrdb_pb2.RawPoint

    def test_equals(self):
        """
        Assert the __eq__ works correctly
        """
        point1 = RawPoint(1, 1)
        point2 = RawPoint(1, 1)
        point3 = RawPoint(1, 2)
        point4 = RawPoint(2, 1)

        assert point1 == point2
        assert point1 != point3
        assert point1 != point4

    def test_repr_str(self):
        """
        Assert __repr__ and __str__ work correctly
        """
        point = RawPoint(1, 10)
        expected = "RawPoint(1, 10)"

        assert str(point) == expected
        assert point.__repr__() == expected


##########################################################################
## StatPoint Tests
##########################################################################

class TestStatPoint(object):

    def test_create(self):
        """
        Ensure we can create the object
        """
        StatPoint(1,1,1,1,1,1)

    def test_from_proto(self):
        """
        Assert from_proto creates a correct object
        """
        proto = StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6)
        point = StatPoint.from_proto(proto)
        assert point.time == proto.time
        assert point.min == proto.min
        assert point.mean == proto.mean
        assert point.max == proto.max
        assert point.count == proto.count
        assert point.stddev == proto.stddev

    def test_from_proto_creates_rawpoint(self):
        """
        Assert from_proto creates a new instance of StatPoint
        """
        proto = StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6)
        point = StatPoint.from_proto(proto)
        assert isinstance(point, StatPoint)

    def test_from_proto_list(self):
        """
        Assert from_proto_list creates valid objects
        """
        protos = [StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6),
            StatPointProto(time=11,min=12,mean=13,max=14,count=15,stddev=16)]
        points = StatPoint.from_proto_list(protos)
        assert points[0].time == protos[0].time
        assert points[0].min == protos[0].min
        assert points[0].mean == protos[0].mean
        assert points[0].max == protos[0].max
        assert points[0].count == protos[0].count
        assert points[0].stddev == protos[0].stddev
        assert points[1].time == protos[1].time
        assert points[1].min == protos[1].min
        assert points[1].mean == protos[1].mean
        assert points[1].max == protos[1].max
        assert points[1].count == protos[1].count
        assert points[1].stddev == protos[1].stddev

    def test_from_proto_list_returns_list_of_statpoint(self):
        """
        Assert from_proto_list returns list of StatPoint
        """
        protos = [StatPointProto(time=1,min=2,mean=3,max=4,count=5,stddev=6),
            StatPointProto(time=11,min=12,mean=13,max=14,count=15,stddev=16)]
        points = StatPoint.from_proto_list(protos)

        assert len(points) == 2
        for point in points:
            assert isinstance(point, StatPoint)

    def test_get_returns_correct_values(self):
        """
        Assert the __get__ returns correct values
        """
        point = StatPoint(1, 2, 3, 4, 5, 6)
        assert point[0] == 1
        assert point[1] == 2
        assert point[2] == 3
        assert point[3] == 4
        assert point[4] == 5
        assert point[5] == 6

    def test_get_raises_indexerror(self):
        """
        Assert the __get__ raises IndexError
        """
        point = StatPoint(1, 2, 3, 4, 5, 6)
        with pytest.raises(IndexError):
            point[6]
        with pytest.raises(IndexError):
            point[-1]
