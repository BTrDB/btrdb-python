# btrdb.point
# Module for Point classes
#
# Author:   PingThings
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# For license information, see LICENSE.txt
# ID: point.py [] allen@pingthings.io $

"""
Module for Point classes
"""

##########################################################################
## Imports
##########################################################################

from btrdb.grpcinterface import btrdb_pb2


##########################################################################
## Point Classes
##########################################################################

class RawPoint(object):
    def __init__(self, time, value):
        self.time = time
        self.value = value

    @staticmethod
    def from_proto(proto):
        return RawPoint(proto.time, proto.value)

    @staticmethod
    def from_proto_list(proto_list):
        return [RawPoint.from_proto(proto) for proto in proto_list]

    def __getitem__(self, index):
        if index == 0:
            return self.time
        elif index == 1:
            return self.value
        else:
            raise IndexError("RawPoint index out of range")

    @staticmethod
    def to_proto(point):
        return btrdb_pb2.RawPoint(time = point[0], value = point[1])

    @staticmethod
    def to_proto_list(points):
        return [RawPoint.to_proto(p) for p in points]

    def __repr__(self):
        return "RawPoint({0}, {1})".format(repr(self.time), repr(self.value))

    def __eq__(self, other):
        return self.time == other.time and self.value == other.value


class StatPoint(object):
    def __init__(self, time, minv, meanv, maxv, count, stddev):
        self.time = time
        self.min = minv
        self.mean = meanv
        self.max = maxv
        self.count = count
        self.stddev = stddev

    @classmethod
    def from_proto(cls, proto):
        return cls(proto.time, proto.min, proto.mean, proto.max, proto.count, proto.stddev)

    @classmethod
    def from_proto_list(cls, proto_list):
        return [cls.from_proto(proto) for proto in proto_list]

    def __getitem__(self, index):
        if index == 0:
            return self.time
        elif index == 1:
            return self.min
        elif index == 2:
            return self.mean
        elif index == 3:
            return self.max
        elif index == 4:
            return self.count
        elif index == 5:
            return self.stddev
        else:
            raise IndexError("RawPoint index out of range")

    def __repr__(self):
        return "StatPoint({0}, {1}, {2}, {3}, {4})".format(
            repr(self.time),
            repr(self.min),
            repr(self.mean),
            repr(self.max),
            repr(self.count),
            repr(self.stddev)
        )
