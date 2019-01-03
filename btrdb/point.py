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
    def fromProto(proto):
        return RawPoint(proto.time, proto.value)

    @staticmethod
    def fromProtoList(protoList):
        rplist = []
        for proto in protoList:
            rp = RawPoint.fromProto(proto)
            rplist.append(rp)
        return rplist

    def __getitem__(self, index):
        if index == 0:
            return self.time
        elif index == 1:
            return self.value
        else:
            raise IndexError("RawPoint index out of range")

    @staticmethod
    def toProto(rp):
        return btrdb_pb2.RawPoint(time = rp[0], value = rp[1])

    @staticmethod
    def toProtoList(rplist):
        protoList = []
        for rp in rplist:
            proto = RawPoint.toProto(rp)
            protoList.append(proto)
        return protoList

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

    @staticmethod
    def fromProto(proto):
        return StatPoint(proto.time, proto.min, proto.mean, proto.max, proto.count, proto.stddev)

    @staticmethod
    def fromProtoList(protoList):
        splist = []
        for proto in protoList:
            sp = StatPoint.fromProto(proto)
            splist.append(sp)
        return splist

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
