# Copyright (c) 2017 Sam Kumar <samkumar@berkeley.edu>
# Copyright (c) 2017 Michael P Andersen <m.andersen@cs.berkeley.edu>
# Copyright (c) 2017 University of California, Berkeley
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the University of California, Berkeley nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNERS OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from btrdb4 import btrdb_pb2
import isodate
import datetime

NANOSECOND = 1
MICROSECOND = 1000 * NANOSECOND
MILLISECOND = 1000 * MICROSECOND
SECOND = 1000 * MILLISECOND
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR

def date(dst):
    """
    This parses a modified isodate into nanoseconds since the epoch. The date format is:
    YYYY-MM-DDTHH:MM:SS.NNNNNNNNN
    Fields may be omitted right to left, but do not elide leading or trailing zeroes in any field
    """
    idate = dst[:26]
    secs = (isodate.parse_datetime(idate) - datetime.datetime(1970,1,1)).total_seconds()
    nsecs = int(secs*1000000) * 1000
    nanoS = dst[26:]
    if len(nanoS) != 3 and len(nanoS) != 0:
        raise Exception("Invalid date string!")
    if len(nanoS) == 3:
        nsecs += int(nanoS)
    return nsecs

class ChangedRange(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    @staticmethod
    def fromProto(proto):
        return ChangedRange(proto.start, proto.end)

    @staticmethod
    def fromProtoList(protoList):
        crlist = []
        for proto in protoList:
            cr = ChangedRange.fromProto(proto)
            crlist.append(cr)
        return crlist

    def __getitem__(self, index):
        if index == 0:
            return self.start
        elif index == 1:
            return self.end
        else:
            raise IndexError("ChangedRange index out of range")

    def __repr__(self):
        return "ChangedRange({0}, {1})".format(repr(self.time), repr(self.value))

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


class StatPoint(object):
    def __init__(self, time, minv, meanv, maxv, count):
        self.time = time
        self.min = minv
        self.mean = meanv
        self.max = maxv
        self.count = count

    @staticmethod
    def fromProto(proto):
        return StatPoint(proto.time, proto.min, proto.mean, proto.max, proto.count)

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
        else:
            raise IndexError("RawPoint index out of range")

    def __repr__(self):
        return "StatPoint({0}, {1}, {2}, {3}, {4})".format(repr(self.time), repr(self.min), repr(self.mean), repr(self.max), repr(self.count))

def unpackProtoStreamDescriptor(desc):
    tags = {}
    for tag in desc.tags:
        tags[tag.key] = tag.value
    anns = {}
    for ann in desc.annotations:
        anns[ann.key] = ann.value
    return tags, anns

class BTrDBError(Exception):
    def __init__(self, code, msg, mash):
        self.code = code
        self.msg = msg
        self.mash = mash

    @staticmethod
    def fromProtoStat(protoStatus):
        return BTrDBError(protoStatus.code, protoStatus.msg, protoStatus.mash)

    @staticmethod
    def checkProtoStat(protoStatus):
        stat = BTrDBError.fromProtoStat(protoStatus)
        if stat.isError():
            raise stat

    def isError(self):
        return self.code != 0

    def __repr__(self):
        return "BTrDBError({0}, {1}, {2})".format(repr(self.code), repr(self.msg), repr(self.mash))

    def __str__(self):
        if self.isError():
            return "[{0}] {1}".format(self.code, self.msg)
        else:
            return "<success>"

class QueryType(object):
    @staticmethod
    def ALIGNED_WINDOWS_QUERY():
        return QueryType(btrdb_pb2.GenerateCSVParams.ALIGNED_WINDOWS_QUERY)

    @staticmethod
    def WINDOWS_QUERY():
        return QueryType(btrdb_pb2.GenerateCSVParams.WINDOWS_QUERY)

    @staticmethod
    def RAW_QUERY():
        return QueryType(btrdb_pb2.GenerateCSVParams.RAW_QUERY)
    
    def __init__(self, protoEnum):
        self.enum = protoEnum

    def toProto(self):
        return self.enum
