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

"""
The 'btrdb4' module provides Python bindings to interact with BTrDB.
"""

import grpc
import btrdb_pb2
import btrdb_pb2_grpc

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
        return "StatPoint({0}, {1})".format(repr(self.time), repr(self.min), repr(self.mean), repr(self.max), repr(self.count))

class BTrDBError(object):
    def __init__(self, code, msg, mash):
        self.code = code
        self.msg = msg
        self.mash = mash

    @staticmethod
    def fromProtoStat(protoStatus):
        return BTrDBError(protoStatus.code, protoStatus.msg, protoStatus.mash)

    def isError(self):
        return self.code != 0

    def __repr__(self):
        return "BTrDBError({0}, {1}, {2})".format(repr(self.code), repr(self.msg), repr(self.mash))

    def __str__(self):
        if self.isError():
            return "[{0}] {1}".format(self.code, self.msg)
        else:
            return "<success>"

class BTrDBConnection(object):
    def __init__(self, addrportstr):
        self.channel = grpc.insecure_channel(addrportstr)

    def newContext(self):
        return BTrDBEndpoint(self.channel)


class BTrDBEndpoint(object):
    def __init__(self, channel):
        self.stub = btrdb_pb2_grpc.BTrDBStub(channel)

    def rawValues(self, uu, start, end, version = 0):
        params = btrdb_pb2.RawValuesParams(uuid = uu.bytes, start = start, end = end, versionMajor = version)
        for result in self.stub.RawValues(params):
            yield RawPoint.fromProtoList(result.values), result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def alignedWindows(self, uu, start, end, pointwidth, version = 0):
        params = btrdb_pb2.AlignedWindowsParams(uuid = uu.bytes, start = start, end = end, versionMajor = version, pointWidth = pointwidth)
        for result in self.stub.AlignedWindows(params):
            yield StatPoint.fromProtoList(result.values), result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def windows(self, uu, start, end, width, depth, version = 0):
        params = btrdb_pb2.WindowsParams(uuid = uu.bytes, start = start, end = end, versionMajor = version, width = width, depth = depth)
        for result in self.stub.Windows(params):
            yield StatPoint.fromProtoList(result.values), result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def streamInfo(self, uu, omitDescriptor, omitVersion):
        params = btrdb_pb2.StreamInfoParams(uuid = uu.bytes, omitVersion = omitVersion, omitDescriptor = omitDescriptor)
        result = self.stub.StreamInfo(params)
        desc = result.descriptor
        tags = {}
        for tag in desc.tags:
            tags[tag.key] = tag.value
        anns = {}
        for ann in desc.annotations:
            anns[ann.key] = ann.value
        return desc.collection, desc.annotationVersion, tags, anns, result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def setStreamAnnotations(self, uu, expected, changes):
        annkvlist = []
        for k, v in changes.iteritems():
            if v is None:
                ov = None
            else:
                ov = btrdb_pb2.OptValue(v)
            kv = btrdb_pb2.KeyOptValue(key = k, value = ov)
            annkvlist.append(kv)
        params = btrdb_pb2.SetStreamAnnotationsParams(uuid = uu.bytes, expectedAnnotationVersion = expected, annotations = annkvlist)
        result = self.stub.SetStreamAnnotations(params)
        return BTrDBError.fromProtoStat(result.stat)

    def create(self, uu, collection, tags = {}, annotations = {}):
        tagkvlist = []
        for k, v in tags.iteritems():
            kv = btrdb_pb2.KeyValue(key = k, value = v)
            tagkvlist.append(kv)
        annkvlist = []
        for k, v in annotations.iteritems():
            kv = btrdb_pb2.KeyValue(key = k, value = v)
            annkvlist.append(kv)
        params = btrdb_pb2.CreateParams(uuid = uu.bytes, collection = collection, tags = tagkvlist, annotations = annkvlist)
        result = self.stub.Create(params)
        return BTrDBError.fromProtoStat(result.stat)

    def listCollections(self, prefix, startingAt, limit):
        params = btrdb_pb2.ListCollectionsParams(prefix = prefix, startWith = startingAt, limit = limit)
        result = self.stub.ListCollections(params)
        return result.collections, BTrDBError.fromProtoStat(result.stat)

    def lookupStreams(self, collection, isCollectionPrefix, tags, annotations):
        tagkvlist = []
        for k, v in tags.iteritems():
            if v is None:
                ov = None
            else:
                ov = btrdb_pb2.OptValue(v)
            kv = btrdb_pb2.KeyOptValue(key = k, value = ov)
            tagkvlist.append(kv)
        annkvlist = []
        for k, v in annotations.iteritems():
            if v is None:
                ov = None
            else:
                ov = btrdb_pb2.OptValue(v)
            kv = btrdb_pb2.KeyOptValue(key = k, value = ov)
            annkvlist.append(kv)
        params = btrdb_pb2.LookupStreamsParams(collection = collection, isCollectionPrefix = isCollectionPrefix, tags = tagkvlist, annotations = annkvlist)
        for result in self.stub.LookupStreams(params):
            yield result.results, BTrDBError.fromProtoStat(result.stat)

    def nearest(self, uu, time, version, backward):
        params = btrdb_pb2.NearestParams(uuid = uu.bytes, time = time, versionMajor = version, backward = backward)
        result = self.stub.Nearest(params)
        return result.value, result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def changes(self, uu, fromVersion, toVersion, resolution):
        params = btrdb_pb2.ChangesParams(uuid = uu.bytes, fromMajor = fromVersion, toMajor = toVersion, resolution = resolution)
        for result in self.stub.Changes(params):
            yield ChangedRange.fromProtoList(result.ranges), result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def insert(self, uu, values, sync = False):
        protoValues = RawPoint.toProtoList(values)
        params = btrdb_pb2.InsertParams(uuid = uu.bytes, sync = sync, values = protoValues)
        result = self.stub.Insert(params)
        return result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def deleteRange(self, uu, start, end):
        params = btrdb_pb2.DeleteParams(uuid = uu.bytes, start = start, end = end)
        result = self.stub.Delete(params)
        return result.versionMajor, BTrDBError.fromProtoStat(result.stat)

    def info(self):
        params = btrdb_pb2.InfoParams()
        result = self.stub.Info(params)
        return result.mash, BTrDBError.fromProtoStat(result.stat)

    def faultInject(self, typ, args):
        params = btrdb_pb2.FaultInjectParams(type = typ, params = args)
        result = self.stub.FaultInject(params)
        return result.rv, BTrDBError.fromProtoStat(result.stat)

    def flush(self, uu):
        params = btrdb_pb2.FlushParams(uuid = uu.bytes)
        result = self.stub.Flush(params)
        return BTrDBError.fromProtoStat(result.stat)
