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

from btrdb.grpcinterface import btrdb_pb2
from btrdb.grpcinterface import btrdb_pb2_grpc
from btrdb.point import RawPoint
from btrdb.exceptions import BTrDBError, error_handler, check_proto_stat
from btrdb.utils.general import unpack_stream_descriptor


class Endpoint(object):
    def __init__(self, channel):
        self.stub = btrdb_pb2_grpc.BTrDBStub(channel)

    @error_handler
    def rawValues(self, uu, start, end, version=0):
        params = btrdb_pb2.RawValuesParams(
            uuid=uu.bytes, start=start, end=end, versionMajor=version
        )
        for result in self.stub.RawValues(params):
            check_proto_stat(result.stat)
            yield result.values, result.versionMajor

    @error_handler
    def alignedWindows(self, uu, start, end, pointwidth, version=0):
        params = btrdb_pb2.AlignedWindowsParams(
            uuid=uu.bytes,
            start=start,
            end=end,
            versionMajor=version,
            pointWidth=int(pointwidth),
        )
        for result in self.stub.AlignedWindows(params):
            check_proto_stat(result.stat)
            yield result.values, result.versionMajor

    @error_handler
    def windows(self, uu, start, end, width, depth, version=0):
        params = btrdb_pb2.WindowsParams(
            uuid=uu.bytes,
            start=start,
            end=end,
            versionMajor=version,
            width=width,
            depth=depth,
        )
        for result in self.stub.Windows(params):
            check_proto_stat(result.stat)
            yield result.values, result.versionMajor

    @error_handler
    def streamInfo(self, uu, omitDescriptor, omitVersion):
        params = btrdb_pb2.StreamInfoParams(
            uuid=uu.bytes, omitVersion=omitVersion, omitDescriptor=omitDescriptor
        )
        result = self.stub.StreamInfo(params)
        desc = result.descriptor
        check_proto_stat(result.stat)
        tagsanns = unpack_stream_descriptor(desc)
        return desc.collection, desc.propertyVersion, tagsanns[0], tagsanns[1], result.versionMajor

    @error_handler
    def obliterate(self, uu):
        params = btrdb_pb2.ObliterateParams(uuid=uu.bytes)
        result = self.stub.Obliterate(params)
        check_proto_stat(result.stat)

    @error_handler
    def setStreamAnnotations(self, uu, expected, changes, removals):
        annkvlist = []
        for k, v in changes.items():
            if v is None:
                ov = None
            else:
                if isinstance(v, str):
                    v = v.encode("utf-8")
                ov = btrdb_pb2.OptValue(value=v)
            kv = btrdb_pb2.KeyOptValue(key=k, val=ov)
            annkvlist.append(kv)
        params = btrdb_pb2.SetStreamAnnotationsParams(
            uuid=uu.bytes,
            expectedPropertyVersion=expected,
            changes=annkvlist,
            removals=removals,
        )
        result = self.stub.SetStreamAnnotations(params)
        check_proto_stat(result.stat)

    @error_handler
    def setStreamTags(self, uu, expected, tags, collection):
        tag_data = []
        for k, v in tags.items():
            if v is None:
                ov = None
            else:
                if isinstance(v, str):
                    v = v.encode("utf-8")
                ov = btrdb_pb2.OptValue(value=v)
            kv = btrdb_pb2.KeyOptValue(key=k, val=ov)
            tag_data.append(kv)
        params = btrdb_pb2.SetStreamTagsParams(
            uuid=uu.bytes,
            expectedPropertyVersion=expected,
            tags=tag_data,
            collection=collection,
        )
        result = self.stub.SetStreamTags(params)
        check_proto_stat(result.stat)

    @error_handler
    def create(self, uu, collection, tags, annotations):
        tagkvlist = []
        for k, v in tags.items():
            kv = btrdb_pb2.KeyOptValue(key = k, val = btrdb_pb2.OptValue(value=v))
            tagkvlist.append(kv)
        annkvlist = []
        for k, v in annotations.items():
            kv = btrdb_pb2.KeyOptValue(key = k, val = btrdb_pb2.OptValue(value=v))
            annkvlist.append(kv)
        params = btrdb_pb2.CreateParams(
            uuid=uu.bytes, collection=collection, tags=tagkvlist, annotations=annkvlist
        )
        result = self.stub.Create(params)
        check_proto_stat(result.stat)

    @error_handler
    def listCollections(self, prefix):
        """
        Returns a generator for windows of collection paths matching search

        Yields
        ------
        collection paths : list[str]
        """
        params = btrdb_pb2.ListCollectionsParams(prefix=prefix)
        for msg in self.stub.ListCollections(params):
            check_proto_stat(msg.stat)
            yield msg.collections

    @error_handler
    def lookupStreams(self, collection, isCollectionPrefix, tags, annotations):
        tagkvlist = []
        for k, v in tags.items():
            if v is None:
                ov = None
            else:
                if isinstance(v, str):
                    v = v.encode("utf-8")
                ov = btrdb_pb2.OptValue(value=v)
            kv = btrdb_pb2.KeyOptValue(key=k, val=ov)
            tagkvlist.append(kv)
        annkvlist = []
        for k, v in annotations.items():
            if v is None:
                ov = None
            else:
                if isinstance(v, str):
                    v = v.encode("utf-8")
                ov = btrdb_pb2.OptValue(value=v)
            kv = btrdb_pb2.KeyOptValue(key=k, val=ov)
            annkvlist.append(kv)
        params = btrdb_pb2.LookupStreamsParams(
            collection=collection,
            isCollectionPrefix=isCollectionPrefix,
            tags=tagkvlist,
            annotations=annkvlist,
        )
        for result in self.stub.LookupStreams(params):
            check_proto_stat(result.stat)
            yield result.results

    @error_handler
    def nearest(self, uu, time, version, backward):
        params = btrdb_pb2.NearestParams(
            uuid=uu.bytes, time=time, versionMajor=version, backward=backward
        )
        result = self.stub.Nearest(params)
        check_proto_stat(result.stat)
        return result.value, result.versionMajor
    
    @error_handler
    def changes(self, uu, fromVersion, toVersion, resolution):
        params = btrdb_pb2.ChangesParams(
            uuid=uu.bytes,
            fromMajor=fromVersion,
            toMajor=toVersion,
            resolution=resolution,
        )
        for result in self.stub.Changes(params):
            check_proto_stat(result.stat)
            yield result.ranges, result.versionMajor

    @error_handler
    def insert(self, uu, values, policy):
        policy_map = {
            "never": btrdb_pb2.MergePolicy.NEVER,
            "equal": btrdb_pb2.MergePolicy.EQUAL,
            "retain": btrdb_pb2.MergePolicy.RETAIN,
            "replace": btrdb_pb2.MergePolicy.REPLACE,
        }
        protoValues = RawPoint.to_proto_list(values)
        params = btrdb_pb2.InsertParams(
            uuid=uu.bytes,
            sync=False,
            values=protoValues,
            merge_policy=policy_map[policy],
        )
        result = self.stub.Insert(params)
        check_proto_stat(result.stat)
        return result.versionMajor

    @error_handler
    def deleteRange(self, uu, start, end):
        params = btrdb_pb2.DeleteParams(uuid=uu.bytes, start=start, end=end)
        result = self.stub.Delete(params)
        check_proto_stat(result.stat)
        return result.versionMajor

    @error_handler
    def info(self):
        params = btrdb_pb2.InfoParams()
        result = self.stub.Info(params)
        check_proto_stat(result.stat)
        return result

    @error_handler
    def faultInject(self, typ, args):
        params = btrdb_pb2.FaultInjectParams(type=typ, params=args)
        result = self.stub.FaultInject(params)
        check_proto_stat(result.stat)
        return result.rv

    @error_handler
    def flush(self, uu):
        params = btrdb_pb2.FlushParams(uuid=uu.bytes)
        result = self.stub.Flush(params)
        check_proto_stat(result.stat)

    @error_handler
    def getMetadataUsage(self, prefix):
        params = btrdb_pb2.MetadataUsageParams(prefix=prefix)
        result = self.stub.GetMetadataUsage(params)
        check_proto_stat(result.stat)
        return result.tags, result.annotations

    @error_handler
    def generateCSV(self, queryType, start, end, width, depth, includeVersions, *streams):
        protoStreams = [btrdb_pb2.StreamCSVConfig(version = stream[0],
                        label = stream[1],
                        uuid = stream[2].bytes)
                        for stream in streams]
        params = btrdb_pb2.GenerateCSVParams(queryType = queryType.to_proto(),
                                            startTime = start,
                                            endTime = end,
                                            windowSize = width,
                                            depth = depth,
                                            includeVersions = includeVersions,
                                            streams = protoStreams)
        for result in self.stub.GenerateCSV(params):
            check_proto_stat(result.stat)
            yield result.row

    @error_handler
    def sql_query(self, stmt, params=[]):
        request = btrdb_pb2.SQLQueryParams(query=stmt, params=params)
        for page in self.stub.SQLQuery(request):
            check_proto_stat(page.stat)
            yield page.SQLQueryRow
