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

from btrdb4.endpoint import Endpoint
from btrdb4.utils import *

class Connection(object):
    def __init__(self, addrportstr):
        self.channel = grpc.insecure_channel(addrportstr)

    def newContext(self):
        e = Endpoint(self.channel)
        return BTrDB(e)

class BTrDB(object):
    def __init__(self, endpoint):
        self.ep = endpoint

    def streamFromUUID(self, uu):
        return Stream(self, uu)

    def create(self, uu, collection, tags = None, annotations = None):
        if tags is None:
            tags = {}
        if annotations is None:
            annotations = {}

        self.ep.create(uu, collection, tags, annotations)
        return Stream(self, uu, True, collection, tags.copy(), annotations.copy(), 0)

    def info(self):
        return self.ep.info()

    def listCollections(self, prefix):
        colls = (prefix,)
        maximum = 10
        got = maximum
        while got == maximum:
            startingAt = colls[-1]
            colls = self.ep.listCollections(prefix, colls, maximum)
            for coll in colls:
                yield coll
            got = len(colls)

    def lookupStreams(self, collection, isCollectionPrefix, tags = None, annotations = None):
        if tags is None:
            tags = {}
        if annotations is None:
            annotations = {}

        streams = self.ep.lookupStreams(collection, isCollectionPrefix, tags, annotations)
        for desc in streams:
            tagsanns = unpackProtoStreamDescriptor(desc)
            yield Stream(self.b, desc.uuid, True, desc.collection, desc.annotationVersion, *tagsanns)


class Stream(object):
    def __init__(self, btrdb, uuid, knownToExist = False, collection = None, tags = None, annotations = None, annotationVersion = None):
        self.b = btrdb
        self.uuid = uuid
        self.knownToExist = knownToExist

        # Some cacheable attributes
        self.tags = tags
        self.annotations = annotations
        self.annotationVersion = annotationVersion
        self.collection = collection

    def refreshMeta(self):
        ep = self.b.ep
        self.collection, self.annotationVersion, self.tags, self.annotations, _ = ep.streamInfo(self.uuid, False, True)
        self.knownToExist = True

    def exists(self):
        if self.knownToExist:
            return True

        try:
            self.refreshMeta()
            return True
        except BTrDBError as bte:
            if bte.code == 404:
                return False
            raise

    def uuid(self):
        return self.uuid

    def tags(self):
        if self.tags is not None:
            return self.tags

        self.refreshMeta()
        return self.tags

    def annotations(self):
        self.refreshMeta()
        return self.annotations, self.annotationVersion

    def cachedAnnotations(self):
        if self.annotations is None:
            self.refeshMeta()
        return self.annotations, self.annotationVersion

    def collection(self):
        if self.collection is not None:
            return self.collection

        self.refreshMeta()
        return self.collection

    def version(self):
        ep = self.b.ep
        _, _, _, _, ver = ep.streamInfo(self.uuid, True, False)
        return ver

    def insert(self, vals, sync = False):
        ep = self.b.ep
        batchsize = 5000
        i = 0
        version = 0
        while i < len(vals):
            thisBatch = vals[i:i + batchsize]
            version = ep.insert(self.uuid, thisBatch, sync = sync)
            i += batchsize
        return version

    def rawValues(self, start, end, version = 0):
        ep = self.b.ep
        rps = ep.rawValues(self.uuid, start, end, version)
        for rplist, version in rps:
            for rp in rplist:
                yield RawPoint.fromProto(rp), version

    def alignedWindows(self, start, end, pointwidth, version = 0):
        ep = self.b.ep
        sps = ep.alignedWindows(self.uuid, start, end, pointwidth, version)
        for splist, version in sps:
            for sp in splist:
                yield StatPoint.fromProto(sp), version

    def windows(self, start, end, width, depth = 0, version = 0):
        ep = self.b.ep
        sps = ep.windows(self.uuid, start, end, width, depth, version)
        for splist, version in sps:
            for sp in splist:
                yield StatPoint.fromProto(sp), version

    def deleteRange(self, start, end):
        ep = self.b.ep
        return ep.deleteRange(self.uuid, start, end)

    def nearest(self, time, version, backward):
        ep = self.b.ep
        rp, version = ep.nearest(self.uuid, time, version, backward)
        return RawPoint.fromProto(rp), version

    def changes(self, fromVersion, toVersion, resolution):
        ep = self.b.ep
        crs = ep.changes(self.uuid, fromVersion, toVersion, resolution)
        for crlist, version in crs:
            for cr in crlist:
                yield ChangeRange.fromProto(cr), version
