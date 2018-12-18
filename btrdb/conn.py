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
The 'btrdb' module provides Python bindings to interact with BTrDB.
"""

import grpc
import uuid
import os
import csv

from btrdb.endpoint import Endpoint
from btrdb.stream import Stream
from btrdb.utils.general import unpack_stream_descriptor
from btrdb.utils.query import QueryType

MIN_TIME = -(16 << 56)
MAX_TIME = 48 << 56
MAX_POINTWIDTH = 63


class Connection(object):
    def __init__(self, addrportstr, apikey=None):
        # type: () -> BTrDB
        """
        Connects to a BTrDB server

        Parameters
        ----------
        addrportstr: str
            The address of the cluster to connect to, e.g 123.123.123:4411
        apikey: str
            The option API key to authenticate requests

        Returns
        -------
        Connection
            A Connection class object.
        """
        addrport = addrportstr.split(":", 2)
        if len(addrport) != 2:
            raise ValueError("expecting address:port")
        if addrport[1] == "4411":
            # grpc bundles its own CA certs which will work for all normal SSL
            # certificates but will fail for custom CA certs. Allow the user
            # to specify a CA bundle via env var to overcome this
            ca_bundle = os.getenv("BTRDB_CA_BUNDLE","")
            if ca_bundle != "":
                with open(ca_bundle, "rb") as f:
                    contents = f.read()
            else:
                contents = None
            if apikey is None:
                self.channel = grpc.secure_channel(addrportstr, grpc.ssl_channel_credentials(contents))
            else:
                self.channel = grpc.secure_channel(addrportstr, grpc.composite_channel_credentials(grpc.ssl_channel_credentials(contents), grpc.access_token_call_credentials(apikey)))
        else:
            if apikey is not None:
                raise ValueError("cannot use an API key with an insecure (port 4410) BTrDB API. Try port 4411")
            self.channel = grpc.insecure_channel(addrportstr)

        return BTrDB(Endpoint(self.channel))


class BTrDB(object):
    def __init__(self, endpoint):
        # type: (Endpoint) -> None
        self.ep = endpoint

    def streamFromUUID(self, uu):
        # type: (UUID) -> Stream
        """
        Creates a stream handle to the BTrDB stream with the UUID `uu`.

        Creates a stream handle to the BTrDB stream with the UUID `uu`. This method does not check whether a stream with the specified UUID exists. It is always good form to check whether the stream existed using stream.exists().


        Parameters
        ----------
        uu: UUID
            The uuid of the requested stream.

        Returns
        -------
        Stream
            A Stream class object.

        """

        return Stream(self, uu)

    def create(self, uu, collection, tags=None, annotations=None):
        # type: (UUID, str, Dict[str, str], Dict[str, str]) -> Stream
        """
        Tells BTrDB to create a new stream with UUID `uu` in `collection` with specified `tags` and `annotations`.

        Parameters
        ----------
        uu: UUID
            The uuid of the requested stream.

        Returns
        -------
        Stream
            a Stream class object
        """

        if tags is None:
            tags = {}

        if annotations is None:
            annotations = {}

        self.ep.create(uu, collection, tags, annotations)
        return Stream(self, uu, True, collection, tags.copy(), annotations.copy(), 0)

    def info(self):
        # type: () -> (Mash)
        """
        Returns information about the connected BTrDB srerver.
        """

        return self.ep.info()

    """
    This function does not work (1/12/2018) so it is being commented out.

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

    """

    def lookupStreams(self, collection, isCollectionPrefix, tags=None, annotations=None):
        # type: (str, bool, Dict[str, str], Dict[str, str]) -> Stream

        """
        Search for streams matching given parameters

        This function allows for searching

        Parameters
        ----------
        collection: str
            The name of the collection to be found, case sensitive.
        isCollectionPrefix: bool
            Whether the collection is a prefix.
        tags: Dict[str, str]
            The tags to identify the stream.
        annotations: Dict[str, str]
            The annotations to identify the stream.

        Yields
        ------
        Stream Generator
            A stream generator that iterates over the search results.

        """

        if tags is None:
            tags = {}

        if annotations is None:
            annotations = {}

        streams = self.ep.lookupStreams(collection, isCollectionPrefix, tags, annotations)
        for desclist in streams:
            for desc in desclist:
                tagsanns = unpack_stream_descriptor(desc)
                yield Stream(self, uuid.UUID(bytes = desc.uuid), True, desc.collection, tagsanns[0], tagsanns[1], desc.annotationVersion)

    def getMetadataUsage(self, prefix):
        # type: (csv.writer, QueryType, int, int, int, int, bool, *Tuple[int, str, UUID]) -> Tuple[Dict[str, int], Dict[str, int]]
        """
        Gives statistics about metadata for collections that match a
        prefix.

        Parameters
        ----------
        prefix: str
            A prefix of the collection names to look at
        """
        ep = self.ep
        tags, annotations = ep.getMetadataUsage(prefix)
        pyTags = {tag.key: tag.count for tag in tags}
        pyAnn = {ann.key: ann.count for ann in annotations}
        return pyTags, pyAnn

    def windowsToCsv(self, csvFile, start, end, width, depth, includeVersions, *streams):
        # type: (File, int, int, int, int, bool, *Tuple[int, str, UUID]) -> None
        """
        Writes windows streams to a csv file using the provided
        configuration.

        Parameters
        ----------
        csvFile: File
            The csv file where rows will be written
        start: int
            The start time in nanoseconds for the queries
        end: int
            The end time in nanoseconds
        width: int
            The width of the stat points
        depth: int
            The depth of the queries
        includeVersions: bool
            Include the stream versions in the header labels
        streams: *Tuple[int, str, UUID]
            The version, label and uuid for the streams to be queried.
        """
        csvWriter = csv.reader(csvFile)
        return self.writeCSV(
            self, csvWriter, QueryType.WINDOWS_QUERY(), start, end, width, depth, includeVersions, *streams)

    def alignedWindowsToCSV(self, csvFile, start, end, width, depth, includeVersions, *streams):
        # type: (File, int, int, int, bool, *Tuple[int, str, UUID]) -> None
        """
        Writes aligned windows streams to a csv file using the
        provided configuration.

        Parameters
        ----------
        csvFile: File
            The csv file where rows will be written
        start: int
            The start time in nanoseconds for the queries
        end: int
            The end time in nanoseconds
        width: int
            The width of the stat points
        depth: int
            The depth of the queries
        includeVersions: bool
            Include the stream versions in the header labels
        streams: *Tuple[int, str, UUID]
            The version, label and uuid for the streams to be queried.
        """
        csvWriter = csv.reader(csvFile)
        return self.writeCSV(
            self, csvWriter, QueryType.ALIGNED_WINDOWS_QUERY(),
            start, end, width, depth, includeVersions, *streams)

    def rawValuesToCSV(self, csvFile, start, end, width, depth, includeVersions, *streams):
        # type: (File, int, int, int, int, bool, *Tuple[int, str, UUID]) -> None
        """
        Writes raw values streams to a csv file using the provided
        configuration.

        Parameters
        ----------
        csvFile: File
            The csv file where rows will be written
        start: int
            The start time in nanoseconds for the queries
        end: int
            The end time in nanoseconds
        width: int
            The width of the stat points
        depth: int
            The depth of the queries
        includeVersions: bool
            Include the stream versions in the header labels
        streams: *Tuple[int, str, UUID]
            The version, label and uuid for the streams to be queried.
        """
        csvWriter = csv.reader(csvFile)
        return self.writeCSV(
            self, csvWriter, QueryType.RAW_QUERY(),
            start, end, width, depth, includeVersions, *streams)

    def writeCSV(self, csvWriter, queryType, start, end, width, depth, includeVersions, *streams):
        # type: (csv.writer, QueryType, int, int, int, int, bool, *Tuple[int, str, UUID]) -> None
        """
        Writes streams to a csv writer using the provided configuration.

        Parameters
        ----------
        csvWriter: csv.writer
            The csv writer where rows will be written
        queryType: QueryType
            The type of query for the streams
        start: int
            The start time in nanoseconds for the queries
        end: int
            The end time in nanoseconds
        width: int
            The width of the stat points (This is only used for windows queries)
        depth: int
            The depth of the queries
        includeVersions: bool
            Include the stream versions in the header labels
        streams: *Tuple[int, str, UUID]
            The version, label and uuid for the streams to be queried.
        """

        rows = self.ep.generateCSV(queryType, start, end, width, depth, includeVersions, *streams)
        for row in rows:
            csvWriter.writerow(row)
