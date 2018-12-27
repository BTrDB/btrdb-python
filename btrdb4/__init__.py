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
import uuid
import os

from btrdb4.endpoint import Endpoint
from btrdb4.utils import *

MIN_TIME = -(16 << 56)
MAX_TIME = 48 << 56
MAX_POINTWIDTH = 63
BTRDB_ENDPOINTS = "BTRDB_ENDPOINTS"
BTRDB_API_KEY = "BTRDB_API_KEY"


def connect(addrportstr=None, apikey=None):
    """
    Connect to a BTrDB server.

    Parameters
    ----------
    addrportstr: str, default=None
        The address and port of the cluster to connect to, e.g. 192.168.1.1:4411.
        If set to None will look up the string from the environment variable
        $BTRDB_ENDPOINTS (recommended).

    apikey: str, default=None
        The API key use to authenticate requests (optional). If None, the key
        is looked up from the environment variable $BTRDB_API_KEY.

    Returns
    -------
    db : BTrDB
        An instance of the BTrDB context to directly interact with the database.
    """
    if addrportstr is None:
        addrportstr = os.environ.get(BTRDB_ENDPOINTS, default="")

    if apikey is None:
        apikey = os.environ.get(BTRDB_API_KEY, default=None)

    return Connection(addrportstr, apikey).newContext()


class Connection(object):
    def __init__(self, addrportstr, apikey=None):
        # type: (str, str) -> Connection
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

    def newContext(self):
        # type: () -> BTrDB
        e = Endpoint(self.channel)
        return BTrDB(e)

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
                tagsanns = unpackProtoStreamDescriptor(desc)
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

    def alignedWindowsToCSV(self, csvFile, start, end, depth, includeVersions, *streams):
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


class Stream(object):
    def __init__(self, btrdb, uuid, knownToExist = False, collection = None, tags = None, annotations = None, annotationVersion = None):
        self.b = btrdb
        self.uu = uuid
        self.knownToExist = knownToExist

        # Some cacheable attributes
        self.cachedTags = tags
        self.cachedAnnotations = annotations
        self.cachedAnnotationVersion = annotationVersion
        self.cachedCollection = collection

    def refreshMeta(self):
        # type: () -> ()
        """
        Refreshes the locally cached meta data for a stream

        Queries the BTrDB server for all stream metadata including collection, annotation, and tags. This method requires a round trip to the server.

        """

        ep = self.b.ep
        self.cachedCollection, self.cachedAnnotationVersion, self.cachedTags, self.cachedAnnotations, _ = ep.streamInfo(self.uu, False, True)
        self.knownToExist = True

    def exists(self):
        # type: () -> bool
        """
        Check if stream exists

        Exists returns true if the stream exists. This is essential after using
        StreamFromUUID as the stream may not exist, causing a 404 error on
        later stream operations. Any operation that returns a stream from
        collection and tags will have ensured the stream exists already.

        Returns
        -------
        bool
            Indicates whether stream exists.
        """

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
        # type: () -> UUID
        """
        Returns the stream's UUID.

        This method returns the stream's UUID. The stream may nor may not exist yet, depending on how the stream object was obtained.

        Parameters
        ----------
        None

        Returns
        -------
        UUID
            The UUID of the stream.


        See Also
        --------
        stream.exists()

        """

        return self.uu

    def tags(self):
        # type: () -> Dict[str, str]
        """
        Returns the stream's tags.

        Tags returns the tags of the stream. It may require a round trip to the server depending on how the stream was acquired. Do not modify the resulting map as it is a reference to the internal stream state.

        Parameters
        ----------
        None

        Returns
        -------
        List[Dict[str, str]]
            A list of dictionaries containing the tags.

        """
        if self.cachedTags is not None:
            return self.cachedTags

        self.refreshMeta()
        return self.cachedTags

    def annotations(self):
        # type: () -> Tuple[Dict[str, str], int]
        """

        Returns a stream's annotations

        Annotations returns the annotations of the stream (and the annotation version). It will always require a round trip to the server. If you are ok with stale data and want a higher performance version, use Stream.CachedAnnotations().

        Do not modify the resulting map.


        Parameters
        ----------
        None

        Returns
        -------
        Tuple[Dict[str, str], int]
            A tuple containing a dictionary of annotations and an integer representing the version of those annotations.

        """
        self.refreshMeta()
        return self.cachedAnnotations, self.cachedAnnotationVersion

    def cachedAnnotations(self):
        # type: () -> Tuple[Dict[str, str], int]
        """

        CachedAnnotations returns the annotations of the stream, reusing previous results if available, otherwise fetching from the server.

        Parameters
        ----------
        None

        Returns
        -------
        Tuple[Dict[str, str], int]
            A tuple containing a dictionary of annotations and an integer representing the version of those annotations.

        """
        if self.cachedAnnotations is None:
            self.refeshMeta()
        return self.cachedAnnotations, self.cachedAnnotationVersion

    def collection(self):
        # type: () -> str
        """
        Returns the collection of the stream. It may require a round trip to the server depending on how the stream was acquired.

        Returns
        -------
        str
            the collection of the stream

        """

        if self.cachedCollection is not None:
            return self.cachedCollection

        self.refreshMeta()
        return self.cachedCollection

    def version(self):
        # type: () -> int
        """
        Returns the current data version of the stream.

        Version returns the current data version of the stream. This is not cached, it queries each time. Take care that you do not intorduce races in your code by assuming this function will always return the same vaue

        Parameters
        ----------
        None

        Returns
        -------
        int
            The version of the stream.

        """
        ep = self.b.ep
        _, _, _, _, ver = ep.streamInfo(self.uu, True, False)
        return ver

    def insert(self, vals):
        # type: List[Tuple[int, float]] -> int
        """
        Insert new data in the form (time, value) into the series.

        Inserts a list of new (time, value) tuples into the series. The tuples in the list need not be sorted by time. If the arrays are larger than appropriate, this function will automatically chunk the inserts. As a consequence, the insert is not necessarily atomic, but can be used with a very large array.

        Returns
        -------
        version : int
            The version of the stream after inserting new points.

        """
        ep = self.b.ep
        batchsize = 5000
        i = 0
        version = 0
        while i < len(vals):
            thisBatch = vals[i:i + batchsize]
            version = ep.insert(self.uu, thisBatch)
            i += batchsize
        return version

    def rawValues(self, start, end, version=0):
        # type: (int, int, int) -> Tuple[RawPoint, int]

        """
        Read raw values from BTrDB between time [a, b) in nanoseconds.

        RawValues queries BTrDB for the raw time series data points between `start` and `end` time, both in nanoseconds since the Epoch for the specified stream `version`.

        Parameters
        ----------
        start: int
            The start time in nanoseconds for the range to be retrieved
        end : int
            The end time in nanoseconds for the range to be deleted
        version: int
            The version of the stream to be queried

        Yields
        ------
        (RawPoint, version) : (RawPoint, int)
            Returns a tuple containing a RawPoint and the stream version


        Notes
        -----
        Note that the raw data points are the original values at the sensor's native sampling rate (assuming the time series represents measurements from a sensor). This is the lowest level of data with the finest time granularity. In the tree data structure of BTrDB, this data is stored in the vector nodes.

        """

        ep = self.b.ep
        rps = ep.rawValues(self.uu, start, end, version)
        for rplist, version in rps:
            for rp in rplist:
                yield RawPoint.fromProto(rp), version

    def alignedWindows(self, start, end, pointwidth, version=0):
        # type: (int, int, int, int) -> Tuple[StatPoint, int]

        """
        Read statistical aggregates of windows of data from BTrDB.

        Query BTrDB for aggregates (or roll ups or windows) of the time series with `version` between time `start` (inclusive) and `end` (exclusive) in nanoseconds. Each point returned is a statistical aggregate of all the raw data within a window of width 2**`pointwidth` nanoseconds. These statistical aggregates currently include the mean, minimum, and maximum of the data and the count of data points composing the window.

        Note that `start` is inclusive, but `end` is exclusive. That is, results will be returned for all windows that start in the interval [start, end). If end < start+2^pointwidth you will not get any results. If start and end are not powers of two, the bottom pointwidth bits will be cleared. Each window will contain statistical summaries of the window. Statistical points with count == 0 will be omitted.

        Parameters
        ----------
        start : int
            The start time in nanoseconds for the range to be queried
        end : int
            The end time in nanoseconds for the range to be queried
        pointwidth : int
            Specify the number of ns between data points (2**pointwidth)
        version : int
            Version of the stream to query

        Yields
        ------
        (StatPoint, int)
            Returns a tuple containing a StatPoint and the stream version


        Notes
        -----
        As the window-width is a power-of-two, it aligns with BTrDB internal tree data structure and is faster to execute than `windows()`.
        """

        ep = self.b.ep
        sps = ep.alignedWindows(self.uu, start, end, pointwidth, version)
        for splist, version in sps:
            for sp in splist:
                yield StatPoint.fromProto(sp), version

    def windows(self, start, end, width, depth=0, version=0):
        # type: (int, int, int, int, int) -> Tuple[StatPoint, int]

        """
        Read arbitrarily-sized windows of data from BTrDB.

        Windows returns arbitrary precision windows from BTrDB. It is slower than AlignedWindows, but still significantly faster than RawValues. Each returned window will be `width` nanoseconds long. `start` is inclusive, but `end` is exclusive (e.g if end < start+width you will get no results). That is, results will be returned for all windows that start at a time less than the end timestamp. If (`end` - `start`) is not a multiple of width, then end will be decreased to the greatest value less than end such that (end - start) is a multiple of `width` (i.e., we set end = start + width * floordiv(end - start, width). The `depth` parameter is an optimization that can be used to speed up queries on fast queries. Each window will be accurate to 2^depth nanoseconds. If depth is zero, the results are accurate to the nanosecond. On a dense stream for large windows, this accuracy may not be required. For example for a window of a day, +- one second may be appropriate, so a depth of 30 can be specified. This is much faster to execute on the database side.


        Parameters
        ----------
        start : int
            The start time in nanoseconds for the range to be queried
        end : int
            The end time in nanoseconds for the range to be queried
        width : int
            Specify the number of ns between data points
        depth : int

        version : int
            Version of the stream to query

        Yields
        ------
        (StatPoint, int)
            Returns a tuple containing a StatPoint and the stream version
        """

        ep = self.b.ep
        sps = ep.windows(self.uu, start, end, width, depth, version)
        for splist, version in sps:
            for sp in splist:
                yield StatPoint.fromProto(sp), version

    def deleteRange(self, start, end):
        # type: (int, int) -> int

        """
        "Delete" all points between [`start`, `end`)

        "Delete" all points between `start` (inclusive) and `end` (exclusive), both in nanoseconds. As BTrDB has persistent multiversioning, the deleted points will still exist as part of an older version of the stream.

        Parameters
        ----------
        start : int
            The start time in nanoseconds for the range to be deleted
        end : int
            The end time in nanoseconds for the range to be deleted

        Returns
        -------
        int
            The version of the new stream created

        """
        ep = self.b.ep
        return ep.deleteRange(self.uu, start, end)

    def nearest(self, time, version, backward):
        # type: (int, int, bool) -> Tuple[RawPoint, int]

        """
        Finds the closest point in the stream to a specified time.

        Return the point nearest to the specified `time` in nanoseconds since Epoch in the stream with `version` while specifying whether to search forward or backward in time. If `backward` is false, the returned point will be >= `time`. If backward is true, the returned point will be < `time`. The version of the stream used to satisfy the query is returned.


        Parameters
        ----------
        time : int
            The time (in nanoseconds since Epoch) to search near
        version : int
            Version of the stream to use in search
        backward : boolean
            True to search backwards from time, else false for forward

        Returns
        -------
        RawPoint
            The point closest to time
        int
            Version of the stream used to satisfy the query


        Raises
        ------
        BTrDBError [401] no such point
            No point satisfies the query in the direction specified

        """

        ep = self.b.ep
        rp, version = ep.nearest(self.uu, time, version, backward)
        return RawPoint.fromProto(rp), version

    """
    This function does not work so is getting commented out. 1/12/18

    def changes(self, fromVersion, toVersion, resolution):
        ep = self.b.ep
        crs = ep.changes(self.uu, fromVersion, toVersion, resolution)
        for crlist, version in crs:
            for cr in crlist:
                yield ChangeRange.fromProto(cr), version
    """

    def flush(self):
        # type: () -> None
        """
        Flush writes the stream buffers out to persistent storage.
        """
        ep = self.b.ep
        ep.flush(self.uu)
