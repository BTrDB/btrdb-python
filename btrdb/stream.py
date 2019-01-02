# btrdb.stream
# Module for Stream and related classes
#
# Author:   PingThings
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# For license information, see LICENSE.txt
# ID: stream.py [] allen@pingthings.io $

"""
Module for Stream and related classes
"""

##########################################################################
## Imports
##########################################################################

import time
from copy import deepcopy

from btrdb.point import RawPoint, StatPoint
from btrdb.transformers import StreamTransformer
from btrdb.utils.buffer import PointBuffer
from btrdb.utils.timez import currently_as_ns
from btrdb.exceptions import BTrDBError


##########################################################################
## Stream Classes
##########################################################################

class Stream(object):
    def __init__(self, btrdb, uuid, knownToExist=False, collection=None, tags=None, annotations=None, propertyVersion=None):
        self.b = btrdb
        self.uu = uuid
        self.knownToExist = knownToExist

        # Some cacheable attributes
        self.cachedTags = tags
        self.cachedAnnotations = annotations
        self.cachedAnnotationVersion = propertyVersion
        self.cachedCollection = collection

    def refresh_metadata(self):
        # type: () -> ()
        """
        Refreshes the locally cached meta data for a stream

        Queries the BTrDB server for all stream metadata including collection,
        annotation, and tags. This method requires a round trip to the server.

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
            self.refresh_metadata()
            return True
        except BTrDBError as bte:
            if bte.code == 404:
                return False
            raise

    def uuid(self):
        # type: () -> UUID
        """
        Returns the stream's UUID.

        This method returns the stream's UUID. The stream may nor may not exist
        yet, depending on how the stream object was obtained.

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

        Tags returns the tags of the stream. It may require a round trip to the
        server depending on how the stream was acquired. Do not modify the
        resulting map as it is a reference to the internal stream state.

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

        self.refresh_metadata()
        return self.cachedTags

    def annotations(self):
        # type: () -> Tuple[Dict[str, str], int]
        """

        Returns a stream's annotations

        Annotations returns the annotations of the stream (and the annotation
        version). It will always require a round trip to the server. If you are
        ok with stale data and want a higher performance version, use
        Stream.CachedAnnotations().

        Do not modify the resulting map.


        Parameters
        ----------
        None

        Returns
        -------
        Tuple[Dict[str, str], int]
            A tuple containing a dictionary of annotations and an integer representing
            the version of those annotations.

        """
        self.refresh_metadata()
        return self.cachedAnnotations, self.cachedAnnotationVersion

    def cachedAnnotations(self):
        # type: () -> Tuple[Dict[str, str], int]
        """

        CachedAnnotations returns the annotations of the stream, reusing previous
        results if available, otherwise fetching from the server.

        Parameters
        ----------
        None

        Returns
        -------
        Tuple[Dict[str, str], int]
            A tuple containing a dictionary of annotations and an integer
            representing the version of those annotations.

        """
        if self.cachedAnnotations is None:
            self.refresh_metadata()
        return self.cachedAnnotations, self.cachedAnnotationVersion

    def collection(self):
        # type: () -> str
        """
        Returns the collection of the stream. It may require a round trip to the
        server depending on how the stream was acquired.

        Returns
        -------
        str
            the collection of the stream

        """

        if self.cachedCollection is not None:
            return self.cachedCollection

        self.refresh_metadata()
        return self.cachedCollection

    def version(self):
        # type: () -> int
        """
        Returns the current data version of the stream.

        Version returns the current data version of the stream. This is not
        cached, it queries each time. Take care that you do not intorduce races
        in your code by assuming this function will always return the same vaue

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

        Inserts a list of new (time, value) tuples into the series. The tuples
        in the list need not be sorted by time. If the arrays are larger than
        appropriate, this function will automatically chunk the inserts. As a
        consequence, the insert is not necessarily atomic, but can be used with
        a very large array.

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

    def values(self, start, end, version=0):
        # type: (int, int, int) -> Tuple[RawPoint, int]

        """
        Read raw values from BTrDB between time [a, b) in nanoseconds.

        RawValues queries BTrDB for the raw time series data points between
        `start` and `end` time, both in nanoseconds since the Epoch for the
        specified stream `version`.

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
        Note that the raw data points are the original values at the sensor's
        native sampling rate (assuming the time series represents measurements
        from a sensor). This is the lowest level of data with the finest time
        granularity. In the tree data structure of BTrDB, this data is stored in
        the vector nodes.

        """
        if isinstance(start, float):
            if start.is_integer():
                start = int(start)
            else:
                raise Exception("start argument must be a whole number")

        if isinstance(end, float):
            if end.is_integer():
                end = int(end)
            else:
                raise Exception("end argument must be a whole number")

        ep = self.b.ep
        rps = ep.rawValues(self.uu, start, end, version)
        for rplist, version in rps:
            for rp in rplist:
                yield RawPoint.fromProto(rp), version

    def aligned_windows(self, start, end, pointwidth, version=0):
        # type: (int, int, int, int) -> Tuple[StatPoint, int]

        """
        Read statistical aggregates of windows of data from BTrDB.

        Query BTrDB for aggregates (or roll ups or windows) of the time series
        with `version` between time `start` (inclusive) and `end` (exclusive) in
        nanoseconds. Each point returned is a statistical aggregate of all the
        raw data within a window of width 2**`pointwidth` nanoseconds. These
        statistical aggregates currently include the mean, minimum, and maximum
        of the data and the count of data points composing the window.

        Note that `start` is inclusive, but `end` is exclusive. That is, results
        will be returned for all windows that start in the interval [start, end).
        If end < start+2^pointwidth you will not get any results. If start and
        end are not powers of two, the bottom pointwidth bits will be cleared.
        Each window will contain statistical summaries of the window.
        Statistical points with count == 0 will be omitted.

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
        As the window-width is a power-of-two, it aligns with BTrDB internal
        tree data structure and is faster to execute than `windows()`.
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

        Windows returns arbitrary precision windows from BTrDB. It is slower
        than AlignedWindows, but still significantly faster than RawValues. Each
        returned window will be `width` nanoseconds long. `start` is inclusive,
        but `end` is exclusive (e.g if end < start+width you will get no
        results). That is, results will be returned for all windows that start
        at a time less than the end timestamp. If (`end` - `start`) is not a
        multiple of width, then end will be decreased to the greatest value less
        than end such that (end - start) is a multiple of `width` (i.e., we set
        end = start + width * floordiv(end - start, width). The `depth`
        parameter is an optimization that can be used to speed up queries on
        fast queries. Each window will be accurate to 2^depth nanoseconds. If
        depth is zero, the results are accurate to the nanosecond. On a dense
        stream for large windows, this accuracy may not be required. For example
        for a window of a day, +- one second may be appropriate, so a depth of
        30 can be specified. This is much faster to execute on the database
        side.


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

    def delete_range(self, start, end):
        # type: (int, int) -> int

        """
        "Delete" all points between [`start`, `end`)

        "Delete" all points between `start` (inclusive) and `end` (exclusive),
        both in nanoseconds. As BTrDB has persistent multiversioning, the
        deleted points will still exist as part of an older version of the
        stream.

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

        Return the point nearest to the specified `time` in nanoseconds since
        Epoch in the stream with `version` while specifying whether to search
        forward or backward in time. If `backward` is false, the returned point
        will be >= `time`. If backward is true, the returned point will be <
        `time`. The version of the stream used to satisfy the query is returned.


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


##########################################################################
## StreamSet  Classes
##########################################################################

class StreamSetBase(object):

    """
    A lighweight wrapper around a list of stream objects
    """

    def __init__(self, streams):
        self._streams = streams
        self._pinned_versions = None

        self.filters = []
        self.point_width = None
        self.width = None
        self.depth = None

    @property
    def allow_window(self):
        return self.point_width or self.width or self.depth

    def _latest_versions(self):
        return {s.uuid(): s.version() for s in self._streams}


    def pin_versions(self, versions=None):
        """
        Saves the stream versions that future materializations should use.  If
        no pin is requested then the first materialization will automatically
        pin the return versions.  Versions can also be supplied through a dict
        object with key:UUID, value:stream.version().
        """
        self._pinned_versions = self._latest_versions() if not versions else versions
        return self

    def versions(self):
        """
        """
        return self._pinned_versions if self._pinned_versions else self._latest_versions()

    def earliest(self):
        """
        Returns earliest timestamp (ns) of data in streams using available
        filters.
        """
        earliest = None
        params = self._params_from_filters()
        start = params.get("start", 0)

        for s in self._streams:
            version = self.versions()[s.uuid()]
            try:
                s_start = s.nearest(start, version=version, backward=False)
            except Exception:
                continue
            if earliest is None or s_start[0].time < earliest:
                earliest = s_start[0].time

        return earliest

    def latest(self):
        """
        Returns latest timestamp (ns) of data in streams using available
        filters.  Note that this method will return None if no
        end filter is provided and point cannot be found that is less than the
        current date/time.
        """
        latest = None
        params = self._params_from_filters()
        start = params.get("end", currently_as_ns())

        for s in self._streams:
            version = self.versions()[s.uuid()]
            try:
                s_latest = s.nearest(start, version=version, backward=True)
            except Exception:
                continue
            if latest is None or s_latest[0].time > latest:
                latest = s_latest[0].time

        return latest

    def filter(self, start=None, end=None):
        obj = self.clone()
        obj.filters.append(StreamFilter(start, end))
        return obj

    def clone(self):
        """
        Returns a deep copy of the object.  Attributes that cannot be copied
        will be referenced to both objects.
        """
        protected = ('_streams', )
        clone = self.__class__(self._streams)
        for attr, val in self.__dict__.items():
            if attr not in protected:
                setattr(clone, attr, deepcopy(val))
        return clone

    def windows(self, width, depth):
        if not self.allow_window:
            raise Exception("A window operation is already requested")

        self.allow_window = False
        self.width = width
        self.depth = depth
        return self

    def aligned_windows(self, pointwidth):
        if not self.allow_window:
            raise Exception("A window operation is already requested")

        self.allow_window = False
        self.pointwidth = pointwidth
        return self

    def rows(self):
        """
        Return iterator of tuples containing stream values at each time
        """
        params = self._params_from_filters()
        result_iterables = [s.values(**params) for s in self._streams]


        buffer = PointBuffer(len(self._streams))

        while True:
            streams_empty = True

            # add next values from streams into buffer
            for stream_idx, data in enumerate(result_iterables):
                if buffer.active[stream_idx]:
                    try:
                        point, _ = next(data)
                        buffer.add_point(stream_idx, point)
                        streams_empty = False
                    except StopIteration:
                        buffer.deactivate(stream_idx)
                        continue

            key = buffer.next_key_ready()
            if key:
                yield tuple(buffer.pop(key))

            if streams_empty and len(buffer.keys()) == 0:
                break


    def _params_from_filters(self):
        params = {}
        for filter in self.filters:
            if filter.start is not None:
                params["start"] = filter.start
            if filter.end is not None:
                params["end"] = filter.end
        return params

    @property
    def values_iterator(self):
        """
        Must return context object which would then close server cursor on __exit__
        """
        raise NotImplementedError()

    @property
    def values(self):
        """
        Returns a fully materialized list of lists for the stream values/points
        """
        result = []
        params = self._params_from_filters()
        stream_output_iterables = [s.values(**params) for s in self._streams]

        for stream_output in stream_output_iterables:
            result.append([point[0] for point in stream_output])

        return result


class StreamSet(StreamSetBase, StreamTransformer):
    """
    Public class for a collection of streams
    """
    pass


##########################################################################
## Utility Classes
##########################################################################

class StreamFilter(object):
    """
    Placeholder for future filtering options? tags? annotations?
    """
    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end
        return
