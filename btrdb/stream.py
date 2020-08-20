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

import re
import json
import uuid as uuidlib
from copy import deepcopy
from collections.abc import Sequence

from btrdb.utils.buffer import PointBuffer
from btrdb.point import RawPoint, StatPoint
from btrdb.transformers import StreamSetTransformer
from btrdb.exceptions import BTrDBError, InvalidOperation
from btrdb.utils.timez import currently_as_ns, to_nanoseconds
from btrdb.utils.conversion import AnnotationEncoder, AnnotationDecoder
from btrdb.utils.general import pointwidth as pw


##########################################################################
## Module Variables
##########################################################################

INSERT_BATCH_SIZE = 50000
MINIMUM_TIME = -(16 << 56)
MAXIMUM_TIME = (48 << 56) - 1

try:
    RE_PATTERN = re._pattern_type
except Exception:
    RE_PATTERN = re.Pattern


##########################################################################
## Stream Classes
##########################################################################

class Stream(object):
    """
    An object that represents a specific time series stream in the BTrDB database.

    Parameters
    ----------
        btrdb : BTrDB
            A reference to the BTrDB object connecting this stream back to the physical server.
        uuid : UUID
            The unique UUID identifier for this stream.
        db_values : kwargs
            Framework only initialization arguments.  Not for developer use.

    """

    def __init__(self, btrdb, uuid, **db_values):
        db_args = ('known_to_exist', 'collection', 'tags', 'annotations', 'property_version')
        for key in db_args:
            value = db_values.pop(key, None)
            setattr(self, "_{}".format(key), value)
        if db_values:
            bad_keys = ", ".join(db_values.keys())
            raise TypeError("got unexpected db_values argument(s) '{}'".format(bad_keys))

        self._btrdb = btrdb
        self._uuid = uuid

    def refresh_metadata(self):
        """
        Refreshes the locally cached meta data for a stream

        Queries the BTrDB server for all stream metadata including collection,
        annotation, and tags. This method requires a round trip to the server.
        """

        ep = self._btrdb.ep
        self._collection, self._property_version, self._tags, self._annotations, _ = ep.streamInfo(self._uuid, False, True)
        self._known_to_exist = True

        # deserialize annoation values
        self._annotations = {
            key: json.loads(val, cls=AnnotationDecoder)
            for key, val in self._annotations.items()
        }

    def exists(self):
        """
        Check if stream exists

        Exists returns true if the stream exists. This is essential after using
        StreamFromUUID as the stream may not exist, causing a 404 error on
        later stream operations. Any operation that returns a stream from
        collection and tags will have ensured the stream exists already.

        Parameters
        ----------
        None

        Returns
        -------
        bool
            Indicates whether stream is extant in the BTrDB server.
        """

        if self._known_to_exist:
            return True

        try:
            self.refresh_metadata()
            return True
        except BTrDBError as bte:
            if bte.code == 404:
                return False
            raise bte

    def count(self, start=MINIMUM_TIME, end=MAXIMUM_TIME, pointwidth=62, precise=False, version=0):
        """
        Compute the total number of points in the stream

        Counts the number of points in the specified window and version. By
        default returns the latest total count of points in the stream. This
        helper method sums the counts of all StatPoints returned by
        ``aligned_windows``. Because of this, note that the start and end
        timestamps may be adjusted if they are not powers of 2. For smaller
        windows of time, you may also need to adjust the pointwidth to ensure
        that the count granularity is captured appropriately.

        Alternatively you can set the precise argument to True which will
        give an exact count to the nanosecond but may be slower to execute.

        Parameters
        ----------
        start : int or datetime like object, default: MINIMUM_TIME
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)

        end : int or datetime like object, default: MAXIMUM_TIME
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)

        pointwidth : int, default: 62
            Specify the number of ns between data points (2**pointwidth).  If the value
            is too large for the time window than the next smallest, appropriate
            pointwidth will be used.

        precise : bool, default: False
            Forces the use of a windows query instead of aligned_windows
            to determine exact count down to the nanosecond.  This will
            be some amount slower than the aligned_windows version.

        version : int, default: 0
            Version of the stream to query

        Returns
        -------
        int
            The total number of points in the stream for the specified window.
        """

        if not precise:
            pointwidth = min(pointwidth, pw.from_nanoseconds(to_nanoseconds(end) - to_nanoseconds(start))-1)
            points = self.aligned_windows(start, end, pointwidth, version)
            return  sum([point.count for point, _ in points])

        depth = 0
        width = to_nanoseconds(end) - to_nanoseconds(start)
        points = self.windows(start, end, width, depth, version)
        return  sum([point.count for point, _ in points])


    @property
    def btrdb(self):
        """
        Returns the stream's BTrDB object.

        Parameters
        ----------
        None

        Returns
        -------
        BTrDB
            The BTrDB database object.

        """
        return self._btrdb

    @property
    def uuid(self):
        """
        Returns the stream's UUID. The stream may nor may not exist
        yet, depending on how the stream object was obtained.

        Returns
        -------
        UUID
            The unique identifier of the stream.


        See Also
        --------
        stream.exists()

        """
        return self._uuid

    @property
    def name(self):
        """
        Returns the stream's name which is parsed from the stream tags.  This
        may require a round trip to the server depending on how the stream was
        acquired.

        Returns
        -------
        str
            The name of the stream.

        """
        return self.tags()["name"]

    @property
    def unit(self):
        """
        Returns the stream's unit which is parsed from the stream tags.  This
        may require a round trip to the server depending on how the stream was
        acquired.

        Returns
        -------
        str
            The unit for values of the stream.

        """
        return self.tags()["unit"]

    @property
    def collection(self):
        """
        Returns the collection of the stream. It may require a round trip to the
        server depending on how the stream was acquired.

        Parameters
        ----------
        None

        Returns
        -------
        str
            the collection of the stream

        """
        if self._collection is not None:
            return self._collection

        self.refresh_metadata()
        return self._collection

    def earliest(self, version=0):
        """
        Returns the first point of data in the stream. Returns None if error
        encountered during lookup or no values in stream.

        Parameters
        ----------
        version : int, default: 0
            Specify the version of the stream to query; if zero, queries the latest
            stream state rather than pinning to a version.

        Returns
        -------
        tuple
            The first data point in the stream and the version of the stream
            the value was retrieved at (tuple(RawPoint, int)).

        """
        start = MINIMUM_TIME
        return self.nearest(start, version=version, backward=False)

    def latest(self, version=0):
        """
        Returns last point of data in the stream. Returns None if error
        encountered during lookup or no values in stream.

        Parameters
        ----------
        version : int, default: 0
            Specify the version of the stream to query; if zero, queries the latest
            stream state rather than pinning to a version.

        Returns
        -------
        tuple
            The last data point in the stream and the version of the stream
            the value was retrieved at (tuple(RawPoint, int)).

        """
        start = MAXIMUM_TIME
        return self.nearest(start, version=version, backward=True)

    def current(self, version=0):
        """
        Returns the point that is closest to the current timestamp, e.g. the latest
        point in the stream up until now. Note that no future values will be returned.
        Returns None if errors during lookup or there are no values before now.

        Parameters
        ----------
        version : int, default: 0
            Specify the version of the stream to query; if zero, queries the latest
            stream state rather than pinning to a version.

        Returns
        -------
        tuple
            The last data point in the stream up until now and the version of the stream
            the value was retrieved at (tuple(RawPoint, int)).
        """
        start = currently_as_ns()
        return self.nearest(start, version, backward=True)

    def tags(self, refresh=False):
        """
        Returns the stream's tags.

        Tags returns the tags of the stream. It may require a round trip to the
        server depending on how the stream was acquired.

        Parameters
        ----------
        refresh: bool
            Indicates whether a round trip to the server should be implemented
            regardless of whether there is a local copy.

        Returns
        -------
        dict
            A dictionary containing the tags.

        """
        if refresh or self._tags is None:
            self.refresh_metadata()

        return deepcopy(self._tags)

    def annotations(self, refresh=False):
        """
        Returns a stream's annotations

        Annotations returns the annotations of the stream (and the annotation
        version). It will always require a round trip to the server. If you are
        ok with stale data and want a higher performance version, use
        Stream.CachedAnnotations().

        Do not modify the resulting map.

        Parameters
        ----------
        refresh: bool
            Indicates whether a round trip to the server should be implemented
            regardless of whether there is a local copy.

        Returns
        -------
        tuple
            A tuple containing a dictionary of annotations and an integer representing
            the version of the metadata (tuple(dict, int)).

        """
        if refresh or self._annotations is None:
            self.refresh_metadata()

        return deepcopy(self._annotations), deepcopy(self._property_version)

    def version(self):
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
        return self._btrdb.ep.streamInfo(self._uuid, True, False)[4]

    def insert(self, data, merge='never'):
        """
        Insert new data in the form (time, value) into the series.

        Inserts a list of new (time, value) tuples into the series. The tuples
        in the list need not be sorted by time. If the arrays are larger than
        appropriate, this function will automatically chunk the inserts. As a
        consequence, the insert is not necessarily atomic, but can be used with
        a very large array.

        Parameters
        ----------
        data: list[tuple[int, float]]
            A list of tuples in which each tuple contains a time (int) and
            value (float) for insertion to the database
        merge: str
            A string describing the merge policy. Valid policies are:
              - 'never': the default, no points are merged
              - 'equal': points are deduplicated if the time and value are equal
              - 'retain': if two points have the same timestamp, the old one is kept
              - 'replace': if two points have the same timestamp, the new one is kept

        Returns
        -------
        int
            The version of the stream after inserting new points.

        """
        i = 0
        version = 0
        while i < len(data):
            thisBatch = data[i:i + INSERT_BATCH_SIZE]
            version = self._btrdb.ep.insert(self._uuid, thisBatch, merge)
            i += INSERT_BATCH_SIZE
        return version

    def _update_tags_collection(self, tags, collection):
        tags = self.tags() if tags is None else tags
        collection = self.collection if collection is None else collection
        if collection is None:
            raise ValueError("collection must be provided to update tags or collection")

        self._btrdb.ep.setStreamTags(
            uu=self.uuid,
            expected=self._property_version,
            tags=tags,
            collection=collection
        )

    def _update_annotations(self, annotations, encoder, replace):
        # make a copy of the annotations to prevent accidental mutable object mutation
        serialized = deepcopy(annotations)
        if encoder is not None:
            serialized = {
                k: json.dumps(v, cls=encoder, indent=None, allow_nan=True)
                for k, v in serialized.items()
            }

        removals = []
        if replace:
            removals = [i for i in self._annotations.keys() if i not in annotations.keys()]

        self._btrdb.ep.setStreamAnnotations(
            uu=self.uuid,
            expected=self._property_version,
            changes=serialized,
            removals=removals
        )

    def update(self, tags=None, annotations=None, collection=None, encoder=AnnotationEncoder, replace=False):
        """
        Updates metadata including tags, annotations, and collection as an
        UPSERT operation.

        By default, the update will only affect the keys and values in the
        specified tags and annotations dictionaries, inserting them if they
        don't exist, or updating the value for the key if it does exist. If
        any of the update arguments (i.e. tags, annotations, collection) are
        None, they will remain unchanged in the database.

        To delete either tags or annotations, you must specify exactly which
        keys and values you want set for the field and set `replace=True`. For
        example:

            >>> annotations, _ = stream.anotations()
            >>> del annotations["key_to_delete"]
            >>> stream.update(annotations=annotations, replace=True)

        This ensures that all of the keys and values for the annotations are
        preserved except for the key to be deleted.

        Parameters
        -----------
        tags : dict, optional
            Specify the tag key/value pairs as a dictionary to upsert or
            replace. If None, the tags  will remain unchanged in the database.
        annotations : dict, optional
            Specify the annotations key/value pairs as a dictionary to upsert
            or replace. If None, the annotations will remain unchanged in the
            database.
        collection : str, optional
            Specify a new collection for the stream. If None, the collection
            will remain unchanged.
        encoder : json.JSONEncoder or None
            JSON encoder class to use for annotation serialization. Set to None
            to prevent JSON encoding of the annotations.
        replace : bool, default: False
            Replace all annotations or tags with the specified dictionaries
            instead of performing the normal upsert operation. Specifying True
            is the only way to remove annotation keys.

        Returns
        -------
        int
            The version of the metadata (separate from the version of the data)
            also known as the "property version".

        """
        if tags is None and annotations is None and collection is None:
            raise ValueError("you must supply a tags, annotations, or collection argument")

        if tags is not None and isinstance(tags, dict) is False:
            raise TypeError("tags must be of type dict")

        if annotations is not None and isinstance(annotations, dict) is False:
            raise TypeError("annotations must be of type dict")

        if collection is not None and isinstance(collection, str) is False:
            raise TypeError("collection must be of type string")

        if tags is not None or collection is not None:
            self._update_tags_collection(tags, collection)
            self.refresh_metadata()

        if annotations is not None:
            self._update_annotations(annotations, encoder, replace)
            self.refresh_metadata()

        return self._property_version

    def delete(self, start, end):
        """
        "Delete" all points between [`start`, `end`)

        "Delete" all points between `start` (inclusive) and `end` (exclusive),
        both in nanoseconds. As BTrDB has persistent multiversioning, the
        deleted points will still exist as part of an older version of the
        stream.

        Parameters
        ----------
        start : int or datetime like object
            The start time in nanoseconds for the range to be deleted. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be deleted. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)

        Returns
        -------
        int
            The version of the new stream created

        """
        return self._btrdb.ep.deleteRange(self._uuid, to_nanoseconds(start),
            to_nanoseconds(end))

    def values(self, start, end, version=0):
        """
        Read raw values from BTrDB between time [a, b) in nanoseconds.

        RawValues queries BTrDB for the raw time series data points between
        `start` and `end` time, both in nanoseconds since the Epoch for the
        specified stream `version`.

        Parameters
        ----------
        start : int or datetime like object
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        version: int
            The version of the stream to be queried

        Returns
        ------
        list
            Returns a list of tuples containing a RawPoint and the stream
            version (list(tuple(RawPoint,int))).


        Notes
        -----
        Note that the raw data points are the original values at the sensor's
        native sampling rate (assuming the time series represents measurements
        from a sensor). This is the lowest level of data with the finest time
        granularity. In the tree data structure of BTrDB, this data is stored in
        the vector nodes.

        """
        materialized = []
        start = to_nanoseconds(start)
        end = to_nanoseconds(end)

        point_windows = self._btrdb.ep.rawValues(self._uuid, start, end, version)
        for point_list, version in point_windows:
            for point in point_list:
                materialized.append((RawPoint.from_proto(point), version))
        return materialized

    def aligned_windows(self, start, end, pointwidth, version=0):
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
        start : int or datetime like object
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        pointwidth : int
            Specify the number of ns between data points (2**pointwidth)
        version : int
            Version of the stream to query

        Returns
        -------
        tuple
            Returns a tuple containing windows of data.  Each window is a tuple
            containing data tuples.  Each data tuple contains a StatPoint and
            the stream version.

        Notes
        -----
        As the window-width is a power-of-two, it aligns with BTrDB internal
        tree data structure and is faster to execute than `windows()`.
        """
        materialized = []
        start = to_nanoseconds(start)
        end = to_nanoseconds(end)

        windows = self._btrdb.ep.alignedWindows(self._uuid, start, end, pointwidth, version)
        for stat_points, version in windows:
            for point in stat_points:
                materialized.append((StatPoint.from_proto(point), version))

        return tuple(materialized)

    def windows(self, start, end, width, depth=0, version=0):
        """
        Read arbitrarily-sized windows of data from BTrDB.  StatPoint objects
        will be returned representing the data for each window.

        Parameters
        ----------
        start : int or datetime like object
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        width : int
            The number of nanoseconds in each window, subject to the depth
            parameter.
        depth : int
            The precision of the window duration as a power of 2 in nanoseconds.
            E.g 30 would make the window duration accurate to roughly 1 second
        version : int
            The version of the stream to query.

        Returns
        -------
        tuple
            Returns a tuple containing windows of data.  Each window is a tuple
            containing data tuples.  Each data tuple contains a StatPoint and
            the stream version (tuple(tuple(StatPoint, int), ...)).

        Notes
        -----
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

        """
        materialized = []
        start = to_nanoseconds(start)
        end = to_nanoseconds(end)

        windows = self._btrdb.ep.windows(self._uuid, start, end, width, depth, version)
        for stat_points, version in windows:
            for point in stat_points:
                materialized.append((StatPoint.from_proto(point), version))

        return tuple(materialized)

    def nearest(self, time, version, backward=False):
        """
        Finds the closest point in the stream to a specified time.

        Return the point nearest to the specified `time` in nanoseconds since
        Epoch in the stream with `version` while specifying whether to search
        forward or backward in time. If `backward` is false, the returned point
        will be >= `time`. If backward is true, the returned point will be <
        `time`. The version of the stream used to satisfy the query is returned.

        Parameters
        ----------
        time : int or datetime like object
            The time (in nanoseconds since Epoch) to search near (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        version : int
            Version of the stream to use in search
        backward : boolean
            True to search backwards from time, else false for forward

        Returns
        -------
        tuple
            The closest data point in the stream and the version of the stream
            the value was retrieved at (tuple(RawPoint, int)).

        """

        try:
            rp, version = self._btrdb.ep.nearest(self._uuid,
                to_nanoseconds(time), version, backward)
        except BTrDBError as exc:
            if exc.code != 401:
                raise
            return None

        return RawPoint.from_proto(rp), version


    def obliterate(self):
        """
        Obliterate deletes a stream from the BTrDB server.  An exception will be
        raised if the stream could not be found.

        Raises
        ------
        BTrDBError [404] stream does not exist
            The stream could not be found in BTrDB

        """
        self._btrdb.ep.obliterate(self._uuid)

    def flush(self):
        """
        Flush writes the stream buffers out to persistent storage.

        """
        self._btrdb.ep.flush(self._uuid)

    def __repr__(self):
        return "<Stream collection={} name={}>".format(self.collection,
            self.name)

##########################################################################
## StreamSet  Classes
##########################################################################

class StreamSetBase(Sequence):
    """
    A lighweight wrapper around a list of stream objects
    """

    def __init__(self, streams):
        self._streams = streams
        self._pinned_versions = None

        self.filters = []
        self.pointwidth = None
        self.width = None
        self.depth = None

    @property
    def allow_window(self):
        return not bool(self.pointwidth or (self.width and self.depth))

    def _latest_versions(self):
        return {s.uuid: s.version() for s in self._streams}


    def pin_versions(self, versions=None):
        """
        Saves the stream versions that future materializations should use.  If
        no pin is requested then the first materialization will automatically
        pin the return versions.  Versions can also be supplied through a dict
        object with key:UUID, value:stream.version().

        Parameters
        ----------
        versions : dict[UUID: int]
            A dict containing the stream UUID and version ints as key/values

        Returns
        -------
        StreamSet
            Returns self

        """
        if versions is not None:
            if not isinstance(versions, dict):
                raise TypeError("`versions` argument must be dict")

            for key in versions.keys():
                if not isinstance(key, uuidlib.UUID):
                    raise TypeError("version keys must be type UUID")


        self._pinned_versions = self._latest_versions() if not versions else versions
        return self

    def versions(self):
        """
        Returns a dict of the stream versions.  These versions are the pinned
        values if previously pinned or the latest stream versions if not
        pinned.


        Parameters
        ----------
        None

        Returns
        -------
        dict
            A dict containing the stream UUID and version ints as key/values

        """
        return self._pinned_versions if self._pinned_versions else self._latest_versions()

    def count(self):
        """
        Compute the total number of points in the streams using filters.

        Computes the total number of points across all streams using the
        specified filters. By default, this returns the latest total count of
        all points in the streams. The count is modified by start and end
        filters or by pinning versions.

        Note that this helper method sums the counts of all StatPoints returned
        by ``aligned_windows``. Because of this the start and end timestamps
        may be adjusted if they are not powers of 2. You can also set the
        pointwidth property for smaller windows of time to ensure that the
        count granularity is captured appropriately.

        Parameters
        ----------
        None

        Returns
        -------
        int
            The total number of points in all streams for the specified filters.
        """
        params = self._params_from_filters()
        start = params.get("start", MINIMUM_TIME)
        end = params.get("end", MAXIMUM_TIME)
        versions = self._pinned_versions if self._pinned_versions else {}
        count = 0

        for s in self._streams:
            version = versions.get(s.uuid, 0)
            count += s.count(start, end, version=version)

        return count

    def earliest(self):
        """
        Returns earliest points of data in streams using available filters.

        Parameters
        ----------
        None

        Returns
        -------
        tuple
            The earliest points of data found among all streams

        """
        earliest = []
        params = self._params_from_filters()
        start = params.get("start", MINIMUM_TIME)

        for s in self._streams:
            version = self.versions()[s.uuid]
            point, _ = s.nearest(start, version=version, backward=False)
            earliest.append(point)

        return tuple(earliest)

    def latest(self):
        """
        Returns latest points of data in the streams using available filters.

        Parameters
        ----------
        None

        Returns
        -------
        tuple
            The latest points of data found among all streams

        """
        latest = []
        params = self._params_from_filters()
        start = params.get("end", MAXIMUM_TIME)

        for s in self._streams:
            version = self.versions()[s.uuid]
            point, _ = s.nearest(start, version=version, backward=True)
            latest.append(point)

        return tuple(latest)

    def current(self):
        """
        Returns the points of data in the streams closest to the current timestamp. If
        the current timestamp is outside of the filtered range of data, a ValueError is
        raised.

        Parameters
        ----------
        None

        Returns
        -------
        tuple
            The latest points of data found among all streams
        """
        latest = []
        params = self._params_from_filters()
        now = currently_as_ns()
        end = params.get("end", None)
        start = params.get("start", None)

        if (end is not None and end <= now) or (start is not None and start > now):
            raise ValueError("current time is not included in filtered stream range")

        for s in self._streams:
            version = self.versions()[s.uuid]
            point, _ = s.nearest(now, version=version, backward=True)
            latest.append(point)

        return tuple(latest)

    def filter(self, start=None, end=None, collection=None, name=None, unit=None,
               tags=None, annotations=None):
        """
        Provides a new StreamSet instance containing stored query parameters and
        stream objects that match filtering criteria.

        The collection, name, and unit arguments will be used to select streams
        from the original StreamSet object.  If a string is supplied, then a
        case-insensitive exact match is used to select streams.  Otherwise, you
        may supply a compiled regex pattern that will be used with `re.search`.

        The tags and annotations arguments expect dictionaries for the desired
        key/value pairs.  Any stream in the original instance that has the exact
        key/values will be included in the new StreamSet instance.

        Parameters
        ----------
        start : int or datetime like object
            the inclusive start of the query (see :func:`btrdb.utils.timez.to_nanoseconds`
            for valid input types)
        end : int or datetime like object
            the exclusive end of the query (see :func:`btrdb.utils.timez.to_nanoseconds`
            for valid input types)
        collection : str or regex
            string for exact (case-insensitive) matching of collection when filtering streams
            or a compiled regex expression for re.search of stream collections.
        name : str or regex
            string for exact (case-insensitive) matching of name when filtering streams
            or a compiled regex expression for re.search of stream names.
        unit : str or regex
            string for exact (case-insensitive) matching of unit when filtering streams
            or a compiled regex expression for re.search of stream units.
        tags : dict
            key/value pairs for filtering streams based on tags
        annotations : dict
            key/value pairs for filtering streams based on annotations

        Returns
        -------
        StreamSet
            a new instance cloned from the original with filters applied

        """

        obj = self.clone()
        if start is not None or end is not None:
            obj.filters.append(StreamFilter(start, end))

        # filter by collection
        if collection is not None:
            if isinstance(collection, RE_PATTERN):
                obj._streams = [s for s in obj._streams for m in [collection.search(s.collection)] if m]
            elif isinstance(collection, str):
                obj._streams = [s for s in obj._streams if s.collection.lower() == collection.lower()]
            else:
                raise TypeError("collection must be string or compiled regex")

        # filter by name
        if name is not None:
            if isinstance(name, RE_PATTERN):
                obj._streams = [s for s in obj._streams for m in [name.search(s.name)] if m]
            elif isinstance(name, str):
                obj._streams = [s for s in obj._streams if s.name.lower() == name.lower()]
            else:
                raise TypeError("name must be string or compiled regex")

        # filter by unit
        if unit is not None:
            if isinstance(unit, RE_PATTERN):
                obj._streams = [s for s in obj._streams for m in [unit.search(s.tags()["unit"])] if m]
            elif isinstance(unit, str):
                obj._streams = [s for s in obj._streams if s.tags().get("unit", "").lower() == unit.lower()]
            else:
                raise TypeError("unit must be string or compiled regex")

        # filter by tags
        if tags:
            # filters if the subset of the tags matches the given tags
            obj._streams = [
                s for s in obj._streams
                if tags.items() <= s.tags().items()
            ]

        # filter by annotations
        if annotations:
            # filters if the subset of the annotations matches the given annotations
            obj._streams = [
                s for s in obj._streams
                if annotations.items() <= s.annotations()[0].items()
            ]

        return obj

    def clone(self):
        """
        Returns a deep copy of the object.  Attributes that cannot be copied
        will be referenced to both objects.

        Parameters
        ----------
        None

        Returns
        -------
        StreamSet
            Returns a new copy of the instance

        """
        protected = ('_streams', )
        clone = self.__class__(self._streams)
        for attr, val in self.__dict__.items():
            if attr not in protected:
                setattr(clone, attr, deepcopy(val))
        return clone

    def windows(self, width, depth):
        """
        Stores the request for a windowing operation when the query is
        eventually materialized.

        Parameters
        ----------
        width : int
            The number of nanoseconds to use for each window size.
        depth : int
            The requested accuracy of the data up to 2^depth nanoseconds.  A
            depth of 0 is accurate to the nanosecond.

        Returns
        -------
        StreamSet
            Returns self


        Notes
        -----
        Windows returns arbitrary precision windows from BTrDB. It is slower
        than aligned_windows, but still significantly faster than values. Each
        returned window will be width nanoseconds long. start is inclusive, but
        end is exclusive (e.g if end < start+width you will get no results).
        That is, results will be returned for all windows that start at a time
        less than the end timestamp. If (end - start) is not a multiple of
        width, then end will be decreased to the greatest value less than end
        such that (end - start) is a multiple of width (i.e., we set end = start
        + width * floordiv(end - start, width). The depth parameter is an
        optimization that can be used to speed up queries on fast queries. Each
        window will be accurate to 2^depth nanoseconds. If depth is zero, the
        results are accurate to the nanosecond. On a dense stream for large
        windows, this accuracy may not be required. For example for a window of
        a day, +- one second may be appropriate, so a depth of 30 can be
        specified. This is much faster to execute on the database side.

        """
        if not self.allow_window:
            raise InvalidOperation("A window operation is already requested")

        # TODO: refactor keeping in mind how exception is raised
        self.width = int(width)
        self.depth = int(depth)
        return self

    def aligned_windows(self, pointwidth):
        """
        Stores the request for an aligned windowing operation when the query is
        eventually materialized.

        Parameters
        ----------
        pointwidth : int
            The length of each returned window as computed by 2^pointwidth.

        Returns
        -------
        StreamSet
            Returns self

        Notes
        -----
        `aligned_windows` reads power-of-two aligned windows from BTrDB. It is
        faster than Windows(). Each returned window will be 2^pointwidth
        nanoseconds long, starting at start. Note that start is inclusive, but
        end is exclusive. That is, results will be returned for all windows that
        start in the interval [start, end). If end < start+2^pointwidth you will
        not get any results. If start and end are not powers of two, the bottom
        pointwidth bits will be cleared. Each window will contain statistical
        summaries of the window. Statistical points with count == 0 will be
        omitted.

        """
        if not self.allow_window:
            raise InvalidOperation("A window operation is already requested")

        self.pointwidth = int(pointwidth)
        return self

    def _streamset_data(self, as_iterators=False):
        """
        Private method to return a list of lists representing the data from each
        stream within the StreamSetself.

        Parameters
        ----------
        as_iterators : bool
            Returns each single stream's data as an iterator.  Defaults to False.
        """
        params = self._params_from_filters()
        versions = self.versions()
        data = []

        if self.pointwidth is not None:
            # create list of stream.aligned_windows data
            params.update({"pointwidth": self.pointwidth})
            for s in self._streams:
                params.update({"version": versions[s.uuid]})
                data.append(s.aligned_windows(**params))


        elif self.width is not None and self.depth is not None:
            # create list of stream.windows data (the windows method should
            # prevent the possibility that only one of these is None)
            params.update({"width": self.width, "depth": self.depth})
            for s in self._streams:
                params.update({"version": versions[s.uuid]})
                data.append(s.windows(**params))

        else:
            # create list of stream.values
            data = [s.values(**params) for s in self._streams]

        if as_iterators:
            return [iter(ii) for ii in data]

        return data


    def rows(self):
        """
        Returns a materialized list of tuples where each tuple contains the
        points from each stream at a unique time.  If a stream has no value for that
        time than None is provided instead of a point object.

        Parameters
        ----------
        None

        Returns
        -------
        list
            A list of tuples containing a RawPoint (or StatPoint) and the stream
            version (list(tuple(RawPoint, int))).

        """
        result = []
        streamset_data = self._streamset_data(as_iterators=True)
        buffer = PointBuffer(len(self._streams))

        while True:
            streams_empty = True

            # add next values from streams into buffer
            for stream_idx, data in enumerate(streamset_data):

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
                result.append(tuple(buffer.pop(key)))

            if streams_empty and len(buffer.keys()) == 0:
                break

        return result

    def _params_from_filters(self):
        params = {}
        for filter in self.filters:
            if filter.start is not None:
                params["start"] = filter.start
            if filter.end is not None:
                params["end"] = filter.end
        return params

    def values_iter(self):
        """
        Must return context object which would then close server cursor on __exit__
        """
        raise NotImplementedError()

    def values(self):
        """
        Returns a fully materialized list of lists for the stream values/points
        """
        result = []
        streamset_data = self._streamset_data()
        for stream_data in streamset_data:
            result.append([point[0] for point in stream_data])

        return result

    def __repr__(self):
        token = "stream" if len(self) == 1 else "streams"
        return "<{}({} {})>".format(
            self.__class__.__name__, len(self._streams), token
        )

    def __str__(self):
        token = "stream" if len(self) == 1 else "streams"
        return "{} with {} {}".format(
            self.__class__.__name__, len(self._streams), token
        )

    def __getitem__(self, item):
        if isinstance(item, str):
            item = uuidlib.UUID(item)

        if isinstance(item, uuidlib.UUID):
            for stream in self._streams:
                if stream.uuid == item:
                    return stream
            raise KeyError("Stream with uuid `{}` not found.".format(str(item)))

        return self._streams[item]

    def __len__(self):
        return len(self._streams)


class StreamSet(StreamSetBase, StreamSetTransformer):
    """
    Public class for a collection of streams
    """
    pass


##########################################################################
## Utility Classes
##########################################################################

class StreamFilter(object):
    """
    Object for storing requested filtering options
    """
    def __init__(self, start=None, end=None):
        self.start = to_nanoseconds(start) if start else None
        self.end = to_nanoseconds(end) if end else None

        if self.start is None and self.end is None:
            raise ValueError("A valid `start` or `end` must be supplied")

        if self.start is not None and self.end is not None and self.start >= self.end:
            raise ValueError("`start` must be strictly less than `end` argument")
