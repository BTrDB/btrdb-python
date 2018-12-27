# btrdb.collection
# Stream collection objects
#
# Author:   Allen Leis <allen@pingthings.io>
# Created:  Tue Dec 18 14:50:05 2018 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: collection.py [] allen@pingthings.io $

"""
Stream collection objects
"""

##########################################################################
## Imports
##########################################################################

from copy import deepcopy
import collections
from collections import defaultdict

from btrdb.stream import RawPoint
from btrdb.transformers import StreamTransformer

##########################################################################
## Classes
##########################################################################

class PointBuffer(defaultdict):

    def __init__(self, length, *args, **kwargs):
        super().__init__(lambda: [None] * length, *args, **kwargs)
        self.num_streams = length
        self.active = [True] * length
        self.last_known_time = [None] * length


    def add_point(self, stream_index, point):
        if not isinstance(point, RawPoint):
            raise TypeError("Expected RawPoint instance")

        self.last_known_time[stream_index] = max(
            (self.last_known_time[stream_index] or -1), point.time
        )

        self[point.time][stream_index] = point

    def earliest(self):
        if len(self.keys()) == 0:
            return None
        return min(self.keys())

    def deactivate(self, stream_index):
        self.active[stream_index] = False

    def is_ready(self, key):
        """
        Returns bool indicating whether a given key has all points from active
        (at the time) streams.
        """
        values = self[key]

        # check each stream value to see if its valid/ready
        for idx, latest_time in enumerate(self.last_known_time):

            # return False if stream is active and we are waiting on val
            if self.active[idx] and key > latest_time:
                return False

        return True

    def next_key_ready(self):
        keys = list(self.keys())
        keys.sort()
        for key in keys:
            if self.is_ready(key):
                return key

        return None



class StreamCollectionBase(object):

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
        return {versions[s.uuid()]: s.version() for s in self._streams}


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
        Returns earliest timestamp (ns) of data in streams
        """
        pass

    def latest(self):
        """
        Returns latest timestamp (ns) of data in streams
        """
        pass

    def filter(self, start=None, end=None):
        # collection = deepcopy(self)
        # collection.filters.append(StreamFilter(start, end))
        # return collection
        self.filters.append(StreamFilter(start, end))
        return self

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


# class StreamSet
class StreamCollection(StreamCollectionBase, StreamTransformer):
    """

    """
    pass



class StreamFilter(object):
    """
    Placeholder for future filtering options? tags? annotations?
    """
    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end
        return
