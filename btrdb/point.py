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
    """
    A point of data representing a single position within a time series. Each
    point contains a read-only time and value attribute.

    Parameters
    ----------
    time : int
        The time portion of a single value in the time series in nanoseconds
        since the Unix epoch.
    value : float
        The value of a time series at a single point in time.

    """

    __slots__ = ["_time", "_value"]

    def __init__(self, time, value):
        self._time = time
        self._value = value

    @property
    def time(self):
        """
        The time portion of a data point in nanoseconds since the Unix epoch.
        """
        return self._time

    @property
    def value(self):
        """
        The value portion of a data point as a float object.
        """
        return self._value

    @classmethod
    def from_proto(cls, proto):
        return cls(proto.time, proto.value)

    @classmethod
    def from_proto_list(cls, proto_list):
        return [cls.from_proto(proto) for proto in proto_list]

    def __getitem__(self, index):
        if index == 0:
            return self.time
        elif index == 1:
            return self.value
        else:
            raise IndexError("RawPoint index out of range")

    @staticmethod
    def to_proto(point):
        return btrdb_pb2.RawPoint(time = point[0], value = point[1])

    @staticmethod
    def to_proto_list(points):
        return [RawPoint.to_proto(p) for p in points]

    def __repr__(self):
        return "RawPoint({0}, {1})".format(repr(self.time), repr(self.value))

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not hasattr(other, "time") or not hasattr(other, "value"):
            return False
        return self.time == other.time and self.value == other.value


class StatPoint(object):
    """
    An aggregated data point representing a summary or rollup of one or more
    points of data within a single time series.

    This aggregation point provides for the min, mean, max, count, and standard
    deviation of all data values it spans.  It is returned by windowing queries
    such as `windows` or `aligned_windows`.

    Parameters
    ----------
    time : int
        The time in which the aggregated values represent in nanoseconds since
        the Unix epoch.
    min : float
        The minimum value in a time series within a specified range of time.
    mean : float
        The mean value in a time series within a specified range of time.
    max : float
        The maximum value in a time series within a specified range of time.
    count : float
        The number of values in a time series within a specified range of time.
    stddev : float
        The standard deviation of values in a time series within a specified
        range of time.


    Notes
    -----
    This object may also be treated as a tuple by referencing the values
    according to position.

    .. code-block:: python

        // returns time
        val = point[0]

        // returns standard deviation
        val = point[5]


    """

    __slots__ = ["_time", "_min", "_mean", "_max", "_count", "_stddev"]

    def __init__(self, time, minv, meanv, maxv, count, stddev):
        self._time = time
        self._min = minv
        self._mean = meanv
        self._max = maxv
        self._count = count
        self._stddev = stddev

    @classmethod
    def from_proto(cls, proto):
        return cls(proto.time, proto.min, proto.mean, proto.max, proto.count, proto.stddev)

    @classmethod
    def from_proto_list(cls, proto_list):
        return [cls.from_proto(proto) for proto in proto_list]

    @property
    def time(self):
        """
        The mean value of the time series point within a range of time
        """
        return self._time

    @property
    def min(self):
        """
        The minimum value of the time series within a range of time
        """
        return self._min

    @property
    def mean(self):
        """
        The mean value of the time series within a range of time
        """
        return self._mean

    @property
    def max(self):
        """
        The maximum value of the time series within a range of time
        """
        return self._max

    @property
    def count(self):
        """
        The number of values within the time series for a range of time
        """
        return self._count

    @property
    def stddev(self):
        """
        The standard deviation of the values of a time series within a range of time
        """
        return self._stddev

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
        return "StatPoint({}, {}, {}, {}, {}, {})".format(
            self.time,
            self.min,
            self.mean,
            self.max,
            self.count,
            self.stddev
        )

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        for attr in "time", "min", "mean", "max", "count", "stddev":
            if not hasattr(other, attr):
                return False

        return self.time == other.time and \
            self.min == other.min and \
            self.mean == other.mean and \
            self.max == other.max and \
            self.count == other.count and \
            self.stddev == other.stddev
