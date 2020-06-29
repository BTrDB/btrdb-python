# btrdb.utils.general
# General utilities for btrdb bindings
#
# Author:   PingThings
# Created:  Wed Jan 02 17:00:49 2019 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: general.py [] allen@pingthings.io $

"""
General utilities for btrdb bindings
"""

##########################################################################
## Functions
##########################################################################


def unpack_stream_descriptor(desc):
    """
    Returns dicts for tags and annotations found in supplied stream
    """
    tags = {}
    for tag in desc.tags:
        tags[tag.key] = tag.val.value

    anns = {}
    for ann in desc.annotations:
        anns[ann.key] = ann.val.value
    return tags, anns


##########################################################################
## Pointwidth Helpers
##########################################################################

class pointwidth(object):
    """
    A representation of a period of time described by the BTrDB tree (and used in
    aligned_windows queries). The pointwidth allows users to traverse to different
    levels of the BTrDB tree and to select StatPoints directly, vastly improving the
    performance of queries. However, because the real duration of the pointwidth is
    defined in powers of 2 (e.g. 2**pointwidth nanoseconds), the durations do not map
    to human time periods (e.g. weeks or hours).

    Parameters
    ----------
    p : int
        cast an integer to the specified pointwidth
    """

    @classmethod
    def from_timedelta(cls, delta):
        """
        Returns the closest pointwidth for the given timedelta without going over the
        specified duration. Because pointwidths are in powers of 2, be sure to check
        that the returned real duration is sufficient.
        """
        return cls.from_nanoseconds(int(delta.total_seconds()*1e9))

    @classmethod
    def from_nanoseconds(cls, nsec):
        """
        Returns the closest pointwidth for the given number of nanoseconds without going
        over the specified duration. Because pointwidths are in powers of 2, be sure to
        check that the returned real duration is sufficient.
        """
        for pos in range(62):
            nsec = nsec >> 1
            if nsec == 0:
                break
        return cls(pos)

    def __init__(self, p):
        self._pointwidth = int(p)

    @property
    def nanoseconds(self):
        return 2**self._pointwidth

    @property
    def microseconds(self):
        return self.nanoseconds / 1e3

    @property
    def milliseconds(self):
        return self.nanoseconds / 1e6

    @property
    def seconds(self):
        return self.nanoseconds / 1e9

    @property
    def minutes(self):
        return self.nanoseconds / 6e10

    @property
    def hours(self):
        return self.nanoseconds / 3.6e12

    @property
    def days(self):
        return self.nanoseconds / 8.64e13

    @property
    def weeks(self):
        return self.nanoseconds / 6.048e14

    @property
    def months(self):
        return self.nanoseconds / 2.628e15

    @property
    def years(self):
        return self.nanoseconds / 3.154e16

    def decr(self):
        return pointwidth(self-1)

    def incr(self):
        return pointwidth(self+1)

    def __int__(self):
        return self._pointwidth

    def __eq__(self, other):
        return int(self) == int(other)

    def __lt__(self, other):
        return int(self) < int(other)

    def __le__(self, other):
        return int(self) <= int(other)

    def __gt__(self, other):
        return int(self) > int(other)

    def __ge__(self, other):
        return int(self) >= int(other)

    def __add__(self, other):
        return pointwidth(int(self)+int(other))

    def __sub__(self, other):
        return pointwidth(int(self)-int(other))

    def __str__(self):
        """
        Returns the pointwidth to the closest unit
        """
        if self >= 55:
            return "{:0.2f} years".format(self.years)

        if self >= 52:
            return "{:0.2f} months".format(self.months)

        if self >= 50:
            return "{:0.2f} weeks".format(self.weeks)

        if self >= 47:
            return "{:0.2f} days".format(self.days)

        if self >= 42:
            return "{:0.2f} hours".format(self.hours)

        if self >= 36:
            return "{:0.2f} minutes".format(self.minutes)

        if self >= 30:
            return "{:0.2f} seconds".format(self.seconds)

        if self >= 20:
            return "{:0.2f} milliseconds".format(self.milliseconds)

        if self >= 10:
            return "{:0.2f} microseconds".format(self.microseconds)

        return "{:0.0f} nanoseconds".format(self.nanoseconds)

    def __repr__(self):
        return "<pointwidth {}>".format(int(self))