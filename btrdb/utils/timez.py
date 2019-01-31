# btrdb.utils.timez
# Conversion utilities for btrdb
#
# Author:   PingThings
# Created:  Wed Jan 02 17:00:49 2019 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: timez.py [] allen@pingthings.io $

"""
Time related utilities
"""

##########################################################################
## Imports
##########################################################################

import datetime
from email.utils import parsedate_to_datetime

import pytz

##########################################################################
## Functions
##########################################################################

def currently_as_ns():
    """
    Returns the current UTC time as nanoseconds since epoch
    """
    dt = datetime.datetime.utcnow()
    return int(dt.timestamp() * 1e9)


def ns_to_datetime(ns):
    """
    Converts nanoseconds to a datetime object (UTC)

    Parameters
    ----------
    ns : int
        nanoseconds since epoch

    Returns
    -------
    nanoseconds since epoch as a datetime object : datetime

    """
    dt = datetime.datetime.utcfromtimestamp(ns / 1e9)
    return dt.replace(tzinfo=pytz.utc)


def datetime_to_ns(dt):
    """
    Converts a datetime object to nanoseconds since epoch.  If a timezone aware
    object is received then it will be converted to UTC.

    Parameters
    ----------
    dt : datetime

    Returns
    -------
    nanoseconds : int

    """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        aware = pytz.utc.localize(dt)
    else:
        aware = dt

    dt_utc = aware.astimezone(pytz.utc)
    return int(dt_utc.timestamp() * 1e9)

def to_nanoseconds(val):
    """
    Converts datetime, datetime64, float, str (RFC 2822) to nanoseconds.  If a
    datetime-like object is received then nanoseconds since epoch is returned.

    Parameters
    ----------
    val : datetime, datetime64, float, str
        an object to convert to nanoseconds

    Returns
    -------
    object converted to nanoseconds : int
    """
    if val is None or isinstance(val, int):
        return val

    try:
        import numpy as np
        if isinstance(val, np.datetime64):
            val = val.astype(datetime.datetime)
    except ImportError:
        pass

    if isinstance(val, str):
        # handle int as string
        if val.isdigit():
            return int(val)

        # handle datetime as string
        try:
            val = parsedate_to_datetime(val)
        except TypeError:
            raise ValueError("string arguments must conform to RFC 2822")

    if isinstance(val, datetime.datetime):
        return datetime_to_ns(val)

    if isinstance(val, float):
        if val.is_integer():
            return int(val)
        else:
            raise ValueError("can only convert whole numbers to nanoseconds")

    raise TypeError("only int, float, str, datetime, and datetime64 are allowed")


def ns_delta(days=0, hours=0, minutes=0, seconds=0, milliseconds=0, \
             microseconds=0, nanoseconds=0):
    """
    Similar to `timedelta`, ns_delta represents a span of time but as
    the total number of nanoseconds.

    Parameters
    ----------
    days : int
        days (as 24 hours) to convert to nanoseconds
    hours : int
        hours to convert to nanoseconds
    minutes : int
        minutes to convert to nanoseconds
    seconds : int
        seconds to convert to nanoseconds
    milliseconds : int
        milliseconds to convert to nanoseconds
    microseconds : int
        microseconds to convert to nanoseconds
    nanoseconds : int
        nanoseconds to add to the time span

    Returns
    -------
    amount of time in nanoseconds : int

    """
    MICROSECOND = 1000
    MILLESECOND = MICROSECOND * 1000
    SECOND = MILLESECOND * 1000
    MINUTE = SECOND * 60
    HOUR = MINUTE * 60
    DAY = HOUR * 24

    nanoseconds += days * DAY
    nanoseconds += hours * HOUR
    nanoseconds += minutes * MINUTE
    nanoseconds += seconds * SECOND
    nanoseconds += milliseconds * MILLESECOND
    nanoseconds += microseconds * MICROSECOND

    return int(nanoseconds)
