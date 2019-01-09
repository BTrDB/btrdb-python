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
    dt = datetime.datetime.utcnow()
    return int(dt.timestamp() * 1e9)


def ns_to_datetime(ns):
    dt = datetime.datetime.utcfromtimestamp(ns / 1e9)
    return dt.replace(tzinfo=pytz.utc)


def datetime_to_ns(dt):
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        aware = pytz.utc.localize(dt)
    else:
        aware = dt

    dt_utc = aware.astimezone(pytz.utc)
    return int(dt_utc.timestamp() * 1e9)

def to_nanoseconds(val):
    """
    Converts datetime, datetime64, float, str to nanoseconds
    """
    if val is None or isinstance(val, int):
        return val

    try:
        import numpy as np
        if isinstance(val, np.datetime64):
            val = val.astype(datetime.datetime)
    except ModuleNotFoundError:
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
