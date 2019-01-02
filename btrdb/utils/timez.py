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

import time
import datetime

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
