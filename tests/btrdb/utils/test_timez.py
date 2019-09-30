# tests.test_timez
# Testing for the btrdb.timez module
#
# Author:   PingThings
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# For license information, see LICENSE.txt
# ID: test_timez.py [] allen@pingthings.io $

"""
Testing for the btrdb.timez module
"""

##########################################################################
## Imports
##########################################################################

import pytz
import pytest
import datetime
import numpy as np
from freezegun import freeze_time

from btrdb.utils.timez import (currently_as_ns, datetime_to_ns, ns_to_datetime,
                              to_nanoseconds, ns_delta)


##########################################################################
## Initialization Tests
##########################################################################

class TestCurrentlyAsNs(object):

    def test_currently_as_ns(self):
        """
        Assert currently_as_ns returns correct value
        """
        expected = 1514808000000000000
        with freeze_time("2018-01-01 12:00:00 -0000"):
            assert currently_as_ns() == expected


class TestDatetimeToNs(object):

    def test_datetime_to_ns_naive(self):
        """
        Assert datetime_to_ns handles naive datetime
        """
        dt = datetime.datetime(2018,1,1,12)
        localized = pytz.utc.localize(dt)
        expected = int(localized.timestamp() * 1e9)

        assert dt.tzinfo is None
        assert datetime_to_ns(dt) == expected


    def test_datetime_to_ns_aware(self):
        """
        Assert datetime_to_ns handles tz aware datetime
        """
        eastern = pytz.timezone("US/Eastern")
        dt = datetime.datetime(2018,1,1,17, tzinfo=eastern)
        expected = int(dt.astimezone(pytz.utc).timestamp() * 1e9)

        assert dt.tzinfo is not None
        assert datetime_to_ns(dt) == expected


class TestNsToDatetime(object):

    def test_ns_to_datetime_is_utc(self):
        """
        Assert ns_to_datetime returns UTC aware datetime
        """
        dt = datetime.datetime.utcnow()
        ns = int(dt.timestamp() * 1e9)

        assert dt.tzinfo is None
        assert ns_to_datetime(ns).tzinfo == pytz.UTC

    def test_ns_to_datetime_is_correct(self):
        """
        Assert ns_to_datetime returns correct datetime
        """
        val = 1514808000000000000
        expected = datetime.datetime(2018,1,1,12, tzinfo=pytz.UTC)

        assert ns_to_datetime(val) == expected


class TestToNanoseconds(object):

    def test_datetime_to_ns_naive(self):
        """
        Assert to_nanoseconds handles naive datetime
        """
        dt = datetime.datetime(2018,1,1,12)
        localized = pytz.utc.localize(dt)
        expected = int(localized.timestamp() * 1e9)

        assert dt.tzinfo is None
        assert to_nanoseconds(dt) == expected

    def test_datetime_to_ns_aware(self):
        """
        Assert to_nanoseconds handles tz aware datetime
        """
        eastern = pytz.timezone("US/Eastern")
        dt = datetime.datetime(2018,1,1,17, tzinfo=eastern)
        expected = int(dt.astimezone(pytz.utc).timestamp() * 1e9)

        assert dt.tzinfo is not None
        assert to_nanoseconds(dt) == expected

    def test_str(self):
        """
        Assert to_nanoseconds handles RFC3339 format
        """
        dt = datetime.datetime(2018,1,1,12, tzinfo=pytz.utc)
        expected = int(dt.timestamp() * 1e9)

        dt_str = "2018-1-1 12:00:00.0-0000"
        assert dt.tzinfo is not None
        assert to_nanoseconds(dt_str) == expected

        dt_str = "2018-1-1 7:00:00.0-0500"
        dt = datetime.datetime(2018,1,1,12, tzinfo=pytz.timezone("US/Eastern"))
        assert dt.tzinfo is not None
        assert to_nanoseconds(dt_str) == expected

        dt_str = "2018-01-15 07:32:49"
        dt = datetime.datetime(2018,1,15,7,32,49, tzinfo=pytz.utc)
        expected = int(dt.timestamp() * 1e9)
        assert to_nanoseconds(dt_str) == expected

    def test_str_midnight(self):
        """
        Test parse a date at midnight
        """
        expected = datetime.datetime(2019, 4, 7, tzinfo=pytz.utc)
        expected = int(expected.timestamp() * 1e9)
        assert to_nanoseconds("2019-04-07") == expected

    def test_str_raise_valueerror(self):
        """
        Assert to_nanoseconds raises on invalid str
        """
        dt_str = "01 Jan 2018 12:00:00 -0000"
        with pytest.raises(ValueError, match="RFC3339") as exc:
            to_nanoseconds(dt_str)

    def test_int(self):
        """
        Assert to_nanoseconds handles int
        """
        assert 42 == to_nanoseconds(42)

    def test_float(self):
        """
        Assert to_nanoseconds handles float
        """
        assert 42 == to_nanoseconds(42.0)

    def test_float_raise_valueerror(self):
        """
        Assert to_nanoseconds raises on invalid float
        """
        with pytest.raises(ValueError) as exc:
            to_nanoseconds(42.5)
        assert "can only convert whole numbers" in str(exc)

    def test_float(self):
        """
        Assert to_nanoseconds handles float
        """
        dt = datetime.datetime(2018,1,1,12, tzinfo=pytz.utc)
        expected = int(dt.timestamp() * 1e9)

        dt64 = np.datetime64('2018-01-01T12:00')
        assert expected == to_nanoseconds(dt64)


class TestToNsDelta(object):

    def test_ns_delta(self):
        """
        Assert ns_delta converts inputs properly
        """
        val = ns_delta(1,2,1,3,1,23,1)
        assert val == 93663001023001

    def test_ns_delta_precision(self):
        """
        Assert ns_delta deals with real inputs
        """
        val = ns_delta(days=365, minutes=0.5, nanoseconds=1)
        assert val == int(1e9 * 60 * 60 * 24 * 365) + int(1e9 * 30) + 1

    def test_returns_int(self):
        """
        Assert ns_delta returns int if floats used for arguments
        """
        val = ns_delta(1.0,1.0,1.0,1.0,1.0,1.0,1)
        assert val == int(1 + 1e3 + 1e6 + 1e9 + (1e9 * 60)  + (1e9 * 60 * 60) + \
            (1e9 * 60 * 60 * 24) )
        assert isinstance(val, int)
