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
                              to_nanoseconds)


##########################################################################
## Initialization Tests
##########################################################################

class TestCurrentlyAsNs(object):

    def test_currently_as_ns(self):
        """
        Assert currently_as_ns returns correct value
        """
        expected = int(datetime.datetime(2018,1,1,12).timestamp() * 1e9)
        with freeze_time("2018-01-01 12:00:00"):
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
        input = 1514808000000000000
        expected = datetime.datetime(2018,1,1,12, tzinfo=pytz.UTC)

        assert ns_to_datetime(input) == expected


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
        Assert to_nanoseconds handles str
        """
        dt = datetime.datetime(2018,1,1,12, tzinfo=pytz.utc)
        expected = int(dt.timestamp() * 1e9)

        dt_str = "01 Jan 2018 12:00:00 -0000"
        assert dt.tzinfo is not None
        assert to_nanoseconds(dt_str) == expected

        dt_str = "01 Jan 2018 7:00:00 -0500"
        dt = datetime.datetime(2018,1,1,12, tzinfo=pytz.timezone("US/Eastern"))
        assert dt.tzinfo is not None
        assert to_nanoseconds(dt_str) == expected

    def test_str_raise_valueerror(self):
        """
        Assert to_nanoseconds raises on invalid str
        """
        dt_str = "2019-01-08T21:41:30Z"
        with pytest.raises(ValueError) as exc:
            to_nanoseconds(dt_str)
        assert "must conform to RFC 2822" in str(exc)

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
