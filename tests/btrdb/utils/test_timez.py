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
from freezegun import freeze_time

from btrdb.utils.timez import currently_as_ns, datetime_to_ns, ns_to_datetime


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
