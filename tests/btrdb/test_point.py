# tests.test_point
# Testing package for the btrdb point module
#
# Author:   PingThings
# Created:  Wed Jan 02 19:26:20 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_point.py [] allen@pingthings.io $

"""
Testing package for the btrdb point module
"""

##########################################################################
## Imports
##########################################################################

import pytest
from btrdb.point import RawPoint, StatPoint

##########################################################################
## RawPoint Tests
##########################################################################

class TestRawPoint(object):

    def test_create(self):
        """
        Ensure we can create the object
        """
        RawPoint(1, 1)


##########################################################################
## StatPoint Tests
##########################################################################

class TestStatPoint(object):

    def test_create(self):
        """
        Ensure we can create the object
        """
        StatPoint(1, 1, 1, 1, 1, 1)
