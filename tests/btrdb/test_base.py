# tests.test_base
# Testing package for the btrdb library.
#
# Author:   Allen Leis <allen@pingthings.io>
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: test_base.py [] allen@pingthings.io $

"""
Testing package for the btrdb database library.
"""

##########################################################################
## Imports
##########################################################################

import pytest

##########################################################################
## Test Constants
##########################################################################

EXPECTED_VERSION = "4.0"


##########################################################################
## Initialization Tests
##########################################################################

class TestBasic(object):

    def test_sanity(self):
        """
        Test that tests work by confirming 7-3 = 4
        """
        assert 7-3 == 4, "The world went wrong!!"

    def test_import(self):
        """
        Assert that the btrdb package can be imported.
        """
        import btrdb

    def test_version(self):
        """
        Assert that the test version matches the library version.
        """
        import btrdb
        assert btrdb.__version__ == EXPECTED_VERSION
