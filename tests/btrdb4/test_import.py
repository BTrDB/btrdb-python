# tests.btrdb4.test_import
# Testing for the btrdb4 package.
#
# Author:   PingThings
# Created:  Fri Feb 22 11:48:05 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_import.py [] allen@pingthings.io $

"""
Testing for the btrdb4 package.
"""

##########################################################################
## Imports
##########################################################################

import pytest

##########################################################################
## Import Tests
##########################################################################

class TestPackage(object):

    def test_import(self):
        """
        Assert that `import brdb4` issues warning exception
        """
        with pytest.raises(ImportWarning, match='not available'):
            import btrdb4
