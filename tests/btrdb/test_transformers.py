# tests.test_transformers
# Testing package for the btrdb transformers module
#
# Author:   PingThings
# Created:  Wed Jan 02 19:26:20 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_transformers.py [] allen@pingthings.io $

"""
Testing package for the btrdb stream module
"""

##########################################################################
## Imports
##########################################################################
import pytest

from btrdb.stream import StreamSet
from btrdb.transformers import *

##########################################################################
## Transformer Tests
##########################################################################

class TestTransformers(object):

    @pytest.mark.skip
    def test_to_dict(self):
        pass

    @pytest.mark.skip
    def test_to_array(self):
        pass

    @pytest.mark.skip
    def test_to_series(self):
        pass

    @pytest.mark.skip
    def test_to_dataframe(self):
        pass

    @pytest.mark.skip
    def test_to_csv(self):
        pass

    @pytest.mark.skip
    def test_to_table(self):
        pass
