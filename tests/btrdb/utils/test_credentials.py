# tests.test_credentials
# Testing for the btrdb.utils.credentials module
#
# Author:   PingThings
# Created:  Thu May 09 14:29:21 2019 -0400
#
# For license information, see LICENSE.txt
# ID: test_credentials.py [] allen@pingthings.io $

"""
Testing for the btrdb.utils.credentials module
"""

##########################################################################
## Imports
##########################################################################

import pytest
from unittest.mock import patch

from btrdb.exceptions import *
from btrdb.utils.credentials import *


##########################################################################
## Tests
##########################################################################


class TestLoadCredentials(object):

    @patch('builtins.open')
    def test_raises_err_if_credentials_not_found(self, mock_open):
        """
        Assert CredentialsFileNotFound is raised if credentials.yaml is not found
        """
        mock_open.side_effect = FileNotFoundError("foo")
        with pytest.raises(CredentialsFileNotFound):
            load_credentials()


class TestLoadProfile(object):

    @patch('btrdb.utils.credentials.load_credentials')
    def test_raises_err_if_profile_not_found(self, mock_credentials):
        """
        Assert ProfileNotFound is raised if profile is asked for but not found
        """
        mock_credentials.return_value = {
            "duck": {
                "btrdb" : {
                    "endpoints": "192.168.1.100:4410",
                    "api_key": "111222333",
                }
            }
        }
        with pytest.raises(ProfileNotFound):
            load_profile("horse")

    @patch('btrdb.utils.credentials.load_credentials')
    def test_returns_requested_profile(self, mock_credentials):
        """
        Assert returns correct profile
        """
        mock_credentials.return_value = {
            "default": {"btrdb": {"endpoints": "default"}},
            "duck": {"btrdb": {"endpoints": "duck"}},
        }
        assert load_profile("duck")["endpoints"] == "duck"

    @patch('btrdb.utils.credentials.load_credentials')
    def test_returns_default_profile(self, mock_credentials):
        """
        Assert default profile is returned
        """
        mock_credentials.return_value = {
            "duck": {
                "btrdb" : {
                    "endpoints": "192.168.1.100:4411",
                    "api_key": "111222333",
                },
                "name": "duck"
            },
            "default": {
                "btrdb" : {
                    "endpoints": "192.168.1.222:4411",
                    "api_key": "333222111",
                },
                "name": "default"
            },
        }
        assert load_profile()["api_key"] == "333222111"
