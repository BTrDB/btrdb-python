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
            load_credentials_from_file()


class TestLoadProfile(object):

    def setup(self):
        for env in ["BTRDB_ENDPOINTS", "BTRDB_PROFILE", "BTRDB_API_KEY"]:
            try:
                del os.environ[env]
            except KeyError:
                pass

    @patch('btrdb.utils.credentials.load_credentials_from_file')
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
            credentials_by_profile("horse")

    @patch('btrdb.utils.credentials.load_credentials_from_file')
    def test_returns_requested_profile(self, mock_credentials):
        """
        Assert returns correct profile
        """
        mock_credentials.return_value = {
            "default": {"btrdb": {"endpoints": "default"}},
            "duck": {"btrdb": {"endpoints": "duck"}},
        }
        assert credentials_by_profile("duck")["endpoints"] == "duck"

    @patch('btrdb.utils.credentials.load_credentials_from_file')
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
        assert credentials_by_profile()["apikey"] == "333222111"

    @patch('btrdb.utils.credentials.load_credentials_from_file')
    def test_returns_no_default_profile(self, mock_credentials):
        """
        Assert empty credentials are returned if no default profile
        """
        mock_credentials.return_value = {
            "duck": {
                "btrdb" : {
                    "endpoints": "192.168.1.100:4411",
                    "api_key": "111222333",
                },
                "name": "duck"
            },
        }
        assert credentials_by_profile() == {}

class TestCredentials(object):

    def setup(self):
        for env in ["BTRDB_ENDPOINTS", "BTRDB_PROFILE", "BTRDB_API_KEY"]:
            try:
                del os.environ[env]
            except KeyError:
                pass

    @patch('btrdb.utils.credentials.load_credentials_from_file')
    @patch('os.path.exists')
    def test_checks_file_existence(self, mock_exists, mock_load):
        mock_exists.return_value = False
        credentials()
        assert not mock_load.called
