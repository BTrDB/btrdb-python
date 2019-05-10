# btrdb.utils.credentials
# Package for interacting with the PredictiveGrid credentials file.
#
# Author:   PingThings
# Created:  Thu May 09 11:36:53 2019 -0400
#
# For license information, see LICENSE.txt
# ID: credentials.py [] allen@pingthings.io $

"""
Package for interacting with the PredictiveGrid credentials file.
"""

##########################################################################
## Imports
##########################################################################

import os
import yaml

from btrdb.exceptions import ProfileNotFound, CredentialsFileNotFound

##########################################################################
## Module Variables
##########################################################################

CONFIG_DIR = ".predictivegrid"
CREDENTIALS_FILENAME = "credentials.yaml"


##########################################################################
## Functions
##########################################################################

def _credentials_path():
    return os.path.join(os.environ["HOME"], CONFIG_DIR, CREDENTIALS_FILENAME)

def load_credentials():
    """
    Returns a dict of the credentials file contents

    Returns
    -------
    dict
        A dictionary of profile connection information
    """
    path = _credentials_path()
    try:
        with open(path, 'r') as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise CredentialsFileNotFound("Cound not find `{}`".format(path)) from exc

def load_profile(name="default"):
    """
    Returns the BTrDB connection information (as dict) for a requested profile
    from the user's credentials file.

    Parameters
    ----------
    name: str
        The name of the profile to retrieve

    Returns
    -------
    dict
        A dictionary of the requested profile's connection information

    Raises
    ------
    CredentialsFileNotFound
        The expected credentials file `~/.predictivegrid/credentials.yaml` could not be found.

    ProfileNotFound
        The requested profile could not be found in the credentials file
    """
    credentials = load_credentials()
    if name not in credentials.keys():
        raise ProfileNotFound("Profile `{}` not found in credentials file.".format(name))
    return credentials[name]["btrdb"]
