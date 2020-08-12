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
from functools import wraps

from btrdb.exceptions import ProfileNotFound, CredentialsFileNotFound

##########################################################################
## Module Variables
##########################################################################

CONFIG_DIR = ".predictivegrid"
CREDENTIALS_FILENAME = "credentials.yaml"
CREDENTIALS_PATH = os.path.join(os.path.expanduser("~"), CONFIG_DIR, CREDENTIALS_FILENAME)

##########################################################################
## Functions
##########################################################################

def filter_none(f):
    """
    decorator for removing dict items with None as value
    """
    @wraps(f)
    def inner(*args, **kwargs):
        return  { k: v for k, v in f(*args, **kwargs).items() if v is not None }
    return inner

def load_credentials_from_file():
    """
    Returns a dict of the credentials file contents

    Returns
    -------
    dict
        A dictionary of profile connection information
    """
    try:
        with open(CREDENTIALS_PATH, 'r') as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise CredentialsFileNotFound("Cound not find `{}`".format(CREDENTIALS_PATH)) from exc

@filter_none
def credentials_by_profile(name=None):
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
    if not name:
        name = os.environ.get("BTRDB_PROFILE", 'default')

    # load from credentials yaml file
    creds = load_credentials_from_file()
    if name not in creds.keys():
        if name == 'default':
            return {}
        raise ProfileNotFound("Profile `{}` not found in credentials file.".format(name))

    # rename api_key if needed and return
    fragment = creds[name].get("btrdb", {})
    if "api_key" in fragment:
        fragment["apikey"] = fragment.pop("api_key")
    return fragment

@filter_none
def credentials_by_env():
    """
    Returns the BTrDB connection information (as dict) using BTRDB_ENDPOINTS and
    BTRDB_API_KEY ENV variables.

    Returns
    -------
    dict
        A dictionary containing connection information
    """
    return {
        "endpoints": os.environ.get("BTRDB_ENDPOINTS", None),
        "apikey": os.environ.get("BTRDB_API_KEY",  None),
    }


def credentials(endpoints=None, apikey=None):
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

    """
    creds = {}
    credentials_by_arg = filter_none(lambda: { "endpoints": endpoints, "apikey": apikey, })
    pipeline = [credentials_by_env, credentials_by_arg]
    if os.path.exists(CREDENTIALS_PATH):
        pipeline.insert(0, credentials_by_profile)

    [creds.update(func()) for func in pipeline]
    return creds
