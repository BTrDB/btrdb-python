# btrdb
# Package for the btrdb database library.
#
# Author:   PingThings
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# For license information, see LICENSE.txt
# ID: __init__.py [] allen@pingthings.io $

"""
Package for the btrdb database library.
"""

##########################################################################
## Imports
##########################################################################

import os
from btrdb.conn import Connection, BTrDB
from btrdb.endpoint import Endpoint
from btrdb.exceptions import ConnectionError, CredentialsFileNotFound, \
    ProfileNotFound
from btrdb.version import get_version
from btrdb.utils.credentials import load_profile

##########################################################################
## Module Variables
##########################################################################

__version__ = get_version()

BTRDB_ENDPOINTS = "BTRDB_ENDPOINTS"
BTRDB_API_KEY = "BTRDB_API_KEY"
BTRDB_PROFILE = "BTRDB_PROFILE"

##########################################################################
## Functions
##########################################################################

def _connect(conn_str=None, apikey=None):
    return BTrDB(Endpoint(Connection(conn_str, apikey=apikey).channel))

def connect(conn_str=None, apikey=None, profile=None):
    """
    Connect to a BTrDB server.

    Parameters
    ----------
    conn_str: str, default=None
        The address and port of the cluster to connect to, e.g. `192.168.1.1:4411`.
        If set to None, will look in the environment variable `$BTRDB_ENDPOINTS`
        (recommended).
    apikey: str, default=None
        The API key used to authenticate requests (optional). If None, the key
        is looked up from the environment variable `$BTRDB_API_KEY`.
    profile: str, default=None
        The name of a profile containing the required connection information as
        found in the user's predictive grid credentials file
        `~/.predictivegrid/credentials.yaml`.

    Returns
    -------
    db : BTrDB
        An instance of the BTrDB context to directly interact with the database.

    """
    # Check function arguments

    if conn_str and profile:
        raise ValueError("Received both conn_str and profile arguments.")

    if profile:
        credentials = load_profile(profile)
        return _connect(credentials["endpoints"], credentials["api_key"])

    if conn_str:
        return _connect(conn_str, apikey)

    # Check ENV variables

    keys = os.environ.keys()
    if BTRDB_PROFILE in keys and BTRDB_ENDPOINTS in keys:
        raise ValueError("Found both BTRDB_PROFILE and BTRDB_ENDPOINTS in ENV. "
            "Only one is allowed.")

    if BTRDB_PROFILE in keys:
        credentials = load_profile(os.environ[BTRDB_PROFILE])
        return _connect(credentials["endpoints"], credentials["api_key"])

    if BTRDB_ENDPOINTS in keys:
        return _connect(os.environ[BTRDB_ENDPOINTS], os.environ.get(BTRDB_API_KEY, None))


    # Attempt default profile (no arguments or ENV found)

    try:
        credentials = load_profile("default")
        return _connect(credentials["endpoints"], credentials["api_key"])
    except (CredentialsFileNotFound, ProfileNotFound):
        pass

    raise ConnectionError("Could not determine credentials to use.")
