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
from btrdb.exceptions import ConnectionError
from btrdb.version import get_version


##########################################################################
## Module Variables
##########################################################################

__version__ = get_version()

BTRDB_ENDPOINTS = "BTRDB_ENDPOINTS"
BTRDB_API_KEY = "BTRDB_API_KEY"

##########################################################################
## Functions
##########################################################################

def connect(conn_str=None, apikey=None):
    """
    Connect to a BTrDB server.

    Parameters
    ----------
    conn_str: str, default=None
        The address and port of the cluster to connect to, e.g. `192.168.1.1:4411`.
        If set to None will look up the string from the environment variable
        $BTRDB_ENDPOINTS (recommended).
    apikey: str, default=None
        The API key used to authenticate requests (optional). If None, the key
        is looked up from the environment variable $BTRDB_API_KEY.

    Returns
    -------
    db : BTrDB
        An instance of the BTrDB context to directly interact with the database.

    """
    if not conn_str:
        conn_str = os.environ.get(BTRDB_ENDPOINTS, None)

    if not conn_str:
        raise ConnectionError("Connection string not supplied and no ENV variable found.")

    if not apikey:
        apikey = os.environ.get(BTRDB_API_KEY, default=None)

    return BTrDB(Endpoint(Connection(conn_str, apikey=apikey).channel))
