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

from btrdb.conn import Connection, BTrDB
from btrdb.endpoint import Endpoint
from btrdb.exceptions import ConnectionError
from btrdb.version import get_version
from btrdb.utils.credentials import credentials_by_profile, credentials
from btrdb.utils.ray import register_serializer
from btrdb.stream import MINIMUM_TIME, MAXIMUM_TIME
from warnings import warn

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

def _connect(endpoints=None, apikey=None):
    return BTrDB(Endpoint(Connection(endpoints, apikey=apikey).channel))

def connect(conn_str=None, apikey=None, profile=None, shareable=False):
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
    shareable: bool, default=False
        Whether or not the connection can be "shared" in a distributed setting such
        as Ray workers. If set to True, the connection can be serialized and sent
        to other workers so that data can be retrieved in parallel; **however**, this
        is less secure because it is possible for other users of the Ray cluster to
        use your API key to fetch data.

    Returns
    -------
    db : BTrDB
        An instance of the BTrDB context to directly interact with the database.

    """
    # do not allow user to provide both address and profile
    if conn_str and profile:
        raise ValueError("Received both conn_str and profile arguments.")

    # check shareable flag and register custom serializer if necessary
    if shareable:
        warn("a shareable connection is potentially insecure; other users of the same cluster may be able to access your API key")
        register_serializer(conn_str=conn_str, apikey=apikey, profile=profile)

    # use specific profile if requested
    if profile:
        return _connect(**credentials_by_profile(profile))

    # resolve credentials using combination of arguments, env
    creds = credentials(conn_str, apikey)
    if "endpoints" in creds:
        return _connect(**creds)

    raise ConnectionError("Could not determine credentials to use.")

