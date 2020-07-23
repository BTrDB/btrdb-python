from functools import partial

import ray

import btrdb
from btrdb.conn import BTrDB

def register_serializer(conn_str=None, apikey=None, profile=None):
    """
    Register serializer for BTrDB Object
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
    """
    ray.register_custom_serializer(
    BTrDB, serializer=btrdb_serializer, deserializer=partial(btrdb_deserializer, conn_str=conn_str, apikey=apikey, profile=profile))

def btrdb_serializer(_):
    """
    sererialize function
    """
    return None

def btrdb_deserializer(_, conn_str=None, apikey=None, profile=None):
    """
    deserialize function
    
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
    return btrdb.connect(conn_str=conn_str, apikey=apikey, profile=profile)
