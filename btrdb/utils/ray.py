from functools import partial

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
    try:
        import ray
    except ImportError:
        raise ImportError("must pip install ray to register custom serializer")
    try:
        import semver
    except ImportError:
        raise ImportError("must pip install semver to register custom serializer")

    assert (
        ray.is_initialized()
    ), "Need to call ray.init() before registering custom serializer"
    ver = semver.VersionInfo.parse(ray.__version__)
    if ver.major == 0:
        ray.register_custom_serializer(
            BTrDB,
            serializer=btrdb_serializer,
            deserializer=partial(
                btrdb_deserializer, conn_str=conn_str, apikey=apikey, profile=profile
            ),
        )
    elif ver.major == 1 and ver.minor in range(
        2, 13
    ):  # as of May 2022, latest Ray version is at 1.12
        # TODO: check different versions of ray?
        ray.util.register_serializer(
            BTrDB,
            serializer=btrdb_serializer,
            deserializer=partial(
                btrdb_deserializer, conn_str=conn_str, apikey=apikey, profile=profile
            ),
        )
    else:
        raise Exception(
            "Ray version %s does not have custom serialization. Please upgrade to >= 1.4.0"
            % ray.__version__
        )


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
