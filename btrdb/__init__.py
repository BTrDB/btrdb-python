import os
from btrdb.conn import Connection
from btrdb.exceptions import ConnectionError


def connect(conn_str=None):
    if not conn_str:
        conn_str = os.environ.get("BTRDB_ENDPOINTS", None)
        if not env:
            raise ConnectionError("Connection string not supplied and no ENV variable found.")

    return Connection(conn_str)
