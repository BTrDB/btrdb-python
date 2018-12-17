# tests
# Testing package for the btrdb library.
#
# Author:   Allen Leis <allen@pingthings.io>
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: __init__.py [] allen@pingthings.io $

"""
Testing package for the btrdb database library.
"""

##########################################################################
## Imports
##########################################################################

import os
from btrdb.conn import Connection
from btrdb.exceptions import ConnectionError


##########################################################################
## Module Variables
##########################################################################

__version__ = "4.0"

##########################################################################
## Functions
##########################################################################

def connect(conn_str=None):
    if not conn_str:
        conn_str = os.environ.get("BTRDB_ENDPOINTS", None)
        if not env:
            raise ConnectionError("Connection string not supplied and no ENV variable found.")

    return Connection(conn_str)