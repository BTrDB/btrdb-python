# btrdb.utils.conversion
# Conversion utilities for btrdb
#
# Author:   PingThings
# Created:  Tue Dec 18 14:50:05 2018 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: conversion.py [] allen@pingthings.io $

"""
Conversion utilities for btrdb
"""

##########################################################################
## Imports
##########################################################################

import uuid
import json
from datetime import datetime


##########################################################################
## Classes
##########################################################################

class AnnotationEncoder(json.JSONEncoder):
    """Default JSON encoder class for saving stream annotations"""

    def default(self, obj):
        RFC3339 = "%Y-%m-%d %H:%M:%S.%f%z"

        # handle Python datetime
        if isinstance(obj, datetime):
            return obj.strftime(RFC3339)

        # handle numpy datetime64
        try:
            import numpy as np
            if isinstance(obj, np.datetime64):
                return obj.astype(datetime).strftime(RFC3339)
        except ImportError:
            pass

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


##########################################################################
## Functions
##########################################################################

def to_uuid(obj):
    """
    Converts argument to UUID

    @param obj: object to be converted to UUID
    @return: returns instance of uuid.UUID
    @raise TypeError: raised if obj is of unsupported class
    """
    if isinstance(obj, uuid.UUID):
        return obj

    if isinstance(obj, bytes):
        obj = obj.decode("UTF-8")

    if isinstance(obj, str):
        return uuid.UUID(obj)

    raise TypeError("Cannot convert object to UUID ({})".format(
        obj.__class__.__name__)
    )
