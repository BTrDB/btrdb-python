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
import pytz

from datetime import datetime

try:
    import numpy as np
except ImportError:
    np = None


RFC3339 = "%Y-%m-%d %H:%M:%S.%f%z"


##########################################################################
## Classes
##########################################################################

class AnnotationEncoder(json.JSONEncoder):
    """Default JSON encoder class for saving stream annotations"""

    def default(self, obj):
        """Handle complex and user-specific types"""
        # handle UUID objects
        if isinstance(obj, uuid.UUID):
            return str(obj)

        # handle Python datetime
        # TODO: better handling for timezone naive datetimes
        if isinstance(obj, datetime):
            return obj.strftime(RFC3339)

        # handle numpy datetime64
        if np is not None and isinstance(obj, np.datetime64):
            # We assume that np.datetime64 is UTC timezone because the datetime
            # will always be timezone naive -- this is kind of shitty
            # https://numpy.org/devdocs/reference/arrays.datetime.html#changes-with-numpy-1-11
            return pytz.utc.localize(obj.astype(datetime)).strftime(RFC3339)

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

    def encode(self, obj):
        """Do not serialize simple string values with quotes"""
        serialized = super(AnnotationEncoder, self).encode(obj)
        if serialized.startswith('"') and serialized.endswith('"'):
            serialized = serialized.strip('"')
        return serialized


class AnnotationDecoder(json.JSONDecoder):
    """Default JSON decoder class for deserializing stream annotations"""

    def decode(self, s):
        """Do not raise JSONDecodeError, just return the raw string"""
        try:
            return super(AnnotationDecoder, self).decode(s)
        except json.JSONDecodeError:
            return s


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
