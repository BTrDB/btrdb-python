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
