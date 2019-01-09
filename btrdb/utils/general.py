# btrdb.utils.general
# General utilities for btrdb bindings
#
# Author:   PingThings
# Created:  Wed Jan 02 17:00:49 2019 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
# ID: general.py [] allen@pingthings.io $

"""
General utilities for btrdb bindings
"""

##########################################################################
## Functions
##########################################################################


def unpack_stream_descriptor(desc):
    """
    Returns dicts for tags and annotations found in supplied stream
    """
    tags = {}
    for tag in desc.tags:
        tags[tag.key] = tag.val.value

    anns = {}
    for ann in desc.annotations:
        anns[ann.key] = ann.val.value
    return tags, anns
