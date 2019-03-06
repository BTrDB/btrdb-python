# btrdb.utils.buffer
# Module for buffering utilities
#
# Author:   PingThings
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# For license information, see LICENSE.txt
# ID: buffer.py [] allen@pingthings.io $

"""
Module for buffering utilities
"""

##########################################################################
## Imports
##########################################################################

from collections import defaultdict


##########################################################################
## Classes
##########################################################################

class PointBuffer(defaultdict):

    def __init__(self, length, *args, **kwargs):
        super().__init__(lambda: [None] * length, *args, **kwargs)
        self.num_streams = length
        self.active = [True] * length
        self.last_known_time = [None] * length


    def add_point(self, stream_index, point):
        self.last_known_time[stream_index] = max(
            (self.last_known_time[stream_index] or -1), point.time
        )

        self[point.time][stream_index] = point

    def earliest(self):
        if len(self.keys()) == 0:
            return None
        return min(self.keys())

    def deactivate(self, stream_index):
        self.active[stream_index] = False

    def is_ready(self, key):
        """
        Returns bool indicating whether a given key has all points from active
        (at the time) streams.
        """
        # check each stream value to see if its valid/ready
        for idx, latest_time in enumerate(self.last_known_time):

            # return False if stream is active and we are waiting on val
            if self.active[idx] and (latest_time is None or key > latest_time):
                return False

        return True

    def next_key_ready(self):
        """

        """
        keys = list(self.keys())
        keys.sort()
        for key in keys:
            if self.is_ready(key):
                return key

        return None
