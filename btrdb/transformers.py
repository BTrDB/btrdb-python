# btrdb.transformers
# Value transformation utilities
#
# Author:   PingThings
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# For license information, see LICENSE.txt
# ID: transformers.py [] allen@pingthings.io $

"""
Value transformation utilities
"""

##########################################################################
## Imports
##########################################################################

import csv

##########################################################################
## Helper Functions
##########################################################################

def _get_time_from_row(row):
    for item in row:
        if item: return item.time
    raise Exception("Row contains no data")


def _stream_names(stream_set):
    return tuple(
        s.collection() + "/" +  s.tags()["name"] \
        for s in stream_set._streams
    )


##########################################################################
## Transform Functions
##########################################################################

def to_series(stream_set):
    """
    Returns a list of Pandas series objects
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    result = []
    for output in stream_set.values():
        times, values = [], []
        for item in output:
            times.append(item.time)
            values.append(item.value)
        result.append(pd.Series(data=values, index=times))
    return result

def to_dataframe(stream_set, columns=None):
    """
    Returns a Pandas DataFrame object.

    @param columns (list) iterable of column names to use for dataframe
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    stream_names = _stream_names(stream_set)
    columns = columns if columns else ["time"] + list(stream_names)
    return pd.DataFrame(to_dict(stream_set), columns=columns)


def to_array(stream_set):
    """
    Returns a tuple of Numpy arrays
    """
    try:
        import numpy as np
    except ImportError:
        raise ImportError("Please install Numpy to use this transformation function.")

    return tuple([np.array(output) for output in stream_set.values()])


def to_dict(stream_set):
    """
    Returns a generator that yields dictionary objects for each time code
    """
    stream_names = _stream_names(stream_set)
    for row in stream_set.rows():
        item = {
            "time": _get_time_from_row(row),
        }
        for idx, col in enumerate(stream_names):
            item[col] = row[idx].value if row[idx] else None
        yield item


def to_csv(stream_set, path, dialect=None, headers=None):
    """
    Saves stream data as csv
    """
    with open(path, "w") as csvfile:
        stream_names = _stream_names(stream_set)
        headers = headers if headers else ["time"] + list(stream_names)

        writer = csv.DictWriter(csvfile, fieldnames=headers, dialect=dialect)
        writer.writeheader()

        for item in to_dict(stream_set):
            writer.writerow(item)


def to_table(stream_set):
    """
    Returns string of table
    """
    try:
        from tabulate import tabulate
    except ImportError:
        raise ImportError("Please install tabulate to use this transformation function.")

    return tabulate(stream_set.to_dict(), headers="keys")


##########################################################################
## Transform Classes
##########################################################################

class StreamSetTransformer(object):
    """
    Base class for StreamSet or Stream transformations
    """
    to_dict = to_dict
    to_array = to_array
    to_series = to_series
    to_dataframe = to_dataframe

    to_csv = to_csv
    to_table = to_table
