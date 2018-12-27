# btrdb.transformers
# Value transformation utilities
#
# Author:   Allen Leis <allen@pingthings.io>
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# Copyright (C) 2018 PingThings LLC
# For license information, see LICENSE.txt
#
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


def _stream_names(collection):
    return tuple(
        s.collection() + "/" +  s.tags()["name"] \
        for s in collection._streams
    )


##########################################################################
## Transform Functions
##########################################################################

def to_series(collection):
    """
    Returns a list of Pandas series objects
    """
    try:
        import pandas as pd
    except ModuleNotFoundError:
        raise Exception("Please install Pandas to use this transformation function.")

    result = []
    for output in collection.values:
        result.append(
            pd.Series([
                {"time": item.time, "value": item.value}
                for item in output
            ])
        )
    return result

def to_dataframe(collection, columns=None):
    """
    Returns a Pandas DataFrame object.

    @param columns (list) iterable of column names to use for dataframe
    """
    try:
        import pandas as pd
    except ModuleNotFoundError:
        raise Exception("Please install Pandas to use this transformation function.")

    stream_names = _stream_names(collection)
    columns = columns if columns else ["time"] + list(stream_names)
    return pd.DataFrame(to_dict(collection), columns=columns)


def to_array(collection):
    """
    Returns a tuple of Numpy arrays
    """
    try:
        import numpy as np
    except ModuleNotFoundError:
        raise Exception("...")

    return tuple([np.array(output) for output in collection.values])


def to_dict(collection):
    """
    Returns a generator that yields dictionary objects for each time code
    """
    stream_names = _stream_names(collection)
    for row in collection.rows():
        item = {
            "time": _get_time_from_row(row),
        }
        for idx, col in enumerate(stream_names):
            item[col] = row[idx].value if row[idx] else None
        yield item


def to_csv(collection, path, dialect=None, headers=None):
    """
    Saves stream data as csv
    """
    with open(path, "w") as csvfile:
        stream_names = _stream_names(collection)
        headers = headers if headers else ["time"] + list(stream_names)

        writer = csv.DictWriter(csvfile, fieldnames=headers, dialect=dialect)
        writer.writeheader()

        for item in to_dict(collection):
            writer.writerow(item)


def to_table(collection):
    """
    Returns string of table
    """
    try:
        from tabulate import tabulate
    except ModuleNotFoundError:
        raise Exception("...")

    return tabulate(collection.to_dict(), headers="keys")


##########################################################################
## Transform Classes
##########################################################################

class StreamTransformer(object):
    """
    Base class for StreamCollection or Stream transformations
    """
    to_dict = to_dict
    to_array = to_array
    to_series = to_series
    to_dataframe = to_dataframe

    to_csv = to_csv
    to_table = to_table
