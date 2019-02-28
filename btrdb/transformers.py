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
import contextlib
from collections import OrderedDict

##########################################################################
## Helper Functions
##########################################################################

def _get_time_from_row(row):
    for item in row:
        if item: return item.time
    raise Exception("Row contains no data")


def _stream_names(stream_set):
    return tuple(
        s.collection + "/" +  s.name \
        for s in stream_set._streams
    )


##########################################################################
## Transform Functions
##########################################################################

def to_series(stream_set, datetime64_index=True):
    """
    Returns a list of Pandas Series objects indexed by time

    Parameters
    ----------
    datetime64_index: bool
        Directs function to convert Series index to np.datetime64[ns] or
        leave as np.int64.

    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    result = []
    stream_names = _stream_names(stream_set)

    for idx, output in enumerate(stream_set.values()):
        times, values = [], []
        for item in output:
            times.append(item.time)
            values.append(item.value)

        if datetime64_index:
            times = pd.Index(times, dtype='datetime64[ns]')

        result.append(pd.Series(
            data=values, index=times, name=stream_names[idx]
        ))
    return result

def to_dataframe(stream_set, columns=None):
    """
    Returns a Pandas DataFrame object indexed by time and using the values of a
    stream for each column.

    Parameters
    ----------
    columns: sequence
        column names to use for DataFrame
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    stream_names = _stream_names(stream_set)
    columns = columns if columns else ["time"] + list(stream_names)
    return pd.DataFrame(to_dict(stream_set), columns=columns).set_index("time")


def to_array(stream_set):
    """
    Returns a list of Numpy arrays (one per stream) containing point classes.
    """
    try:
        import numpy as np
    except ImportError:
        raise ImportError("Please install Numpy to use this transformation function.")

    return [np.array(output) for output in stream_set.values()]


def to_dict(stream_set):
    """
    Returns a list of OrderedDict for each time code with the appropriate
    stream data attached.
    """
    data = []
    stream_names = _stream_names(stream_set)
    for row in stream_set.rows():
        item = OrderedDict({
            "time": _get_time_from_row(row),
        })
        for idx, col in enumerate(stream_names):
            item[col] = row[idx].value if row[idx] else None
        data.append(item)
    return data


def to_csv(stream_set, fobj, dialect=None, fieldnames=None):
    """
    Saves stream data as a CSV file.

    Parameters
    ----------
    fobj: str or file-like object
        Path to use for saving CSV file or a file-like object to use to write to.

    dialect: csv.Dialect
        CSV dialect object from Python csv module.  See Python's csv module for
        more information.

    fieldnames: sequence
        A sequence of strings to use as fieldnames in the CSV header.  See
        Python's csv module for more information.

    """

    @contextlib.contextmanager
    def open_path_or_file(path_or_file):
        if isinstance(path_or_file, str):
            f = file_to_close = open(path_or_file, 'w')
        else:
            f = path_or_file
            file_to_close = None
        try:
            yield f
        finally:
            if file_to_close:
                file_to_close.close()

    with open_path_or_file(fobj) as csvfile:
        stream_names = _stream_names(stream_set)
        fieldnames = fieldnames if fieldnames else ["time"] + list(stream_names)

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect=dialect)
        writer.writeheader()

        for item in to_dict(stream_set):
            writer.writerow(item)


def to_table(stream_set):
    """
    Returns string representation of the data in tabular form using the tabulate
    library.
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
