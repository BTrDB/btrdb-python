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


def _stream_names(streamset):
    return tuple(
        s.collection + "/" +  s.name \
        for s in streamset._streams
    )


##########################################################################
## Transform Functions
##########################################################################

def to_series(streamset, datetime64_index=True, agg="mean"):
    """
    Returns a list of Pandas Series objects indexed by time

    Parameters
    ----------
    datetime64_index: bool
        Directs function to convert Series index to np.datetime64[ns] or
        leave as np.int64.

    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to create the Series
        from. Must be one of "min", "mean", "max", "count", or "stddev". This
        argument is ignored if RawPoint values are passed into the function.

    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    result = []
    stream_names = _stream_names(streamset)

    for idx, output in enumerate(streamset.values()):
        times, values = [], []
        for point in output:
            times.append(point.time)
            if point.__class__.__name__ == "RawPoint":
                values.append(point.value)
            else:
                values.append(getattr(point, agg))

        if datetime64_index:
            times = pd.Index(times, dtype='datetime64[ns]')

        result.append(pd.Series(
            data=values, index=times, name=stream_names[idx]
        ))
    return result


def to_dataframe(streamset, columns=None, agg="mean"):
    """
    Returns a Pandas DataFrame object indexed by time and using the values of a
    stream for each column.

    Parameters
    ----------
    columns: sequence
        column names to use for DataFrame

    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to create the Series
        from. Must be one of "min", "mean", "max", "count", or "stddev". This
        argument is ignored if RawPoint values are passed into the function.

    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    stream_names = _stream_names(streamset)
    columns = columns if columns else ["time"] + list(stream_names)
    return pd.DataFrame(to_dict(streamset,agg=agg), columns=columns).set_index("time")


def to_array(streamset, agg="mean"):
    """
    Returns a multidimensional numpy array (similar to a list of lists) containing point
    classes.

    Parameters
    ----------
    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to return for the
        arrays. Must be one of "min", "mean", "max", "count", or "stddev". This
        argument is ignored if RawPoint values are passed into the function.

    """
    try:
        import numpy as np
    except ImportError:
        raise ImportError("Please install Numpy to use this transformation function.")

    results = []
    for points in streamset.values():
        segment = []
        for point in points:
            if point.__class__.__name__ == "RawPoint":
                segment.append(point.value)
            else:
                segment.append(getattr(point, agg))
        results.append(segment)
    return np.array(results)


def to_dict(streamset, agg="mean"):
    """
    Returns a list of OrderedDict for each time code with the appropriate
    stream data attached.

    Parameters
    ----------
    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to constrain dict
        keys. Must be one of "min", "mean", "max", "count", or "stddev". This
        argument is ignored if RawPoint values are passed into the function.

    """
    data = []
    stream_names = _stream_names(streamset)
    for row in streamset.rows():
        item = OrderedDict({
            "time": _get_time_from_row(row),
        })
        for idx, col in enumerate(stream_names):
            if row[idx].__class__.__name__ == "RawPoint":
                item[col] = row[idx].value if row[idx] else None
            else:
                item[col] = getattr(row[idx], agg) if row[idx] else None
        data.append(item)
    return data


def to_csv(streamset, fobj, dialect=None, fieldnames=None, agg="mean"):
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

    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to return when
        limiting results. Must be one of "min", "mean", "max", "count", or "stddev".
        This argument is ignored if RawPoint values are passed into the function.
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
        stream_names = _stream_names(streamset)
        fieldnames = fieldnames if fieldnames else ["time"] + list(stream_names)

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect=dialect)
        writer.writeheader()

        for item in to_dict(streamset, agg=agg):
            writer.writerow(item)


def to_table(streamset, agg="mean"):
    """
    Returns string representation of the data in tabular form using the tabulate
    library.

    Parameters
    ----------
    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to create the Series
        from. Must be one of "min", "mean", "max", "count", or "stddev". This
        argument is ignored if RawPoint values are passed into the function.

    """
    try:
        from tabulate import tabulate
    except ImportError:
        raise ImportError("Please install tabulate to use this transformation function.")

    return tabulate(streamset.to_dict(agg=agg), headers="keys")


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
