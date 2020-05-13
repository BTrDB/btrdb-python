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
from warnings import warn

##########################################################################
## Helper Functions
##########################################################################

_STAT_PROPERTIES = ('min', 'mean', 'max', 'count', 'stddev')

def _get_time_from_row(row):
    for item in row:
        if item: return item.time
    raise Exception("Row contains no data")


def _stream_names(streamset, func):
    """
    private convenience function to come up with proper final stream names
    before sending a collection of streams (dataframe, etc.) back to the
    user.
    """
    return tuple(
        func(s) for s in streamset._streams
    )


##########################################################################
## Transform Functions
##########################################################################

def to_series(streamset, datetime64_index=True, agg="mean", name_callable=None):
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

    name_callable : lambda, default: lambda s: s.collection + "/" +  s.name
        Sprecify a callable that can be used to determine the series name given a
        Stream object.

    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    # TODO: allow this at some future point
    if agg == "all":
        raise AttributeError("cannot use 'all' as aggregate at this time")

    if not callable(name_callable):
        name_callable = lambda s: s.collection + "/" +  s.name


    result = []
    stream_names = _stream_names(streamset, name_callable)

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


def to_dataframe(streamset, columns=None, agg="mean", name_callable=None):
    """
    Returns a Pandas DataFrame object indexed by time and using the values of a
    stream for each column.

    Parameters
    ----------
    columns: sequence
        column names to use for DataFrame.  Deprecated and not compatible with name_callable.

    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to create the Series
        from. Must be one of "min", "mean", "max", "count", "stddev", or "all". This
        argument is ignored if not using StatPoints.

    name_callable : lambda, default: lambda s: s.collection + "/" +  s.name
        Sprecify a callable that can be used to determine the series name given a
        Stream object.  This is not compatible with agg == "all" at this time


    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Please install Pandas to use this transformation function.")

    # deprecation warning added in v5.8
    if columns:
        warn("the columns argument is deprecated and will be removed in a future release", DeprecationWarning, stacklevel=2)

    # TODO: allow this at some future point
    if agg == "all" and name_callable is not None:
        raise AttributeError("cannot provide name_callable when using 'all' as aggregate at this time")

    # do not allow agg="all" with RawPoints
    if agg == "all" and streamset.allow_window:
        agg=""

    # default arg values
    if not callable(name_callable):
        name_callable = lambda s: s.collection + "/" +  s.name


    df = pd.DataFrame(to_dict(streamset,agg=agg))
    df = df.set_index("time")

    if agg == "all" and not streamset.allow_window:
        stream_names = [[s.collection, s.name, prop] for s in streamset._streams for prop in _STAT_PROPERTIES]
        df.columns=pd.MultiIndex.from_tuples(stream_names)
    else:
        df.columns =  columns if columns else _stream_names(streamset, name_callable)

    return df


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

    # TODO: allow this at some future point
    if agg == "all":
        raise AttributeError("cannot use 'all' as aggregate at this time")

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


def to_dict(streamset, agg="mean", name_callable=None):
    """
    Returns a list of OrderedDict for each time code with the appropriate
    stream data attached.

    Parameters
    ----------
    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to constrain dict
        keys. Must be one of "min", "mean", "max", "count", or "stddev". This
        argument is ignored if RawPoint values are passed into the function.

    name_callable : lambda, default: lambda s: s.collection + "/" +  s.name
        Sprecify a callable that can be used to determine the series name given a
        Stream object.

    """
    if not callable(name_callable):
        name_callable = lambda s: s.collection + "/" +  s.name

    data = []
    stream_names = _stream_names(streamset, name_callable)

    for row in streamset.rows():
        item = OrderedDict({
            "time": _get_time_from_row(row),
        })
        for idx, col in enumerate(stream_names):
            if row[idx].__class__.__name__ == "RawPoint":
                item[col] = row[idx].value if row[idx] else None
            else:
                if agg == "all":
                    for stat in _STAT_PROPERTIES:
                        item["{}-{}".format(col, stat)] = getattr(row[idx], stat) if row[idx] else None
                else:
                    item[col] = getattr(row[idx], agg) if row[idx] else None
        data.append(item)
    return data


def to_csv(streamset, fobj, dialect=None, fieldnames=None, agg="mean", name_callable=None):
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

    name_callable : lambda, default: lambda s: s.collection + "/" +  s.name
        Sprecify a callable that can be used to determine the series name given a
        Stream object.
    """

    # TODO: allow this at some future point
    if agg == "all":
        raise AttributeError("cannot use 'all' as aggregate at this time")

    if not callable(name_callable):
        name_callable = lambda s: s.collection + "/" +  s.name

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
        stream_names = _stream_names(streamset, name_callable)
        fieldnames = fieldnames if fieldnames else ["time"] + list(stream_names)

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect=dialect)
        writer.writeheader()

        for item in to_dict(streamset, agg=agg):
            writer.writerow(item)


def to_table(streamset, agg="mean", name_callable=None):
    """
    Returns string representation of the data in tabular form using the tabulate
    library.

    Parameters
    ----------
    agg : str, default: "mean"
        Specify the StatPoint field (e.g. aggregating function) to create the Series
        from. Must be one of "min", "mean", "max", "count", or "stddev". This
        argument is ignored if RawPoint values are passed into the function.

    name_callable : lambda, default: lambda s: s.collection + "/" +  s.name
        Sprecify a callable that can be used to determine the column name given a
        Stream object.

    """
    try:
        from tabulate import tabulate
    except ImportError:
        raise ImportError("Please install tabulate to use this transformation function.")

    # TODO: allow this at some future point
    if agg == "all":
        raise AttributeError("cannot use 'all' as aggregate at this time")

    if not callable(name_callable):
        name_callable = lambda s: s.collection + "/" +  s.name

    return tabulate(streamset.to_dict(agg=agg, name_callable=name_callable), headers="keys")


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
