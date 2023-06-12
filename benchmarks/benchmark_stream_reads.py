import pprint
import uuid
from time import perf_counter
from typing import Dict, Union

import pandas as pd

import btrdb


def time_single_stream_raw_values(
    stream: btrdb.stream.Stream, start: int, end: int, version: int = 0
) -> Dict[str, Union[int, str]]:
    """Return the elapsed time for the stream raw values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return raw values.
    start : int, required
        The start time (in nanoseconds) to return raw values (inclusive).
    end : int, required
        The end time (in nanoseconds) to return raw values (exclusive)
    version : int, optional, default : 0
        The version of the stream to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict
        The performance results of the stream method
    """
    expected_count = stream.count(start, end, version=version, precise=True)
    tic = perf_counter()
    vals = stream.values(start, end, version=version)
    toc = perf_counter()
    # minus 1 to account for the exclusive end time
    queried_points = len(vals)
    assert queried_points == expected_count
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(
        stream.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_single_stream_arrow_raw_values(
    stream: btrdb.stream.Stream, start: int, end: int, version: int = 0
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the stream arrow raw values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return the raw data as an arrow table.
    start : int, required
        The start time (in nanoseconds) to return raw values (inclusive).
    end : int, required
        The end time (in nanoseconds) to return raw values (exclusive)
    version : int, optional, default : 0
        The version of the stream to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict
        The performance results of the stream method
    """
    # minus 1 to account for the exclusive end time for the values query
    expected_count = stream.count(start, end, version=version, precise=True)
    tic = perf_counter()
    vals = stream.arrow_values(start, end, version=version)
    toc = perf_counter()
    # num of rows
    queried_points = vals.num_rows
    assert queried_points == expected_count
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(
        stream.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_single_stream_windows_values(
    stream: btrdb.stream.Stream, start: int, end: int, width_ns: int, version: int = 0
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the stream windows values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return the windowed data as a list of statpoints
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    width_ns : int, required
        The window width (in nanoseconds) for the statpoints
    version : int, optional, default : 0
        The version of the stream to query for points.

    Returns
    -------
    results : dict
        The performance results of the stream method
    """
    tic = perf_counter()
    vals = stream.windows(start, end, width=width_ns, version=version)
    toc = perf_counter()
    # num of statpoints
    queried_points = len(vals)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(
        stream.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_single_stream_arrow_windows_values(
    stream: btrdb.stream.Stream, start: int, end: int, width_ns: int, version: int = 0
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the stream arrow window values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return the windowed data as an arrow table.
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    width_ns : int, required
        The window width (in nanoseconds) for the statpoints
    version : int, optional, default : 0
        The version of the stream to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict
        The performance results of the stream method
    """
    tic = perf_counter()
    vals = stream.arrow_windows(start, end, width=width_ns, version=version)
    toc = perf_counter()
    # num of statpoints
    queried_points = vals.num_rows
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(
        stream.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_single_stream_aligned_windows_values(
    stream: btrdb.stream.Stream, start: int, end: int, pointwidth: int, version: int = 0
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the stream window values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return the windowed data as a list of statpoints
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    pointwidth : int, required
        The level of the tree to return statpoints (the exponent k in 2**k)
    version : int, optional, default : 0
        The version of the stream to query for points.

    Returns
    -------
    results : dict
        The performance results of the stream method
    """
    tic = perf_counter()
    vals = stream.aligned_windows(start, end, pointwidth=pointwidth, version=version)
    toc = perf_counter()
    # num of statpoints
    queried_points = len(vals)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(
        stream.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_single_stream_arrow_aligned_windows_values(
    stream: btrdb.stream.Stream, start: int, end: int, pointwidth: int, version: int = 0
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the stream arrow aligned window values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return the windowed data as an arrow table.
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    pointwidth : int, required
        The level of the tree to return statpoints (the exponent k in 2**k)
    version : int, optional, default : 0
        The version of the stream to query for points.

    Returns
    -------
    results : dict
        The performance results of the stream method
    """
    tic = perf_counter()
    vals = stream.arrow_aligned_windows(
        start, end, pointwidth=pointwidth, version=version
    )
    toc = perf_counter()
    # num of statpoints
    queried_points = vals.num_rows
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(
        stream.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def _create_stream_result_dict(
    uu: uuid.UUID,
    point_count: int,
    total_time: float,
    version: int,
) -> Dict[str, Union[str, int, float]]:
    return {
        "uuid": str(uu),
        "total_points": point_count,
        "total_time_seconds": total_time,
        "stream_version": version,
    }


def main():
    """Run a single run of the benchmarks"""
    conn = btrdb.connect(profile="andy")
    stream1 = conn.stream_from_uuid(
        list(conn.streams_in_collection("andy/7064-6684-5e6e-9e14-ff9ca7bae46e"))[
            0
        ].uuid
    )
    start = stream1.earliest()[0].time
    end = stream1.latest()[0].time
    width_ns = btrdb.utils.timez.ns_delta(minutes=1)
    pointwidth = btrdb.utils.general.pointwidth(38)
    print(f"pointwidth of: {pointwidth}")
    res_list = []
    for f in [time_single_stream_arrow_raw_values, time_single_stream_raw_values]:
        res = f(stream1, start, end, 0)
        res["func"] = f.__name__
        res_list.append(res)
    for f in [
        time_single_stream_arrow_windows_values,
        time_single_stream_windows_values,
    ]:
        res = f(stream1, start, end, width_ns=width_ns, version=0)
        res["func"] = f.__name__
        res_list.append(res)
    for f in [
        time_single_stream_arrow_aligned_windows_values,
        time_single_stream_aligned_windows_values,
    ]:
        res = f(stream1, start, end, pointwidth=pointwidth, version=0)
        res["func"] = f.__name__
        res_list.append(res)
    return res_list


if __name__ == "__main__":
    result = main()
    pprint.pprint(result)
    print(pd.DataFrame.from_dict(result, orient="columns"))
