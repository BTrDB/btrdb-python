import uuid
from time import perf_counter
from typing import Dict, Union

import pandas

import btrdb


def time_streamset_raw_values(
    streamset: btrdb.stream.StreamSet, start: int, end: int, version: int = 0
) -> Dict[str, Union[int, str]]:
    """Return the elapsed time for the streamset raw values query

    Parameters
    ----------
    streamset : btrdb.StreamSet, required
        The data streamset to return raw values.
    start : int, required
        The start time (in nanoseconds) to return raw values (inclusive).
    end : int, required
        The end time (in nanoseconds) to return raw values (exclusive)
    version : int, optional, default : 0
        The version of the streamset to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict
        The performance results of the streamset method
    """
    streamset = streamset.filter(start=start, end=end)
    versions = {s.uuid: 0 for s in streamset._streams}
    streamset.pin_versions(versions)
    expected_count = streamset.count(precise=True)
    tic = perf_counter()
    vals = streamset.values()
    toc = perf_counter()
    # print(vals)
    # minus 1 to account for the exclusive end time
    queried_points = 0
    for points in vals:
        print(len(points))
        queried_points += len(points)
    expected_count = streamset.count(precise=True)
    print(queried_points)
    print(expected_count)
    assert queried_points == expected_count
    # time in seconds to run
    run_time = toc - tic
    res = _create_streamset_result_dict(
        streamset, point_count=queried_points, total_time=run_time, version=version
    )
    return res


def time_streamset_arrow_raw_values(
    streamset: btrdb.stream.StreamSet, start: int, end: int, version: int = 0
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the streamset arrow raw values query

    Parameters
    ----------
    streamset : btrdb.StreamSet, required
        The data streamset to return the raw data as an arrow table.
    start : int, required
        The start time (in nanoseconds) to return raw values (inclusive).
    end : int, required
        The end time (in nanoseconds) to return raw values (exclusive)
    version : int, optional, default : 0
        The version of the streamset to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict
        The performance results of the streamset method
    """
    streamset = streamset.filter(start=start, end=end)
    versions = {s.uuid: 0 for s in streamset._streams}
    streamset.pin_versions(versions)
    expected_count = streamset.count(precise=True)
    tic = perf_counter()
    vals = streamset.arrow_values()
    toc = perf_counter()
    queried_points = len(streamset) * (vals.num_rows - 1)
    print(queried_points)
    print(expected_count)
    assert queried_points == expected_count
    # time in seconds to run
    run_time = toc - tic
    res = _create_streamset_result_dict(
        streamset, point_count=queried_points, total_time=run_time, version=version
    )
    return res


def time_streamset_windows_values(
    streamset: btrdb.stream.StreamSet,
    start: int,
    end: int,
    width_ns: int,
    version: int = 0,
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the streamset windows values query

    Parameters
    ----------
    streamset : btrdb.StreamSet, required
        The data streamset to return the windowed data as a list of statpoints
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    width_ns : int, required
        The window width (in nanoseconds) for the statpoints
    version : int, optional, default : 0
        The version of the streamset to query for points.

    Returns
    -------
    results : dict
        The performance results of the streamset method
    """
    tic = perf_counter()
    vals = streamset.windows(start, end, width=width_ns, version=version)
    toc = perf_counter()
    # num of statpoints
    queried_points = len(vals)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_streamset_arrow_windows_values(
    streamset: btrdb.stream.StreamSet,
    start: int,
    end: int,
    width_ns: int,
    version: int = 0,
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the streamset arrow window values query

    Parameters
    ----------
    streamset : btrdb.StreamSet, required
        The data streamset to return the windowed data as an arrow table.
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    width_ns : int, required
        The window width (in nanoseconds) for the statpoints
    version : int, optional, default : 0
        The version of the streamset to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict
        The performance results of the streamset method
    """
    tic = perf_counter()
    vals = streamset.arrow_windows(start, end, width=width_ns, version=version)
    toc = perf_counter()
    # num of statpoints
    queried_points = vals.num_rows
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_streamset_aligned_windows_values(
    streamset: btrdb.stream.StreamSet,
    start: int,
    end: int,
    pointwidth: int,
    version: int = 0,
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the streamset window values query

    Parameters
    ----------
    streamset : btrdb.StreamSet, required
        The data streamset to return the windowed data as a list of statpoints
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    pointwidth : int, required
        The level of the tree to return statpoints (the exponent k in 2**k)
    version : int, optional, default : 0
        The version of the streamset to query for points.

    Returns
    -------
    results : dict
        The performance results of the streamset method
    """
    tic = perf_counter()
    vals = streamset.aligned_windows(start, end, pointwidth=pointwidth, version=version)
    toc = perf_counter()
    # num of statpoints
    queried_points = len(vals)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_streamset_arrow_aligned_windows_values(
    streamset: btrdb.stream.StreamSet,
    start: int,
    end: int,
    pointwidth: int,
    version: int = 0,
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the streamset arrow aligned window values query

    Parameters
    ----------
    streamset : btrdb.StreamSet, required
        The data streamset to return the windowed data as an arrow table.
    start : int, required
        The start time (in nanoseconds) to return statpoint values
    end : int, required
        The end time (in nanoseconds) to return statpoint values
    pointwidth : int, required
        The level of the tree to return statpoints (the exponent k in 2**k)
    version : int, optional, default : 0
        The version of the streamset to query for points.

    Returns
    -------
    results : dict
        The performance results of the streamset method
    """
    tic = perf_counter()
    vals = streamset.arrow_aligned_windows(
        start, end, pointwidth=pointwidth, version=version
    )
    toc = perf_counter()
    # num of statpoints
    queried_points = vals.num_rows
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset.uuid, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def _create_streamset_result_dict(
    streamset: btrdb.stream.StreamSet,
    point_count: int,
    total_time: float,
    version: int,
) -> Dict[str, Union[str, int, float]]:
    return {
        "n_streams": len(streamset),
        "total_points": point_count,
        "total_time_seconds": total_time,
        "streamset_version": version,
    }


def main():
    """Run a single run of the benchmarks"""
    conn = btrdb.connect(profile="andy")
    stream1 = conn.stream_from_uuid(
        list(conn.streams_in_collection("andy/7064-6684-5e6e-9e14-ff9ca7bae46e"))[
            0
        ].uuid
    )
    stream2 = conn.stream_from_uuid(
        list(conn.streams_in_collection("andy/30e6-d72f-5cc7-9966-bc1579dc4a72"))[
            0
        ].uuid
    )
    streamset = btrdb.stream.StreamSet([stream1, stream2])
    start = max(stream1.earliest()[0].time, stream2.earliest()[0].time)
    end = min(stream1.latest()[0].time, stream2.latest()[0].time)
    width_ns = btrdb.utils.timez.ns_delta(minutes=1)
    pointwidth = btrdb.utils.general.pointwidth(38)
    print(f"pointwidth of: {pointwidth}")
    res_list = []
    for f in [time_streamset_raw_values, time_streamset_arrow_raw_values]:
        res = f(streamset, start, end, 0)
        res["func"] = f.__name__
    # for f in [time_streamset_windows_values, time_streamset_arrow_windows_values]:
    #     res = f(streamset, start, end, width_ns=width_ns, version=0)
    #     res["func"] = f.__name__
    # for f in [time_streamset_aligned_windows_values, time_streamset_arrow_aligned_windows_values]:
    #     res = f(streamset, start, end, pointwidth=pointwidth, version=0)
    #     res["func"] = res

    return res


if __name__ == "__main__":
    results = main()
    print(pandas.DataFrame(results))
