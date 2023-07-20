import pprint
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
    versions = {s.uuid: 0 for s in streamset}
    streamset.pin_versions(versions)
    # minus 1 * n_streams to account for the exclusive end time
    expected_count = streamset.count(precise=True) - len(streamset)
    tic = perf_counter()
    vals = streamset.values()
    toc = perf_counter()
    # print(vals)
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
    versions = {s.uuid: 0 for s in streamset}
    streamset.pin_versions(versions)
    # minus 1 * n_streams to account for the exclusive end time
    expected_count = streamset.count(precise=True) - len(streamset)
    tic = perf_counter()
    vals = streamset.arrow_values()
    toc = perf_counter()
    # TODO: These point counts and assertion needs to be column specific
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
    streamset = streamset.filter(start=start, end=end)
    versions = {s.uuid: 0 for s in streamset}
    streamset.pin_versions(versions)
    streamset = streamset.windows(width=width_ns)
    tic = perf_counter()
    vals = streamset.values()
    toc = perf_counter()
    # num of statpoints
    queried_points = len(vals[0]) * len(streamset)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset, point_count=queried_points, total_time=run_time, version=version
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
    streamset = streamset.filter(start=start, end=end)
    versions = {s.uuid: 0 for s in streamset}
    streamset.pin_versions(versions)
    streamset = streamset.windows(width=width_ns)
    tic = perf_counter()
    vals = streamset.arrow_values()
    toc = perf_counter()
    # num of statpoints
    queried_points = vals.num_rows * len(streamset)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset, point_count=queried_points, total_time=run_time, version=version
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
    streamset = streamset.filter(start=start, end=end)
    versions = {s.uuid: 0 for s in streamset}
    streamset.pin_versions(versions)
    streamset = streamset.aligned_windows(pointwidth=pointwidth)
    tic = perf_counter()
    vals = streamset.values()
    toc = perf_counter()
    # num of statpoints
    queried_points = len(vals[0]) * len(streamset)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset, point_count=queried_points, total_time=run_time, version=version
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
    streamset = streamset.filter(start=start, end=end)
    versions = {s.uuid: 0 for s in streamset}
    streamset.pin_versions(versions)
    streamset = streamset.aligned_windows(pointwidth=pointwidth)
    tic = perf_counter()
    vals = streamset.arrow_values()
    toc = perf_counter()
    # num of statpoints
    queried_points = vals.num_rows * len(streamset)
    assert queried_points != 0
    # time in seconds to run
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset, point_count=queried_points, total_time=run_time, version=version
    )
    return results


def time_streamset_arrow_multistream_raw_values_non_timesnapped(
    streamset: btrdb.stream.StreamSet,
    start: int,
    end: int,
    version: int = 0,
    sampling_frequency: int = None,
) -> Dict[str, Union[str, int, float]]:
    """Use the arrow multistream endpoint that joins the stream data on-server before sending to the client.

    We make sure to set a sampling rate of 0 to ensure that we do not time snap and just perform a full-outer join on
    the streams.

    Parameters
    ----------
    streamset : btrdb.stream.StreamSet, required
        The streamset to perform the multistream query on.
    start : int, required
        The start time (in nanoseconds) to query raw data from.
    end : int, required
        The end time (in nanoseconds) non-exclusive, to query raw data from.
    version : int, optional, default=0
        The version of the stream to pin against, currently this is unused.
    sampling_frequency : int, optional, ignored
        The sampling frequency of the data stream in Hz

    Notes
    -----
    Sampling_frequency is not used here, it will be manually set.
    """
    streamset = streamset.filter(start=start, end=end, sampling_frequency=0)
    versions = {s.uuid: 0 for s in streamset}
    streamset = streamset.pin_versions(versions)
    tic = perf_counter()
    vals = streamset.arrow_values()
    toc = perf_counter()
    queried_points = vals.num_rows * len(streamset)
    #    print(vals)
    #    print(vals.to_pandas().describe())
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset=streamset, total_time=run_time, point_count=queried_points, version=0
    )
    return results


def time_streamset_arrow_multistream_raw_values_timesnapped(
    streamset: btrdb.stream.StreamSet,
    start: int,
    end: int,
    sampling_frequency: int,
    version: int = 0,
) -> Dict[str, Union[str, int, float]]:
    """Use the arrow multistream endpoint that joins the stream data on-server before sending to the client.

    We make sure to set a sampling rate to ensure that we time snap the returned data.

    Parameters
    ----------
    streamset : btrdb.stream.StreamSet, required
        The streamset to perform the multistream query on.
    start : int, required
        The start time (in nanoseconds) to query raw data from.
    end : int, required
        The end time (in nanoseconds) non-exclusive, to query raw data from.
    sampling_frequency : int, required
        The common sampling frequency (in Hz) of the data to snap the data points to.
    version : int, optional, default=0
        The version of the stream to pin against, currently this is unused.
    """
    streamset = streamset.filter(
        start=start, end=end, sampling_frequency=sampling_frequency
    )
    versions = {s.uuid: 0 for s in streamset}
    streamset = streamset.pin_versions(versions)
    tic = perf_counter()
    vals = streamset.arrow_values()
    toc = perf_counter()
    queried_points = vals.num_rows * len(streamset)
    #    print(vals)
    run_time = toc - tic
    results = _create_streamset_result_dict(
        streamset=streamset, total_time=run_time, point_count=queried_points, version=0
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
        res_list.append(res)
    for f in [time_streamset_windows_values, time_streamset_arrow_windows_values]:
        res = f(streamset, start, end, width_ns=width_ns, version=0)
        res["func"] = f.__name__
        res_list.append(res)
    for f in [
        time_streamset_aligned_windows_values,
        time_streamset_arrow_aligned_windows_values,
    ]:
        res = f(streamset, start, end, pointwidth=pointwidth, version=0)
        res["func"] = f.__name__
        res_list.append(res)
    for f in [
        time_streamset_arrow_multistream_raw_values_non_timesnapped,
        time_streamset_arrow_multistream_raw_values_timesnapped,
    ]:
        res = f(streamset, start, end, sampling_frequency=2, version=0)
        res["func"] = f.__name__
        res_list.append(res)

    return res_list


if __name__ == "__main__":
    results = main()
    pprint.pprint(results)
    print(pandas.DataFrame.from_dict(results, orient="columns"))
