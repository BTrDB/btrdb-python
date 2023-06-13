from time import perf_counter
from typing import Dict, List, Tuple, Union

import pyarrow

import btrdb


def time_stream_insert(
    stream: btrdb.stream.Stream,
    data: List[Tuple[int, float]],
    merge_policy: str = "never",
) -> Dict[str, Union[int, float, str]]:
    """Insert raw data into a single stream, where data is a List of tuples of int64 timestamps and float64 values.

    Parameters
    ----------
    stream : btrdb.stream.Stream, required
        The stream to insert data into.
    data : List[Tuple[int, float]], required
        The data to insert into stream.
    merge_policy : str, optional, default = 'never'
        How should the platform handle duplicated data?
        Valid policies:
        `never`: the default, no points are merged
        `equal`: points are deduplicated if the time and value are equal
        `retain`: if two points have the same timestamp, the old one is kept
        `replace`: if two points have the same timestamp, the new one is kept
    """
    prev_ver = stream.version()
    tic = perf_counter()
    new_ver = stream.insert(data, merge=merge_policy)
    toc = perf_counter()
    run_time = toc - tic
    n_points = len(data)
    result = {
        "uuid": stream.uuid,
        "previous_version": prev_ver,
        "new_version": new_ver,
        "points_to_insert": n_points,
        "total_time_seconds": run_time,
        "merge_policy": merge_policy,
    }
    return result


def time_stream_arrow_insert(
    stream: btrdb.stream.Stream, data: pyarrow.Table, merge_policy: str = "never"
) -> Dict[str, Union[int, float, str]]:
    """Insert raw data into a single stream, where data is a pyarrow Table of timestamps and float values.

    Parameters
    ----------
    stream : btrdb.stream.Stream, required
        The stream to insert data into.
    data : pyarrow.Table, required
        The table of data to insert into stream.
    merge_policy : str, optional, default = 'never'
        How should the platform handle duplicated data?
        Valid policies:
        `never`: the default, no points are merged
        `equal`: points are deduplicated if the time and value are equal
        `retain`: if two points have the same timestamp, the old one is kept
        `replace`: if two points have the same timestamp, the new one is kept
    """
    prev_ver = stream.version()
    tic = perf_counter()
    new_ver = stream.arrow_insert(data, merge=merge_policy)
    toc = perf_counter()
    run_time = toc - tic
    n_points = data.num_rows
    result = {
        "uuid": stream.uuid,
        "previous_version": prev_ver,
        "new_version": new_ver,
        "points_to_insert": n_points,
        "total_time_seconds": run_time,
        "merge_policy": merge_policy,
    }
    return result
