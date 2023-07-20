import uuid
from time import perf_counter
from typing import Dict, List, Tuple, Union

import pyarrow

import btrdb


def time_streamset_inserts(
    streamset: btrdb.stream.StreamSet,
    data_map: Dict[uuid.UUID, List[Tuple[int, float]]],
    merge_policy: str = "never",
) -> Dict[str, Union[str, int, float, List, Dict]]:
    """Insert data into a streamset with a provided data map.

    Parameters
    ----------
    streamset : btrdb.stream.StreamSet, required
        The streams to insert data into.
    data_map : Dict[uuid.UUID, List[Tuple[int, float]], required
        The mappings of streamset uuids to data to insert.
    merge_policy : str, optional, default = 'never'
        How should the platform handle duplicated data?
        Valid policies:
        `never`: the default, no points are merged
        `equal`: points are deduplicated if the time and value are equal
        `retain`: if two points have the same timestamp, the old one is kept
        `replace`: if two points have the same timestamp, the new one is kept
    """
    _ensure_all_streams_in_data_map(streamset, data_map)
    prev_vers = streamset.versions()
    tic = perf_counter()
    new_vers = streamset.insert(data_map=data_map, merge=merge_policy)
    toc = perf_counter()
    n_streams = len(streamset)
    total_points = sum([len(v) for v in data_map.values()])
    points_insert_per_stream = {s.uuid: len(data_map[s.uuid]) for s in streamset}
    total_time = toc - tic
    result = {
        "n_streams": n_streams,
        "total_points": total_points,
        "previous_versions": prev_vers,
        "new_versions": new_vers,
        "total_time_seconds": total_time,
        "points_insert_per_stream": points_insert_per_stream,
        "merge_policy": merge_policy,
    }
    return result


def time_streamset_arrow_inserts(
    streamset: btrdb.stream.StreamSet,
    data_map: Dict[uuid.UUID, pyarrow.Table],
    merge_policy: str = "never",
) -> Dict[str, Union[str, int, float, List, Dict]]:
    """Insert data into streamset using pyarrow Tables.

    Parameters
    ----------
    streamset : btrdb.stream.StreamSet, required
        The streams to insert data into.
    data_map : Dict[uuid.UUID, pyarrow.Table], required
        The mappings of streamset uuids to pyarrow tables to insert.
    merge_policy : str, optional, default = 'never'
        How should the platform handle duplicated data?
        Valid policies:
        `never`: the default, no points are merged
        `equal`: points are deduplicated if the time and value are equal
        `retain`: if two points have the same timestamp, the old one is kept
        `replace`: if two points have the same timestamp, the new one is kept
    """
    _ensure_all_streams_in_data_map(streamset, data_map)
    prev_vers = streamset.versions()
    tic = perf_counter()
    new_vers = streamset.arrow_insert(data_map=data_map, merge=merge_policy)
    toc = perf_counter()
    n_streams = len(streamset)
    total_points = sum([v.num_rows for v in data_map.values()])
    points_insert_per_stream = {s.uuid: data_map[s.uuid].num_rows for s in streamset}
    total_time = toc - tic
    result = {
        "n_streams": n_streams,
        "total_points": total_points,
        "previous_versions": prev_vers,
        "new_versions": new_vers,
        "total_time_seconds": total_time,
        "points_insert_per_stream": points_insert_per_stream,
        "merge_policy": merge_policy,
    }
    return result


def _ensure_all_streams_in_data_map(
    streamset: btrdb.stream.StreamSet,
    data_map: Dict[uuid.UUID, Union[pyarrow.Table, List[Tuple[int, float]]]],
) -> None:
    truthy = [s.uuid in data_map for s in streamset]
    uus = [s.uuid for s in streamset]
    if not all(truthy):
        raise ValueError(
            f"Streams in streamset are not present in the data map. Data_map keys: {data_map.keys()}, streamset uuids: {uus}"
        )
