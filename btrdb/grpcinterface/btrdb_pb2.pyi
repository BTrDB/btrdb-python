from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
EQUAL: MergePolicy
NEVER: MergePolicy
REPLACE: MergePolicy
RETAIN: MergePolicy

class AlignedWindowsParams(_message.Message):
    __slots__ = ["end", "pointWidth", "start", "uuid", "versionMajor"]
    END_FIELD_NUMBER: _ClassVar[int]
    POINTWIDTH_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    end: int
    pointWidth: int
    start: int
    uuid: bytes
    versionMajor: int
    def __init__(self, uuid: _Optional[bytes] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., versionMajor: _Optional[int] = ..., pointWidth: _Optional[int] = ...) -> None: ...

class AlignedWindowsResponse(_message.Message):
    __slots__ = ["stat", "values", "versionMajor", "versionMinor"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    values: _containers.RepeatedCompositeFieldContainer[StatPoint]
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ..., values: _Optional[_Iterable[_Union[StatPoint, _Mapping]]] = ...) -> None: ...

class ChangedRange(_message.Message):
    __slots__ = ["end", "start"]
    END_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    end: int
    start: int
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class ChangesParams(_message.Message):
    __slots__ = ["fromMajor", "resolution", "toMajor", "uuid"]
    FROMMAJOR_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    TOMAJOR_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    fromMajor: int
    resolution: int
    toMajor: int
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., fromMajor: _Optional[int] = ..., toMajor: _Optional[int] = ..., resolution: _Optional[int] = ...) -> None: ...

class ChangesResponse(_message.Message):
    __slots__ = ["ranges", "stat", "versionMajor", "versionMinor"]
    RANGES_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    ranges: _containers.RepeatedCompositeFieldContainer[ChangedRange]
    stat: Status
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ..., ranges: _Optional[_Iterable[_Union[ChangedRange, _Mapping]]] = ...) -> None: ...

class CreateParams(_message.Message):
    __slots__ = ["annotations", "collection", "tags", "uuid"]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    annotations: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    collection: str
    tags: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., collection: _Optional[str] = ..., tags: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ..., annotations: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ...) -> None: ...

class CreateResponse(_message.Message):
    __slots__ = ["stat"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ...) -> None: ...

class DeleteParams(_message.Message):
    __slots__ = ["end", "start", "uuid"]
    END_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    end: int
    start: int
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., start: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ["stat", "versionMajor", "versionMinor"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ...) -> None: ...

class FaultInjectParams(_message.Message):
    __slots__ = ["params", "type"]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    params: bytes
    type: int
    def __init__(self, type: _Optional[int] = ..., params: _Optional[bytes] = ...) -> None: ...

class FaultInjectResponse(_message.Message):
    __slots__ = ["rv", "stat"]
    RV_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    rv: bytes
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., rv: _Optional[bytes] = ...) -> None: ...

class FlushParams(_message.Message):
    __slots__ = ["uuid"]
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ...) -> None: ...

class FlushResponse(_message.Message):
    __slots__ = ["stat", "versionMajor", "versionMinor"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ...) -> None: ...

class GenerateCSVParams(_message.Message):
    __slots__ = ["depth", "endTime", "includeVersions", "queryType", "startTime", "streams", "windowSize"]
    class QueryType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    ALIGNED_WINDOWS_QUERY: GenerateCSVParams.QueryType
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    ENDTIME_FIELD_NUMBER: _ClassVar[int]
    INCLUDEVERSIONS_FIELD_NUMBER: _ClassVar[int]
    QUERYTYPE_FIELD_NUMBER: _ClassVar[int]
    RAW_QUERY: GenerateCSVParams.QueryType
    STARTTIME_FIELD_NUMBER: _ClassVar[int]
    STREAMS_FIELD_NUMBER: _ClassVar[int]
    WINDOWSIZE_FIELD_NUMBER: _ClassVar[int]
    WINDOWS_QUERY: GenerateCSVParams.QueryType
    depth: int
    endTime: int
    includeVersions: bool
    queryType: GenerateCSVParams.QueryType
    startTime: int
    streams: _containers.RepeatedCompositeFieldContainer[StreamCSVConfig]
    windowSize: int
    def __init__(self, queryType: _Optional[_Union[GenerateCSVParams.QueryType, str]] = ..., startTime: _Optional[int] = ..., endTime: _Optional[int] = ..., windowSize: _Optional[int] = ..., depth: _Optional[int] = ..., includeVersions: bool = ..., streams: _Optional[_Iterable[_Union[StreamCSVConfig, _Mapping]]] = ...) -> None: ...

class GenerateCSVResponse(_message.Message):
    __slots__ = ["isHeader", "row", "stat"]
    ISHEADER_FIELD_NUMBER: _ClassVar[int]
    ROW_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    isHeader: bool
    row: _containers.RepeatedScalarFieldContainer[str]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., isHeader: bool = ..., row: _Optional[_Iterable[str]] = ...) -> None: ...

class GetCompactionConfigParams(_message.Message):
    __slots__ = ["uuid"]
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ...) -> None: ...

class GetCompactionConfigResponse(_message.Message):
    __slots__ = ["CompactedVersion", "LatestMajorVersion", "reducedResolutionRanges", "stat", "unused0"]
    COMPACTEDVERSION_FIELD_NUMBER: _ClassVar[int]
    CompactedVersion: int
    LATESTMAJORVERSION_FIELD_NUMBER: _ClassVar[int]
    LatestMajorVersion: int
    REDUCEDRESOLUTIONRANGES_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    UNUSED0_FIELD_NUMBER: _ClassVar[int]
    reducedResolutionRanges: _containers.RepeatedCompositeFieldContainer[ReducedResolutionRange]
    stat: Status
    unused0: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., LatestMajorVersion: _Optional[int] = ..., CompactedVersion: _Optional[int] = ..., reducedResolutionRanges: _Optional[_Iterable[_Union[ReducedResolutionRange, _Mapping]]] = ..., unused0: _Optional[int] = ...) -> None: ...

class InfoParams(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class InfoResponse(_message.Message):
    __slots__ = ["build", "majorVersion", "mash", "minorVersion", "proxy", "stat"]
    BUILD_FIELD_NUMBER: _ClassVar[int]
    MAJORVERSION_FIELD_NUMBER: _ClassVar[int]
    MASH_FIELD_NUMBER: _ClassVar[int]
    MINORVERSION_FIELD_NUMBER: _ClassVar[int]
    PROXY_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    build: str
    majorVersion: int
    mash: Mash
    minorVersion: int
    proxy: ProxyInfo
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., mash: _Optional[_Union[Mash, _Mapping]] = ..., majorVersion: _Optional[int] = ..., minorVersion: _Optional[int] = ..., build: _Optional[str] = ..., proxy: _Optional[_Union[ProxyInfo, _Mapping]] = ...) -> None: ...

class InsertParams(_message.Message):
    __slots__ = ["merge_policy", "sync", "uuid", "values"]
    MERGE_POLICY_FIELD_NUMBER: _ClassVar[int]
    SYNC_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    merge_policy: MergePolicy
    sync: bool
    uuid: bytes
    values: _containers.RepeatedCompositeFieldContainer[RawPoint]
    def __init__(self, uuid: _Optional[bytes] = ..., sync: bool = ..., merge_policy: _Optional[_Union[MergePolicy, str]] = ..., values: _Optional[_Iterable[_Union[RawPoint, _Mapping]]] = ...) -> None: ...

class InsertResponse(_message.Message):
    __slots__ = ["stat", "versionMajor", "versionMinor"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ...) -> None: ...

class KeyCount(_message.Message):
    __slots__ = ["count", "key"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    count: int
    key: str
    def __init__(self, key: _Optional[str] = ..., count: _Optional[int] = ...) -> None: ...

class KeyOptValue(_message.Message):
    __slots__ = ["key", "val"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VAL_FIELD_NUMBER: _ClassVar[int]
    key: str
    val: OptValue
    def __init__(self, key: _Optional[str] = ..., val: _Optional[_Union[OptValue, _Mapping]] = ...) -> None: ...

class KeyValue(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class ListCollectionsParams(_message.Message):
    __slots__ = ["prefix", "role"]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    role: Role
    def __init__(self, prefix: _Optional[str] = ..., role: _Optional[_Union[Role, _Mapping]] = ...) -> None: ...

class ListCollectionsResponse(_message.Message):
    __slots__ = ["collections", "stat"]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    collections: _containers.RepeatedScalarFieldContainer[str]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., collections: _Optional[_Iterable[str]] = ...) -> None: ...

class LookupStreamsParams(_message.Message):
    __slots__ = ["annotations", "collection", "isCollectionPrefix", "role", "tags"]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    ISCOLLECTIONPREFIX_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    annotations: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    collection: str
    isCollectionPrefix: bool
    role: Role
    tags: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    def __init__(self, collection: _Optional[str] = ..., isCollectionPrefix: bool = ..., tags: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ..., annotations: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ..., role: _Optional[_Union[Role, _Mapping]] = ...) -> None: ...

class LookupStreamsResponse(_message.Message):
    __slots__ = ["results", "stat"]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[StreamDescriptor]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., results: _Optional[_Iterable[_Union[StreamDescriptor, _Mapping]]] = ...) -> None: ...

class Mash(_message.Message):
    __slots__ = ["healthy", "leader", "leaderRevision", "members", "revision", "totalWeight", "unmapped"]
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    LEADERREVISION_FIELD_NUMBER: _ClassVar[int]
    LEADER_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    TOTALWEIGHT_FIELD_NUMBER: _ClassVar[int]
    UNMAPPED_FIELD_NUMBER: _ClassVar[int]
    healthy: bool
    leader: str
    leaderRevision: int
    members: _containers.RepeatedCompositeFieldContainer[Member]
    revision: int
    totalWeight: int
    unmapped: float
    def __init__(self, revision: _Optional[int] = ..., leader: _Optional[str] = ..., leaderRevision: _Optional[int] = ..., totalWeight: _Optional[int] = ..., healthy: bool = ..., unmapped: _Optional[float] = ..., members: _Optional[_Iterable[_Union[Member, _Mapping]]] = ...) -> None: ...

class Member(_message.Message):
    __slots__ = ["enabled", "end", "grpcEndpoints", "hash", "httpEndpoints", "nodename", "readPreference", "start", "up", "weight"]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    GRPCENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    HTTPENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    IN_FIELD_NUMBER: _ClassVar[int]
    NODENAME_FIELD_NUMBER: _ClassVar[int]
    READPREFERENCE_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    UP_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    end: int
    grpcEndpoints: str
    hash: int
    httpEndpoints: str
    nodename: str
    readPreference: float
    start: int
    up: bool
    weight: int
    def __init__(self, hash: _Optional[int] = ..., nodename: _Optional[str] = ..., up: bool = ..., enabled: bool = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., weight: _Optional[int] = ..., readPreference: _Optional[float] = ..., httpEndpoints: _Optional[str] = ..., grpcEndpoints: _Optional[str] = ..., **kwargs) -> None: ...

class MetadataUsageParams(_message.Message):
    __slots__ = ["prefix", "role"]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    role: Role
    def __init__(self, prefix: _Optional[str] = ..., role: _Optional[_Union[Role, _Mapping]] = ...) -> None: ...

class MetadataUsageResponse(_message.Message):
    __slots__ = ["annotations", "stat", "tags"]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    annotations: _containers.RepeatedCompositeFieldContainer[KeyCount]
    stat: Status
    tags: _containers.RepeatedCompositeFieldContainer[KeyCount]
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., tags: _Optional[_Iterable[_Union[KeyCount, _Mapping]]] = ..., annotations: _Optional[_Iterable[_Union[KeyCount, _Mapping]]] = ...) -> None: ...

class NearestParams(_message.Message):
    __slots__ = ["backward", "time", "uuid", "versionMajor"]
    BACKWARD_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    backward: bool
    time: int
    uuid: bytes
    versionMajor: int
    def __init__(self, uuid: _Optional[bytes] = ..., time: _Optional[int] = ..., versionMajor: _Optional[int] = ..., backward: bool = ...) -> None: ...

class NearestResponse(_message.Message):
    __slots__ = ["stat", "value", "versionMajor", "versionMinor"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    value: RawPoint
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ..., value: _Optional[_Union[RawPoint, _Mapping]] = ...) -> None: ...

class ObliterateParams(_message.Message):
    __slots__ = ["uuid"]
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ...) -> None: ...

class ObliterateResponse(_message.Message):
    __slots__ = ["stat"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ...) -> None: ...

class OptValue(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class ProxyInfo(_message.Message):
    __slots__ = ["proxyEndpoints"]
    PROXYENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    proxyEndpoints: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, proxyEndpoints: _Optional[_Iterable[str]] = ...) -> None: ...

class RawPoint(_message.Message):
    __slots__ = ["time", "value"]
    TIME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    time: int
    value: float
    def __init__(self, time: _Optional[int] = ..., value: _Optional[float] = ...) -> None: ...

class RawValuesParams(_message.Message):
    __slots__ = ["end", "start", "uuid", "versionMajor"]
    END_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    end: int
    start: int
    uuid: bytes
    versionMajor: int
    def __init__(self, uuid: _Optional[bytes] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., versionMajor: _Optional[int] = ...) -> None: ...

class RawValuesResponse(_message.Message):
    __slots__ = ["stat", "values", "versionMajor", "versionMinor"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    values: _containers.RepeatedCompositeFieldContainer[RawPoint]
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ..., values: _Optional[_Iterable[_Union[RawPoint, _Mapping]]] = ...) -> None: ...

class ReducedResolutionRange(_message.Message):
    __slots__ = ["End", "Resolution", "Start"]
    END_FIELD_NUMBER: _ClassVar[int]
    End: int
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    Resolution: int
    START_FIELD_NUMBER: _ClassVar[int]
    Start: int
    def __init__(self, Start: _Optional[int] = ..., End: _Optional[int] = ..., Resolution: _Optional[int] = ...) -> None: ...

class Role(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class SQLQueryParams(_message.Message):
    __slots__ = ["params", "query", "role"]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    params: _containers.RepeatedScalarFieldContainer[str]
    query: str
    role: Role
    def __init__(self, query: _Optional[str] = ..., params: _Optional[_Iterable[str]] = ..., role: _Optional[_Union[Role, _Mapping]] = ...) -> None: ...

class SQLQueryResponse(_message.Message):
    __slots__ = ["SQLQueryRow", "stat"]
    SQLQUERYROW_FIELD_NUMBER: _ClassVar[int]
    SQLQueryRow: _containers.RepeatedScalarFieldContainer[bytes]
    STAT_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., SQLQueryRow: _Optional[_Iterable[bytes]] = ...) -> None: ...

class SetCompactionConfigParams(_message.Message):
    __slots__ = ["CompactedVersion", "reducedResolutionRanges", "unused0", "uuid"]
    COMPACTEDVERSION_FIELD_NUMBER: _ClassVar[int]
    CompactedVersion: int
    REDUCEDRESOLUTIONRANGES_FIELD_NUMBER: _ClassVar[int]
    UNUSED0_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    reducedResolutionRanges: _containers.RepeatedCompositeFieldContainer[ReducedResolutionRange]
    unused0: int
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., CompactedVersion: _Optional[int] = ..., reducedResolutionRanges: _Optional[_Iterable[_Union[ReducedResolutionRange, _Mapping]]] = ..., unused0: _Optional[int] = ...) -> None: ...

class SetCompactionConfigResponse(_message.Message):
    __slots__ = ["stat"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ...) -> None: ...

class SetStreamAnnotationsParams(_message.Message):
    __slots__ = ["changes", "expectedPropertyVersion", "removals", "uuid"]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    EXPECTEDPROPERTYVERSION_FIELD_NUMBER: _ClassVar[int]
    REMOVALS_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    changes: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    expectedPropertyVersion: int
    removals: _containers.RepeatedScalarFieldContainer[str]
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., expectedPropertyVersion: _Optional[int] = ..., changes: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ..., removals: _Optional[_Iterable[str]] = ...) -> None: ...

class SetStreamAnnotationsResponse(_message.Message):
    __slots__ = ["stat"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ...) -> None: ...

class SetStreamTagsParams(_message.Message):
    __slots__ = ["collection", "expectedPropertyVersion", "remove", "tags", "uuid"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    EXPECTEDPROPERTYVERSION_FIELD_NUMBER: _ClassVar[int]
    REMOVE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    collection: str
    expectedPropertyVersion: int
    remove: _containers.RepeatedScalarFieldContainer[str]
    tags: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., expectedPropertyVersion: _Optional[int] = ..., tags: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ..., collection: _Optional[str] = ..., remove: _Optional[_Iterable[str]] = ...) -> None: ...

class SetStreamTagsResponse(_message.Message):
    __slots__ = ["stat"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ...) -> None: ...

class StatPoint(_message.Message):
    __slots__ = ["count", "max", "mean", "min", "stddev", "time"]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    MEAN_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    STDDEV_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    count: int
    max: float
    mean: float
    min: float
    stddev: float
    time: int
    def __init__(self, time: _Optional[int] = ..., min: _Optional[float] = ..., mean: _Optional[float] = ..., max: _Optional[float] = ..., count: _Optional[int] = ..., stddev: _Optional[float] = ...) -> None: ...

class Status(_message.Message):
    __slots__ = ["code", "mash", "msg"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MASH_FIELD_NUMBER: _ClassVar[int]
    MSG_FIELD_NUMBER: _ClassVar[int]
    code: int
    mash: Mash
    msg: str
    def __init__(self, code: _Optional[int] = ..., msg: _Optional[str] = ..., mash: _Optional[_Union[Mash, _Mapping]] = ...) -> None: ...

class StreamCSVConfig(_message.Message):
    __slots__ = ["label", "uuid", "version"]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    label: str
    uuid: bytes
    version: int
    def __init__(self, version: _Optional[int] = ..., label: _Optional[str] = ..., uuid: _Optional[bytes] = ...) -> None: ...

class StreamDescriptor(_message.Message):
    __slots__ = ["annotations", "collection", "propertyVersion", "tags", "uuid"]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    PROPERTYVERSION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    annotations: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    collection: str
    propertyVersion: int
    tags: _containers.RepeatedCompositeFieldContainer[KeyOptValue]
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., collection: _Optional[str] = ..., tags: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ..., annotations: _Optional[_Iterable[_Union[KeyOptValue, _Mapping]]] = ..., propertyVersion: _Optional[int] = ...) -> None: ...

class StreamInfoParams(_message.Message):
    __slots__ = ["omitDescriptor", "omitVersion", "role", "uuid"]
    OMITDESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    OMITVERSION_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    omitDescriptor: bool
    omitVersion: bool
    role: Role
    uuid: bytes
    def __init__(self, uuid: _Optional[bytes] = ..., omitVersion: bool = ..., omitDescriptor: bool = ..., role: _Optional[_Union[Role, _Mapping]] = ...) -> None: ...

class StreamInfoResponse(_message.Message):
    __slots__ = ["descriptor", "stat", "versionMajor", "versionMinor"]
    DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    descriptor: StreamDescriptor
    stat: Status
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ..., descriptor: _Optional[_Union[StreamDescriptor, _Mapping]] = ...) -> None: ...

class WindowsParams(_message.Message):
    __slots__ = ["depth", "end", "start", "uuid", "versionMajor", "width"]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    WIDTH_FIELD_NUMBER: _ClassVar[int]
    depth: int
    end: int
    start: int
    uuid: bytes
    versionMajor: int
    width: int
    def __init__(self, uuid: _Optional[bytes] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., versionMajor: _Optional[int] = ..., width: _Optional[int] = ..., depth: _Optional[int] = ...) -> None: ...

class WindowsResponse(_message.Message):
    __slots__ = ["stat", "values", "versionMajor", "versionMinor"]
    STAT_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    VERSIONMAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSIONMINOR_FIELD_NUMBER: _ClassVar[int]
    stat: Status
    values: _containers.RepeatedCompositeFieldContainer[StatPoint]
    versionMajor: int
    versionMinor: int
    def __init__(self, stat: _Optional[_Union[Status, _Mapping]] = ..., versionMajor: _Optional[int] = ..., versionMinor: _Optional[int] = ..., values: _Optional[_Iterable[_Union[StatPoint, _Mapping]]] = ...) -> None: ...

class MergePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
