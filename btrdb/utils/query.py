from btrdb.grpcinterface import btrdb_pb2

class QueryType(object):
    # TODO: class method
    @classmethod
    def ALIGNED_WINDOWS_QUERY(cls):
        return cls(btrdb_pb2.GenerateCSVParams.ALIGNED_WINDOWS_QUERY)

    @staticmethod
    def WINDOWS_QUERY():
        return QueryType(btrdb_pb2.GenerateCSVParams.WINDOWS_QUERY)

    @staticmethod
    def RAW_QUERY():
        return QueryType(btrdb_pb2.GenerateCSVParams.RAW_QUERY)

    def __init__(self, protoEnum):
        self.enum = protoEnum

    def toProto(self):
        return self.enum


import enum
from btrdb.grpcinterface import btrdb_pb2

class QueryType(enum.Enum):

    ALIGNED_WINDOWS_QUERY = btrdb_pb2.GenerateCSVParams.ALIGNED_WINDOWS_QUERY
    WINDOWS_QUERY = btrdb_pb2.GenerateCSVParams.WINDOWS_QUERY
    RAW_QUERY = btrdb_pb2.GenerateCSVParams.RAW_QUERY

    def toProto(self):
        return self.value


# QueryType.RAW_QUERY() --> QueryType.RAW_QUERY

type QueryType int

const QueryTypes (
    ALIGNED_WINDOWS_QUERY QueryType = iota
    WINDOWS_QUERY
    RAW_QUERY
)
