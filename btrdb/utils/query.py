from btrdb.grpcinterface import btrdb_pb2

class QueryType(object):
    @staticmethod
    def ALIGNED_WINDOWS_QUERY():
        return QueryType(btrdb_pb2.GenerateCSVParams.ALIGNED_WINDOWS_QUERY)

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
