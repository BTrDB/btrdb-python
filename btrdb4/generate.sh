#!/bin/bash
# run this from the btrdb4-python root directory
python -m grpc_tools.protoc -Ibtrdb4/grpcinterface --python_out=btrdb4 --grpc_python_out=btrdb4 btrdb4/grpcinterface/btrdb.proto

# afterwards edit btrdb_pb2_grpc.py to say
# from btrdb4 import btrdb_pb2 as btrdb__pb2
# instead of
# import btrdb_pb2 as btrdb__pb2
