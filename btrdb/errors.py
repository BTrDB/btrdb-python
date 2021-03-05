# btrdb.errors
# Module for error handling
#
# Author:   PingThings
# Created:  Thurs Feb 25 2021
#
# For license information, see LICENSE.txt
# ID: errors.py [] michael.chestnut@pingthings.io $

##########################################################################
## Imports
##########################################################################

import grpc
import inspect
from functools import partial, wraps
from btrdb.exceptions import (
    BTrDBError,
    BTRDBServerError,
    NoSuchPoint,
    StreamNotFoundError,
    InvalidCollection,
    InvalidTagKey,
    InvalidTagValue,
    InvalidTimeRange,
    InvalidPointWidth,
    StreamExists,
    AmbiguousStream,
    BadValue,
    RecycledUUID,
    BadSQLValue,
    VersionNotAvailable,
    PermissionDenied
)

##########################################################################
## Error mapping
##########################################################################

# Errors that we have custom Exceptions for
BTRDB_ERRORS = {
    401: NoSuchPoint,
    404: StreamNotFoundError,
    407: InvalidCollection,
    408: InvalidTagKey,
    409: InvalidTagValue,
    413: InvalidTimeRange,
    415: InvalidPointWidth,
    417: StreamExists,
    418: AmbiguousStream,
    425: BadValue,
    429: RecycledUUID,
    441: BadSQLValue,
    450: VersionNotAvailable
}

# All of these raise BTRDBServerError
BTRDB_SERVER_ERRORS = [
    402,    # ContextError
    403,    # InsertFailure
    405,    # WrongEndpoint
    414,    # InsertTooBig
    421,    # WrongArgs
    423,    # AnnotationVersionMismatch
    424,    # FaultInjectionDisabled
    426,    # ResourceDepleted
    427,    # InvalidVersions
    431,    # ObliterateDisabled
    432,    # CephError
    433,    # NodeExisted
    434,    # JournalError
    438,    # InvalidParameter
    440,    # MetadataConnectionError
    452,    # OverlappingTrimRange
    453,    # LockFailed
    454,    # NoLeafNode
    455,    # TierCacheError
    456,    # StreamEmpty
    457,    # TieredStorageBackendError
    458,    # TieredStorageOutOfBounds
    459,    # TieredStorageTemporaryError
    500,    # InvariantFailure
    501,    # NotImplemented
    502,    # UnsupportedRollback
]

##########################################################################
## Decorators
##########################################################################

def consume_generator(fn, *args, **kwargs):
    yield from fn(*args, **kwargs)

def error_handler(fn):
    """
    Base decorator that checks endpoint functions for grpc.RpcErrors

    Parameters
    ----------
    fn: function
    """
    # allows input func to keep its name and metadata
    @wraps(fn)
    def wrap(*args, **kwargs):
        try:
            if inspect.isgeneratorfunction(fn):
                # putting yield directly in this function turns it into a generator,
                # so keeping it separate
                consume_generator(fn, *args, **kwargs)
            return fn(*args, **kwargs)
        except grpc.RpcError as e:
            handle_grpc_error(e)
    return wrap

##########################################################################
## gRPC error handling
##########################################################################

# NOTE: this function relies on matching strings and isn't really ideal.
# this is more of a band-aid solution while we figure out how to send
# better errors from btrdb-server
def handle_grpc_error(err):
    """
    Called by endpoint functions when a gRPC error is encountered.
    Checks error details strings to catch known errors, if error is not
    known then a generic BTrDBError gets raised

    Parameters
    ----------
    err: grpc.RpcError
    """
    details = err.details()
    if details == "[404] stream does not exist":
        raise StreamNotFoundError("Stream not found with provided uuid") from None
    elif details == "failed to connect to all addresses":
        raise ConnectionError("Failed to connect to BTrDB") from None
    elif any(str(e) in err.details() for e in BTRDB_SERVER_ERRORS):
        raise BTRDBServerError("An error has occured with btrdb-server") from None
    elif str(err.code()) == "StatusCode.PERMISSION_DENIED":
        raise PermissionDenied(details) from None
    raise BTrDBError(details) from None


def check_proto_stat(stat):
    """
    Checks status of result after gRPC request and raises appropriate
    error based on status code

    Parameters
    ----------
    stat: btrdb_pb2.Status
    """
    code = stat.code
    if code != 0:
        if code in BTRDB_ERRORS:
            raise BTRDB_ERRORS[code](stat.msg)
        elif code in BTRDB_SERVER_ERRORS:
            raise BTRDBServerError(stat.msg)
        raise BTrDBError(stat.msg)