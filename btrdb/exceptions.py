# btrdb.exceptions
# Module for custom exceptions
#
# Author:   PingThings
# Created:  Tue Dec 18 14:50:05 2018 -0500
#
# For license information, see LICENSE.txt
# ID: exceptions.py [] allen@pingthings.io $

##########################################################################
## Imports
##########################################################################

import inspect
from grpc import RpcError
from functools import wraps

##########################################################################
## Decorators
##########################################################################

def consume_generator(fn, *args, **kwargs):
    # when a generator is passed back to the calling function, it may encounter an error
    # when trying to call next(), in that case we want to yield an Exception
    try:
        yield from fn(*args, **kwargs)
    except RpcError as e:
        handle_grpc_error(e)

def error_handler(fn):
    """
    decorates endpoint functions and checks for grpc.RpcErrors

    Parameters
    ----------
    fn: function
    """
    # allows input func to keep its name and metadata
    @wraps(fn)
    def wrap(*args, **kwargs):
        if inspect.isgeneratorfunction(fn):
            return consume_generator(fn, *args, **kwargs)
        try:
            return fn(*args, **kwargs)
        except RpcError as e:
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

##########################################################################
## BTrDB Exceptions
##########################################################################

class BTrDBError(Exception):
    """
    The primary exception for BTrDB errors.
    """
    pass

class ConnectionError(BTrDBError):
    """
    Raised when an error occurrs while trying to establish a connection with BTrDB.
    """
    pass

class StreamNotFoundError(BTrDBError):
    """
    Raised when attempting to perform an operation on a stream that does not exist in
    the specified BTrDB allocation.
    """
    pass

class CredentialsFileNotFound(FileNotFoundError, BTrDBError):
    """
    Raised when a credentials file could not be found.
    """
    pass

class ProfileNotFound(BTrDBError):
    """
    Raised when a requested profile could not be found in the credentials file.
    """
    pass

class BTRDBServerError(BTrDBError):
    """
    Raised when an error occurs with btrdb-server.
    """
    pass

class BTRDBTypeError(TypeError, BTrDBError):
    """
    Raised when attempting to perform an operation with an invalid type.
    """
    pass

class InvalidOperation(BTrDBError):
    """
    Raised when an invalid BTrDB operation has been requested.
    """
    pass

class StreamExists(InvalidOperation):
    """
    Raised when create() has been attempted and the uuid already exists.
    """
    pass

class AmbiguousStream(InvalidOperation):
    """
    Raised when create() has been attempted and uuid is different, but collection and tags already exist
    """
    pass

class PermissionDenied(InvalidOperation):
    """
    Raised when user does not have permission to perform an operation.
    """
    pass

class BTRDBValueError(ValueError, BTrDBError):
    """
    Raised when an invalid value has been passed to a BTrDB operation.
    """
    pass

class InvalidCollection(BTRDBValueError):
    """
    Raised when a collection name is invalid. It is either too long or not a valid string.
    """
    pass

class InvalidTagKey(BTRDBValueError):
    """
    Raised when a tag key is invalid. Must be one of ("name", "unit", "ingress", "distiller").
    """
    pass

class InvalidTagValue(BTRDBValueError):
    """
    Raised when a tag value is invalid. It is either too long or not a valid string.
    """
    pass

class InvalidTimeRange(BTRDBValueError):
    """
    Raised when insert data contains a timestamp outside the range of (btrdb.MINIMUM_TIME, btrdb.MAXIMUM_TIME)
    """
    pass

class InvalidPointWidth(BTRDBValueError):
    """
    Raised when attempting to use a pointwidth that is not a whole number between 0 and 64 (exclusive).
    """
    pass

class BadValue(BTRDBValueError):
    """
    Raised when attempting to insert data that contains non-float values such as None.
    """
    pass

class RecycledUUID(BTRDBValueError):
    """
    Raised when attempting to create a stream with a uuid that matches a previously deleted stream.
    """
    pass

class BadSQLValue(BTRDBValueError):
    """
    Raised when invalid parameters have been passed to metadata db.
    """
    pass

class VersionNotAvailable(BTRDBValueError):
    """
    Raised when querying a stream at a pruned or otherwise invalid version number.
    """
    pass

class NoSuchPoint(BTRDBValueError):
    """
    Raised when asking for next/previous point and there isn't one.
    """
    pass


##########################################################################
## Exception mapping
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