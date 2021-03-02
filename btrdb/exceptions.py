# btrdb.exceptions
# Module for custom exceptions
#
# Author:   PingThings
# Created:  Tue Dec 18 14:50:05 2018 -0500
#
# For license information, see LICENSE.txt
# ID: exceptions.py [] allen@pingthings.io $

##########################################################################
## BTrDB Exceptions
##########################################################################

class BTrDBError(Exception):
    """
    The primary exception for grpc related errors.
    """
    pass

class ConnectionError(BTrDBError):
    """
    An error has occurred while trying to establish a connection with BTrDB.
    """
    pass

class StreamNotFoundError(BTrDBError):
    """
    A problem interacting with the BTrDB server.
    """
    pass

class CredentialsFileNotFound(FileNotFoundError, BTrDBError):
    """
    The credentials file could not be found.
    """
    pass

class ProfileNotFound(BTrDBError):
    """
    A requested profile could not be found in the credentials file.
    """
    pass

class BTRDBServerError(BTrDBError):
    """
    An error occured with btrdb-server.
    """
    pass

class BTRDBTypeError(TypeError, BTrDBError):
    """
    A problem interacting with the BTrDB server.
    """
    pass

class InvalidOperation(BTrDBError):
    """
    An invalid BTrDB operation has been requested.
    """
    pass

class StreamExists(InvalidOperation):
    """
    Create() has been attempted and the uuid already exists
    """
    pass

class AmbiguousStream(InvalidOperation):
    """
    Create() has been attempted and uuid is different, but collection and tags already exist
    """
    pass

class BTRDBValueError(ValueError, BTrDBError):
    """
    An invalid value has been passed to a BTrDB operation.
    """
    pass

class InvalidCollection(BTRDBValueError):
    """
    Collection name is invalid. It is either too long or not a valid string
    """
    pass

class InvalidTagKey(BTRDBValueError):
    """
    Tag key is invalid. Must be one of ("name", "unit", "ingress", "distiller")
    """
    pass

class InvalidTagValue(BTRDBValueError):
    """
    Tag value is invalid. It is either too long or not a valid string
    """
    pass

class InvalidTimeRange(BTRDBValueError):
    """
    Insert contains a timestamp outside the range of (btrdb.MINIMUM_TIME, btrdb.MAXIMUM_TIME)
    """
    pass

class InvalidPointWidth(BTRDBValueError):
    """
    Valid pointwidths are (0, 64)
    """
    pass

class BadValue(BTRDBValueError):
    """
    Returned when you try to insert None values
    """
    pass

class RecycledUUID(BTRDBValueError):
    """
    Returned if you try to create a stream with a uuid that matches a previously deleted stream
    """
    pass

class BadSQLValue(BTRDBValueError):
    """
    Invalid parameters have been passed to metadata db
    """
    pass

class VersionNotAvailable(BTRDBValueError):
    """
    When querying a stream at a pruned version
    """
    pass

class NoSuchPoint(BTRDBValueError):
    """
    If you ask for next/previous point and there isn't one
    """
    pass