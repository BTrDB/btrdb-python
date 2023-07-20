# btrdb.conn
# Connection related objects for the BTrDB library
#
# Author:   PingThings
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# For license information, see LICENSE.txt
# ID: conn.py [] allen@pingthings.io $

"""
Connection related objects for the BTrDB library
"""

##########################################################################
## Imports
##########################################################################

import json
import logging
import os
import re
import uuid as uuidlib
from concurrent.futures import ThreadPoolExecutor

import certifi
import grpc
from grpc._cython.cygrpc import CompressionAlgorithm

from btrdb.exceptions import InvalidOperation, StreamNotFoundError, retry
from btrdb.stream import Stream, StreamSet
from btrdb.utils.conversion import to_uuid
from btrdb.utils.general import unpack_stream_descriptor

##########################################################################
## Module Variables
##########################################################################

MIN_TIME = -(16 << 56)
MAX_TIME = 48 << 56
MAX_POINTWIDTH = 63


##########################################################################
## Classes
##########################################################################
logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(self, addrportstr, apikey=None):
        """
        Connects to a BTrDB server

        Parameters
        ----------
        addrportstr: str, required
            The address of the cluster to connect to, e.g 123.123.123:4411
        apikey: str, optional
            The optional API key to authenticate requests

        """
        addrport = addrportstr.split(":", 2)
        chan_ops = []

        if len(addrport) != 2:
            raise ValueError("expecting address:port")

        if addrport[1] == "4411":
            # grpc bundles its own CA certs which will work for all normal SSL
            # certificates but will fail for custom CA certs. Allow the user
            # to specify a CA bundle via env var to overcome this
            env_bundle = os.getenv("BTRDB_CA_BUNDLE", "")

            # certifi certs are provided as part of this package install
            # https://github.com/certifi/python-certifi
            lib_certs = certifi.where()

            ca_bundle = env_bundle

            if ca_bundle == "":
                ca_bundle = lib_certs
            try:
                with open(ca_bundle, "rb") as f:
                    contents = f.read()
            except Exception:
                if env_bundle != "":
                    # The user has given us something but we can't use it, we need to make noise
                    raise Exception(
                        "BTRDB_CA_BUNDLE(%s) env is defined but could not read file"
                        % ca_bundle
                    )
                else:
                    contents = None

            if apikey is None:
                self.channel = grpc.secure_channel(
                    addrportstr,
                    grpc.ssl_channel_credentials(contents),
                    options=chan_ops,
                )
            else:
                self.channel = grpc.secure_channel(
                    addrportstr,
                    grpc.composite_channel_credentials(
                        grpc.ssl_channel_credentials(contents),
                        grpc.access_token_call_credentials(apikey),
                    ),
                    options=chan_ops,
                )
        else:
            self.channel = grpc.insecure_channel(addrportstr, chan_ops)
            if apikey is not None:

                class AuthCallDetails(grpc.ClientCallDetails):
                    def __init__(self, apikey, client_call_details):
                        metadata = []
                        if client_call_details.metadata is not None:
                            metadata = list(client_call_details.metadata)
                        metadata.append(("authorization", "Bearer " + apikey))
                        self.method = client_call_details.method
                        self.timeout = client_call_details.timeout
                        self.credentials = client_call_details.credentials
                        self.wait_for_ready = client_call_details.wait_for_ready
                        self.compression = client_call_details.compression
                        self.metadata = metadata

                class AuthorizationInterceptor(
                    grpc.UnaryUnaryClientInterceptor,
                    grpc.UnaryStreamClientInterceptor,
                    grpc.StreamUnaryClientInterceptor,
                    grpc.StreamStreamClientInterceptor,
                ):
                    def __init__(self, apikey):
                        self.apikey = apikey

                    def intercept_unary_unary(
                        self, continuation, client_call_details, request
                    ):
                        return continuation(
                            AuthCallDetails(self.apikey, client_call_details), request
                        )

                    def intercept_unary_stream(
                        self, continuation, client_call_details, request
                    ):
                        return continuation(
                            AuthCallDetails(self.apikey, client_call_details), request
                        )

                    def intercept_stream_unary(
                        self, continuation, client_call_details, request_iterator
                    ):
                        return continuation(
                            AuthCallDetails(self.apikey, client_call_details),
                            request_iterator,
                        )

                    def intercept_stream_stream(
                        self, continuation, client_call_details, request_iterator
                    ):
                        return continuation(
                            AuthCallDetails(self.apikey, client_call_details),
                            request_iterator,
                        )

                self.channel = grpc.intercept_channel(
                    self.channel,
                    AuthorizationInterceptor(apikey),
                )


def _is_arrow_enabled(info):
    info = {
        "majorVersion": info.majorVersion,
        "minorVersion": info.minorVersion,
    }
    major = info.get("majorVersion", -1)
    minor = info.get("minorVersion", -1)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"major version: {major}")
        logger.debug(f"minor version: {minor}")
    if major >= 5 and minor >= 30:
        return True
    else:
        return False


class BTrDB(object):
    """
    The primary server connection object for communicating with a BTrDB server.
    """

    def __init__(self, endpoint):
        self.ep = endpoint
        self._executor = ThreadPoolExecutor()
        try:
            self._ARROW_ENABLED = _is_arrow_enabled(self.ep.info())
        except Exception:
            self._ARROW_ENABLED = False
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"ARROW ENABLED: {self._ARROW_ENABLED}")

    @retry
    def query(
        self,
        stmt,
        params=None,
        auto_retry=False,
        retries=5,
        retry_delay=3,
        retry_backoff=4,
    ):
        """
        Performs a SQL query on the database metadata and returns a list of
        dictionaries from the resulting cursor.

        Parameters
        ----------
        stmt: str
            a SQL statement to be executed on the BTrDB metadata.  Available
            tables are noted below.  To sanitize inputs use a `$1` style parameter such as
            `select * from streams where name = $1 or name = $2`.
        params: list or tuple
            a list of parameter values to be sanitized and interpolated into the
            SQL statement. Using parameters forces value/type checking and is
            considered a best practice at the very least.
        auto_retry: bool, default: False
            Whether to retry this request in the event of an error
        retries: int, default: 5
            Number of times to retry this request if there is an error. Will
            be ignored if auto_retry is False
        retry_delay: int, default: 3
            initial time to wait before retrying function call if there is an error.
            Will be ignored if auto_retry is False
        retry_backoff: int, default: 4
            Exponential factor by which the backoff increases between retries.
            Will be ignored if auto_retry is False

        Returns
        -------
        list
            a list of dictionary object representing the cursor results.


        Notes
        -------
        Parameters will be inserted into the SQL statement as noted by the
        paramter number such as `$1`, `$2`, or `$3`.  The `streams` table is
        available for `SELECT` statements only.

        See https://btrdb.readthedocs.io/en/latest/ for more info.
        """
        if params is None:
            params = list()
        return [
            json.loads(row.decode("utf-8"))
            for page in self.ep.sql_query(stmt, params)
            for row in page
        ]

    def streams(self, *identifiers, versions=None, is_collection_prefix=False):
        """
        Returns a StreamSet object with BTrDB streams from the supplied
        identifiers.  If any streams cannot be found matching the identifier
        than StreamNotFoundError will be returned.

        Parameters
        ----------
        identifiers: str or UUID
            a single item or iterable of items which can be used to query for
            streams.  identiers are expected to be UUID as string, UUID as UUID,
            or collection/name string.

        versions: list[int]
            a single or iterable of version numbers to match the identifiers

        """
        if versions is not None and not isinstance(versions, list):
            raise TypeError("versions argument must be of type list")

        if versions and len(versions) != len(identifiers):
            raise ValueError("number of versions does not match identifiers")

        streams = []
        for ident in identifiers:
            if isinstance(ident, uuidlib.UUID):
                streams.append(self.stream_from_uuid(ident))
                continue

            if isinstance(ident, str):
                # attempt UUID lookup
                pattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                if re.match(pattern, ident):
                    streams.append(self.stream_from_uuid(ident))
                    continue

                # attempt collection/name lookup
                if "/" in ident:
                    parts = ident.split("/")
                    found = self.streams_in_collection(
                        "/".join(parts[:-1]),
                        is_collection_prefix=is_collection_prefix,
                        tags={"name": parts[-1]},
                    )
                    if len(found) == 1:
                        streams.append(found[0])
                        continue
                    raise StreamNotFoundError(f"Could not identify stream `{ident}`")

            raise ValueError(
                f"Could not identify stream based on `{ident}`.  Identifier must be UUID or collection/name."
            )

        obj = StreamSet(streams)

        if versions:
            version_dict = {
                streams[idx].uuid: versions[idx] for idx in range(len(versions))
            }
            obj.pin_versions(version_dict)

        return obj

    def stream_from_uuid(self, uuid):
        """
        Creates a stream handle to the BTrDB stream with the UUID `uuid`. This
        method does not check whether a stream with the specified UUID exists.
        It is always good form to check whether the stream existed using
        `stream.exists()`.


        Parameters
        ----------
        uuid: UUID
            The uuid of the requested stream.

        Returns
        -------
        Stream
            instance of Stream class or None

        """
        return Stream(self, to_uuid(uuid))

    @retry
    def create(
        self,
        uuid,
        collection,
        tags=None,
        annotations=None,
        auto_retry=False,
        retries=5,
        retry_delay=3,
        retry_backoff=4,
    ):
        """
        Tells BTrDB to create a new stream with UUID `uuid` in `collection` with specified `tags` and `annotations`.

        Parameters
        ----------
        uuid: UUID, required
            The uuid of the requested stream.
        collection: str, required
            The collection string prefix that the stream will belong to.
        tags: dict, required
            The tags-level immutable metadata key:value pairs.
        annotations: dict, optional
            The mutable metadata of the stream, key:value pairs
        auto_retry: bool, default: False
            Whether to retry this request in the event of an error
        retries: int, default: 5
            Number of times to retry this request if there is an error. Will
            be ignored if auto_retry is False
        retry_delay: int, default: 3
            initial time to wait before retrying function call if there is an error.
            Will be ignored if auto_retry is False
        retry_backoff: int, default: 4
            Exponential factor by which the backoff increases between retries.
            Will be ignored if auto_retry is False

        Returns
        -------
        Stream
            instance of Stream class
        """

        if tags is None:
            tags = {}

        if annotations is None:
            annotations = {}

        self.ep.create(uuid, collection, tags, annotations)
        return Stream(
            self,
            uuid,
            known_to_exist=True,
            collection=collection,
            tags=tags.copy(),
            annotations=annotations.copy(),
            property_version=0,
        )

    def info(self):
        """
        Returns information about the connected BTrDB server.

        Returns
        -------
        dict
            server connection and status information

        """
        info = self.ep.info()
        return {
            "majorVersion": info.majorVersion,
            "minorVersion": info.minorVersion,
            "build": info.build,
            "proxy": {"proxyEndpoints": [ep for ep in info.proxy.proxyEndpoints]},
        }

    def list_collections(self, starts_with=""):
        """
        Returns a list of collection paths using the `starts_with` argument for
        filtering.

        Parameters
        ----------
        starts_with: str, optional, default = ''
            Filter collections that start with the string provided, if none passed, will list all collections.

        Returns
        -------
        collections: List[str]

        """
        return [c for some in self.ep.listCollections(starts_with) for c in some]

    def _list_unique_tags_annotations(self, key, collection):
        """
        Returns a SQL statement and parameters to get list of tags or annotations.
        """
        if key == "annotations":
            query = "select distinct({}) as {} from streams".format(
                "skeys(annotations)", "annotations"
            )
        else:
            query = "select distinct({}) as {} from streams".format(key, key)
        params = []
        if isinstance(collection, str):
            params.append("{}%".format(collection))
            query = " where ".join([query, """collection like $1"""])
        return [metadata[key] for metadata in self.query(query, params)]

    def list_unique_annotations(self, collection=None):
        """
        Returns a list of annotation keys used in a given collection prefix.

        Parameters
        -------
        collection: str
            Prefix of the collection to filter.

        Returns
        -------
        annotations: list[str]
        """
        return self._list_unique_tags_annotations("annotations", collection)

    def list_unique_names(self, collection=None):
        """
        Returns a list of names used in a given collection prefix.

        Parameters
        -------
        collection: str
            Prefix of the collection to filter.

        Returns
        -------
        names: list[str]
        """
        return self._list_unique_tags_annotations("name", collection)

    def list_unique_units(self, collection=None):
        """
        Returns a list of units used in a given collection prefix.

        Parameters
        -------
        collection: str
            Prefix of the collection to filter.

        Returns
        -------
        units: list[str]
        """
        return self._list_unique_tags_annotations("unit", collection)

    @retry
    def streams_in_collection(
        self,
        *collection,
        is_collection_prefix=True,
        tags=None,
        annotations=None,
        auto_retry=False,
        retries=5,
        retry_delay=3,
        retry_backoff=4,
    ):
        """
        Search for streams matching given parameters

        This function allows for searching

        Parameters
        ----------
        collection: str
            collections to use when searching for streams, case sensitive.
        is_collection_prefix: bool
            Whether the collection is a prefix.
        tags: Dict[str, str]
            The tags to identify the stream.
        annotations: Dict[str, str]
            The annotations to identify the stream.
        auto_retry: bool, default: False
            Whether to retry this request in the event of an error
        retries: int, default: 5
            Number of times to retry this request if there is an error. Will
            be ignored if auto_retry is False
        retry_delay: int, default: 3
            initial time to wait before retrying function call if there is an error.
            Will be ignored if auto_retry is False
        retry_backoff: int, default: 4
            Exponential factor by which the backoff increases between retries.
            Will be ignored if auto_retry is False

        Returns
        ------
        list
            A list of stream objects found with the provided search arguments.

        """
        result = []

        if tags is None:
            tags = {}

        if annotations is None:
            annotations = {}

        if not collection:
            collection = [None]

        for item in collection:
            streams = self.ep.lookupStreams(
                item, is_collection_prefix, tags, annotations
            )
            for desclist in streams:
                for desc in desclist:
                    tagsanns = unpack_stream_descriptor(desc)
                    result.append(
                        Stream(
                            self,
                            uuidlib.UUID(bytes=desc.uuid),
                            known_to_exist=True,
                            collection=desc.collection,
                            tags=tagsanns[0],
                            annotations=tagsanns[1],
                            property_version=desc.propertyVersion,
                        )
                    )

        return result

    @retry
    def collection_metadata(
        self,
        prefix,
        auto_retry=False,
        retries=5,
        retry_delay=3,
        retry_backoff=4,
    ):
        """
        Gives statistics about metadata for collections that match a
        prefix.

        Parameters
        ----------
        prefix: str, required
            A prefix of the collection names to look at
        auto_retry: bool, default: False
            Whether to retry this request in the event of an error
        retries: int, default: 5
            Number of times to retry this request if there is an error. Will
            be ignored if auto_retry is False
        retry_delay: int, default: 3
            initial time to wait before retrying function call if there is an error.
            Will be ignored if auto_retry is False
        retry_backoff: int, default: 4
            Exponential factor by which the backoff increases between retries.
            Will be ignored if auto_retry is False

        Returns
        -------
        tuple
            A tuple of dictionaries containing metadata on the streams in the
            provided collection.

        """
        ep = self.ep
        tags, annotations = ep.getMetadataUsage(prefix)
        pyTags = {tag.key: tag.count for tag in tags}
        pyAnn = {ann.key: ann.count for ann in annotations}
        return pyTags, pyAnn

    def __reduce__(self):
        raise InvalidOperation("BTrDB object cannot be reduced.")
