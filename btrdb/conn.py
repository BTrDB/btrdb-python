# Copyright (c) 2017 Sam Kumar <samkumar@berkeley.edu>
# Copyright (c) 2017 Michael P Andersen <m.andersen@cs.berkeley.edu>
# Copyright (c) 2017 University of California, Berkeley
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the University of California, Berkeley nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNERS OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
The 'btrdb' module provides Python bindings to interact with BTrDB.
"""

import grpc
import uuid as uuidlib
import os

from grpc._cython.cygrpc import CompressionAlgorithm

from btrdb.stream import Stream, StreamSet
from btrdb.utils.general import unpack_stream_descriptor
from btrdb.utils.conversion import to_uuid

MIN_TIME = -(16 << 56)
MAX_TIME = 48 << 56
MAX_POINTWIDTH = 63


class Connection(object):

    def __init__(self, addrportstr, apikey=None):
        """
        Connects to a BTrDB server

        Parameters
        ----------
        addrportstr: str
            The address of the cluster to connect to, e.g 123.123.123:4411
        apikey: str
            The option API key to authenticate requests

        """
        addrport = addrportstr.split(":", 2)
        chan_ops = [('grpc.default_compression_algorithm', CompressionAlgorithm.gzip)]

        if len(addrport) != 2:
            raise ValueError("expecting address:port")

        if addrport[1] == "4411":
            # grpc bundles its own CA certs which will work for all normal SSL
            # certificates but will fail for custom CA certs. Allow the user
            # to specify a CA bundle via env var to overcome this
            ca_bundle = os.getenv("BTRDB_CA_BUNDLE","")
            if ca_bundle != "":
                with open(ca_bundle, "rb") as f:
                    contents = f.read()
            else:
                contents = None

            if apikey is None:
                self.channel = grpc.secure_channel(
                    addrportstr,
                    grpc.ssl_channel_credentials(contents),
                    options=chan_ops
                )
            else:
                self.channel = grpc.secure_channel(
                    addrportstr,
                    grpc.composite_channel_credentials(
                        grpc.ssl_channel_credentials(contents),
                        grpc.access_token_call_credentials(apikey)
                    ),
                    options=chan_ops
                )
        else:
            if apikey is not None:
                raise ValueError("cannot use an API key with an insecure (port 4410) BTrDB API. Try port 4411")
            self.channel = grpc.insecure_channel(addrportstr, chan_ops)




class BTrDB(object):
    """
    The primary server connection object for communicating with a BTrDB server.
    """

    def __init__(self, endpoint):
        self.ep = endpoint

    def streams(self, *identifiers, versions=None):
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

        streams = [self.stream_from_uuid(ident) for ident in identifiers]
        obj = StreamSet(streams)

        if versions:
            version_dict = {streams[idx].uuid: versions[idx] for idx in range(len(versions))}
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

    def create(self, uuid, collection, tags=None, annotations=None):
        """
        Tells BTrDB to create a new stream with UUID `uuid` in `collection` with specified `tags` and `annotations`.

        Parameters
        ----------
        uuid: UUID
            The uuid of the requested stream.

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
        return Stream(self, uuid,
            known_to_exist=True,
            collection=collection,
            tags=tags.copy(),
            annotations=annotations.copy(),
            property_version=0
        )

    def info(self):
        """
        Returns information about the connected BTrDB srerver.

        Returns
        -------
        dict
            server connection and status information

        """
        info = self.ep.info()
        return {
            "majorVersion": info.majorVersion,
            "build": info.build,
            "proxy": { "proxyEndpoints": [ep for ep in info.proxy.proxyEndpoints] },
        }

    def list_collections(self, starts_with=""):
        """
        Returns a list of collection paths using the `starts_with` argument for
        filtering.

        Returns
        -------
        collection paths: list[str]

        """
        return [c for some in self.ep.listCollections(starts_with) for c in some]

    def streams_in_collection(self, *collection, is_collection_prefix=True, tags=None, annotations=None):
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

        for item in collection:
            streams = self.ep.lookupStreams(item, is_collection_prefix, tags, annotations)
            for desclist in streams:
                for desc in desclist:
                    tagsanns = unpack_stream_descriptor(desc)
                    result.append(Stream(
                        self, uuidlib.UUID(bytes = desc.uuid),
                        known_to_exist=True, collection=desc.collection,
                        tags=tagsanns[0], annotations=tagsanns[1],
                        property_version=desc.propertyVersion
                    ))

        return result

    def collection_metadata(self, prefix):
        """
        Gives statistics about metadata for collections that match a
        prefix.

        Parameters
        ----------
        prefix: str
            A prefix of the collection names to look at

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
