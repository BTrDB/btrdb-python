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
import uuid
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
        # type: () -> BTrDB
        """
        Connects to a BTrDB server

        Parameters
        ----------
        addrportstr: str
            The address of the cluster to connect to, e.g 123.123.123:4411
        apikey: str
            The option API key to authenticate requests

        Returns
        -------
        Connection
            A Connection class object.
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

    """

    def __init__(self, endpoint):
        self.ep = endpoint


    def streams(self, *identifiers, versions=[]):
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
        if versions and len(versions) != len(identifiers):
            raise ValueError("Number of versions does not match identifiers")

        if not versions:
            versions = [0] * len(identifiers)

        streams = [self.stream_from_uuid(ident) for ident in identifiers]
        return StreamSet(streams)

    def stream_from_uuid(self, uu):
        # type: (UUID or str or bytes) -> Stream
        """
        Creates a stream handle to the BTrDB stream with the UUID `uu`. This
        method does not check whether a stream with the specified UUID exists.
        It is always good form to check whether the stream existed using
        `stream.exists()`.


        Parameters
        ----------
        uu: UUID
            The uuid of the requested stream.

        Returns
        -------
        Stream
            A Stream class object.

        """
        return Stream(self, to_uuid(uu))

    def create(self, uu, collection, tags=None, annotations=None):
        # type: (UUID, str, Dict[str, str], Dict[str, str]) -> Stream
        """
        Tells BTrDB to create a new stream with UUID `uu` in `collection` with specified `tags` and `annotations`.

        Parameters
        ----------
        uu: UUID
            The uuid of the requested stream.

        Returns
        -------
        Stream
            a Stream class object
        """

        if tags is None:
            tags = {}

        if annotations is None:
            annotations = {}

        self.ep.create(uu, collection, tags, annotations)
        return Stream(self, uu, True, collection, tags.copy(), annotations.copy(), 0)

    def info(self):
        # type: () -> (Mash)
        """
        Returns information about the connected BTrDB srerver.

        """
        return self.ep.info()

    def streams_in_collection(self, collection, isCollectionPrefix=True, tags=None, annotations=None):
        # type: (str, bool, Dict[str, str], Dict[str, str]) -> Stream

        """
        Search for streams matching given parameters

        This function allows for searching

        Parameters
        ----------
        collection: str
            The name of the collection to be found, case sensitive.
        isCollectionPrefix: bool
            Whether the collection is a prefix.
        tags: Dict[str, str]
            The tags to identify the stream.
        annotations: Dict[str, str]
            The annotations to identify the stream.

        Yields
        ------
        Stream Generator
            A stream generator that iterates over the search results.

        """

        if tags is None:
            tags = {}

        if annotations is None:
            annotations = {}

        streams = self.ep.lookupStreams(collection, isCollectionPrefix, tags, annotations)
        for desclist in streams:
            for desc in desclist:
                tagsanns = unpack_stream_descriptor(desc)
                yield Stream(self, uuid.UUID(bytes = desc.uuid), True, desc.collection, tagsanns[0], tagsanns[1], desc.propertyVersion)

    def collection_metadata(self, prefix):
        # type: (csv.writer, QueryType, int, int, int, int, bool, *Tuple[int, str, UUID]) -> Tuple[Dict[str, int], Dict[str, int]]
        """
        Gives statistics about metadata for collections that match a
        prefix.

        Parameters
        ----------
        prefix: str
            A prefix of the collection names to look at

        """
        ep = self.ep
        tags, annotations = ep.getMetadataUsage(prefix)
        pyTags = {tag.key: tag.count for tag in tags}
        pyAnn = {ann.key: ann.count for ann in annotations}
        return pyTags, pyAnn
