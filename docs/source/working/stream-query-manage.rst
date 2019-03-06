Querying and Managing Streams
================================

With BTrDB, you can easily create, delete, and query for streams using simple
method calls.  Simple examples are included below but please review the API docs
for further options.

Create a Stream
--------------------------
Creating a stream requires only a UUID, collection, and dictionary for the initial tags.

.. code-block:: python

    conn = btrdb.connect()

    stream = conn.create(
        uuid=uuid.uuid4(),
        collection="NORTHWEST/90001",
        tags={"name": "L1MAG", "unit": "volts"}
    )

Delete a Stream
--------------------------
Deleting a stream can be performed by calling the `obliterate` method on the
stream object.  If the stream could not be found than an error is raised.

.. code-block:: python

		conn = btrdb.connect()
		stream = conn.stream_from_uuid("66466a91-dcfe-42ea-9e88-87c51f847944")
		stream.obliterate()


Find by UUID
--------------------------
To retrieve your stream from the server at a later date, you can easily query
for it by using the UUID it was created with.  As a convenience, you can provide
either a UUID object or a string of the UUID value.  If a stream matching the
supplied UUID cannot be found then :code:`None` will be returned.

.. code-block:: python

    conn = btrdb.connect()
    stream = conn.stream_from_uuid("71466a91-dcfe-42ea-9e88-87c51f847942")


Query by collection
--------------------------
You can also search for multiple streams by collection using the server object's
:code:`streams_in_collection` method which will return a simple list of
:code:`Stream` instances.  Aside from the collection name, you can provide more
information such as tags and annotations.  Please see the API docs for more
detail.

.. code-block:: python

    conn = btrdb.connect()
    streams = conn.streams_in_collection("NORTHEAST/NH")
