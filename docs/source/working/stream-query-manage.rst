Querying and Managing Streams
================================


.. note::

	The current release for this library does not allow you to delete a stream. This is supported on other language bindings and will be included in a future update.


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

Find by UUID
--------------------------
To retrieve your stream from the server at a later date, you can easily query
for it by using the UUID it was created with.  As a convenience, you can provide
either a UUID object or a string of the UUID value.

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
