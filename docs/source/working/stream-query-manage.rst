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


Look up collections
--------------------------
You can look up collections found in the server by using the :code:`list_collections`
method, which returns a list of string collection names. Additionally,
you can use the :code:`starts_with` parameter to filter the results to include only collections
that begin with the provided prefix. Omitting the :code:`starts_with` parameter will return
all available collections from the server.

.. code-block:: python

    conn = btrdb.connect()
    collections = conn.list_collections(starts_with="NORTHWEST")


Finding by collection
--------------------------
You can also search for multiple streams by collection using the server object's
:code:`streams_in_collection` method which will return a simple list of
:code:`Stream` instances.  Aside from the collection name, you can provide more
information such as tags and annotations.  Please see the API docs for more
detail.

.. code-block:: python

    conn = btrdb.connect()
    streams = conn.streams_in_collection("NORTHEAST/NH")


Querying Metadata
-----------------
Finally, you can query for metadata using standard SQL although at the moment, only the
`streams` table is available.  SQL queries can be submitted using the `query`
method which accepts both a `stmt` and `params` argument.  The `stmt` should
contain the SQL you'd like executed with parameter placeholders such as `$1` or
`$2` as shown below.

.. code-block:: python

    conn = btrdb.connect()
    stmt = "select uuid from streams where name = $1 or name = $2"
    params = ["Boston_1", "Boston_2"]

    for row in conn.query(stmt, params):
      print(row)

The SQL query results are returned as a list of dictionaries where each key
matches a column from your SQL projection.  You can choose your columns from the
schema of the streams table as follows.


+------------------+------------------------+-----------+
|      Column      |          Type          | Nullable  |
+==================+========================+===========+
| uuid             | uuid                   | not null  |
+------------------+------------------------+-----------+
| collection       | character varying(256) | not null  |
+------------------+------------------------+-----------+
| name             | character varying(256) | not null  |
+------------------+------------------------+-----------+
| unit             | character varying(256) | not null  |
+------------------+------------------------+-----------+
| ingress          | character varying(256) | not null  |
+------------------+------------------------+-----------+
| property_version | bigint                 | not null  |
+------------------+------------------------+-----------+
| annotations      | hstore                 |           |
+------------------+------------------------+-----------+
