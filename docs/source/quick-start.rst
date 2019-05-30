========================
Quick Start
========================

Connecting to a server
----------------------

Connecting to a server is easy with the supplied :code:`connect` function from the btrdb package.

.. code-block:: python

    import btrdb

    # connect without credentials
    conn = btrdb.connect("192.168.1.101:4410")

    # connect using TLS
    conn = btrdb.connect("192.168.1.101:4411")

    # connect with API key
    conn = btrdb.connect("192.168.1.101:4411", apikey="123456789123456789")


Retrieving a Stream
----------------------

In order to interact with data, you'll need to obtain or create a :code:`Stream` object.  A
number of options are available to get existing streams.

Find streams by collection
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Multiple streams are often organized under a single collection which is similar
to the concept of a directory path.  To search for all streams under a given
collection you can use the :code:`streams_in_collection` method.

.. code-block:: python

    streams = conn.streams_in_collection("USEAST_NOC1/90807")
    for stream in streams:
        print(stream.uuid, stream.name)

Find stream by UUID
^^^^^^^^^^^^^^^^^^^^^
A method has also been provided if you already know the UUID of a single stream you
would like to retrieve. For convenience, this method accepts instances of either
:code:`str` or :code:`UUID`.

.. code-block:: python

    stream = conn.stream_from_uuid("07d28a44-4991-492d-b9c5-2d8cec5aa6d4")



Viewing a Stream's Data
------------------------

To view data within a stream, you'll need to specify a time range to query for as
well as a version number (defaults to latest version).  Remember that BTrDB
stores data to the nanosecond and so Unix timestamps will need to be converted
if needed.

.. code-block:: python

    start = datetime(2018,1,1,12,30, tzinfo=timezone.utc)
    start = start.timestamp() * 1e9
    end = start + (3600 * 1e9)

    for point, _ in stream.values(start, end):
      print(point.time, point.value)

Some convenience functions are available to make it easier to deal with
converting to nanoseconds.

.. code-block:: python

    from btrdb.utils.timez import to_nanoseconds, currently_as_ns

    start = to_nanoseconds(datetime(2018,1,1, tzinfo=timezone.utc))
    end = currently_as_ns()

    for point, _ in stream.values(start, end):
      print(point.time, point.value)

You can also view windows of data at arbitrary levels of detail.  One such
windowing feature is shown below.

.. code-block:: python

    # query for windows of data 10,000 nanoseconds wide using a depth of zero
    # which is accurate to the nanosecond.
    params = {
        "start": 1500000000000000000,
        "end": 1500000000010000000,
        "width": 2000000,
        "depth": 0,
    }
    for window in stream.windows(**params):
        for point, version in window:
            print(point, version)

Using StreamSets
--------------------
A :code:`StreamSet` is a wrapper around a list of :code:`Stream` objects with a
number of convenience methods available.  Future updates will allow you to
query for streams using a SQL-like syntax but for now you will need to provide
a list of UUIDs.

The StreamSet allows you to interact with a group of streams rather than at the
level of the individual :code:`Stream` object.  Aside from being useful to see
concurrent data across streams, you can also easily transform the data to other
data structures or even serialize the data to disk in one operation.

Some quick examples are shown below but please review the API docs for the full
list of features.

.. code-block:: python

    streams = db.streams(*uuid_list)

    # serialize data to disk as CSV
    streams.filter(start=1500000000000000000).to_csv("data.csv")

    # convert data to a pandas DataFrame
    streams.filter(start=1500000000000000000).to_dataframe()
    >>                    time  NW/stream0  NW/stream1
        0  1500000000000000000         NaN         1.0
        1  1500000000100000000         2.0         NaN
        2  1500000000200000000         NaN         3.0
        3  1500000000300000000         4.0         NaN
        4  1500000000400000000         NaN         5.0
        5  1500000000500000000         6.0         NaN
        6  1500000000600000000         NaN         7.0
        7  1500000000700000000         8.0         NaN
        8  1500000000800000000         NaN         9.0
        9  1500000000900000000        10.0         NaN

    # materialize the streams' data
    streams.filter(start=1500000000000000000).values()
    >> [[RawPoint(1500000000100000000, 2.0),
        RawPoint(1500000000300000000, 4.0),
        RawPoint(1500000000500000000, 6.0),
        RawPoint(1500000000700000000, 8.0),
        RawPoint(1500000000900000000, 10.0)],
       [RawPoint(1500000000000000000, 1.0),
        RawPoint(1500000000200000000, 3.0),
        RawPoint(1500000000400000000, 5.0),
        ...
