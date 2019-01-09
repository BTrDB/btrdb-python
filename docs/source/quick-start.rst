========================
Quick Start
========================

Connecting to a server
----------------------

Connecting to a server is easy with the supplied :code:`connect` function from the btrdb package.

.. code-block:: python

    import btrdb
    conn = btrdb.connect("192.168.1.101:4410")


Retrieving Streams
----------------------

In order to interact with data, you'll need to obtain or create a :code:`Stream` object.  A
number of options are available to get existing streams...

Find streams by collection
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    streams = conn.streams_in_collection("USEAST_NOC1/90807")
    for stream in streams:
        print(stream.uuid())

Find stream by UUID
~~~~~~~~~~~~~~~~~~~~~~~~~~

BTrDB has a unique UUID for each stream which you can use for direct access.  For
convenience, this method accepts instances of either :code:`str` or :code:`UUID`.

.. code-block:: python

    stream = conn.stream_from_uuid("07d28a44-4991-492d-b9c5-2d8cec5aa6d4")



Viewing Data
----------------------

To view data within a stream, you'll need to specify a time range to query for as
well as a version number.  Remember that BTrDB stores data to the nanosecond
and so Unix timestamps will need to be converted if needed.

.. code-block:: python

    start = datetime(2018,1,1,12,30, tzinfo=timezone.utc)
    start = start.timestamp() * 1e9
    end = start + (3600 * 1e9)

    for point, _ in stream.rawValues(start, end):
      print(point.time, point.value)
