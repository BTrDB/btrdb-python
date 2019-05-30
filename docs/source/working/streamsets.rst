Working with StreamSets
========================

Often you will want to query and work with multiple streams instead of just an
individual stream - StreamSets allow you to do this effectively. It is a light
wrapper around a list of Stream objects with convenience methods provided to
help you work with multiple streams of data.


Creating a StreamSet
---------------------

Creating a :code:`StreamSet` is relatively simple assuming you have a :code:`UUID` for each
stream that should be a member.  In the future, other options may exist such as
providing collection or tag matching parameters.

.. code-block:: python

    UUIDs = [
        uuid.UUID('0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a'),
        uuid.UUID('17dbe387-89ea-42b6-864b-f505cdb483f5'),
        uuid.UUID('71466a91-dcfe-42ea-9e88-87c51f847942'),
        uuid.UUID('570aa71d-fb4f-456f-8533-2b11a28fa1f5')
    ]

    streams = conn.streams(*UUIDs)

If you've already obtained a list of :code:`Stream` objects, you may create
a StreamSet directly by providing a list of streams for initialization.

.. code-block:: python

    streams = StreamSet([stream1, stream2, stream3])


Filtering
----------
To apply query parameters to your request, you should use the :code:`filter`
method to supply a :code:`start` or :code:`end` argument.

Keep in mind that :code:`filter` will return a new object so you can keep
multiple filtered StreamSets in memory while you explore your data.  The
:code:`filter` method may be called multiple times but only the final values
will be used when it is time to fulfill the request by the server.

.. code-block:: python

    from btrdb.utils.timez import currently_as_ns, to_nanoseconds

    streams = conn.streams(*UUIDs)

    start = to_nanoseconds(datetime(2016, 1, 1, 0, 0, 0))
    end = to_nanoseconds(datetime(2016, 1, 3, 12, 0, 0))

    # replace instance with a filtered version from 1/1/2016 00:00:00 to
    # 1/3/2016 12:00:00
    streams = streams.filter(start=start, end=end)

    # create a new instance with epoch as start and the current time as
    # the end parameters
    alt = streams.filter(start=0, end=currently_as_ns())

Aside from filtering results at query execution, you may also filter the streams
that should be included in the new object.  For instance, you may wish to create
a new StreamSet containing only voltage streams or only from a specific
collection.

To filter the available streams, you may provide a :code:`collection`,
:code:`name`, or :code:`unit` argument.  If you provide a string, then a
case-insensitive exact match will be used to select the desired streams.  You
may instead provide a compiled regex expression which will be used with
re.search to choose the streams to include.

.. code-block:: python

    # select only voltage streams
    voltage_streams = streams.filter(unit="volts")

    # select only voltage or amperage streams using regex pattern
    other_streams = streams.filter(unit=re.compile("volts|amps"))


Retrieving Data
----------------

There are two options available when you are ready to process the data from the
server.  Both options are fully materialized but are organized in different ways
according to what is more convenient for you.

StreamSet.values()
^^^^^^^^^^^^^^^^^^
Calling the :code:`values` method will materialize the streams using the
filtering parameters you specified.  The data will be returned to you as a list
of lists.  Each member list contains tuples of :code:`RawPoint`, :code:`int` for
the data and stream version.

This method aligns data by stream so you can easily deal with all of the data
on a stream by stream basis.  The following example shows a toy dataset which
consists of 4 streams.

.. code-block:: python

    streams.values()
    >>[[RawPoint(1500000000100000000, 2.0),
    >>  RawPoint(1500000000300000000, 4.0),
    >>  RawPoint(1500000000500000000, 6.0),
    >>  RawPoint(1500000000700000000, 8.0),
    >>  RawPoint(1500000000900000000, 10.0)],
    >> [RawPoint(1500000000000000000, 1.0),
    >>  RawPoint(1500000000200000000, 3.0),
    >>  RawPoint(1500000000400000000, 5.0),
    >>  RawPoint(1500000000600000000, 7.0),
    >>  RawPoint(1500000000800000000, 9.0)],
    >> [RawPoint(1500000000000000000, 1.0),
    >>  RawPoint(1500000000100000000, 2.0),
    >>  RawPoint(1500000000300000000, 4.0),
    >>  RawPoint(1500000000400000000, 5.0),
    >>  RawPoint(1500000000600000000, 7.0),
    >>  RawPoint(1500000000700000000, 8.0),
    >>  RawPoint(1500000000800000000, 9.0),
    >>  RawPoint(1500000000900000000, 10.0)],
    >> [RawPoint(1500000000000000000, 1.0),
    >>  RawPoint(1500000000100000000, 2.0),
    >>  RawPoint(1500000000200000000, 3.0),
    >>  RawPoint(1500000000300000000, 4.0),
    >>  RawPoint(1500000000400000000, 5.0),
    >>  RawPoint(1500000000500000000, 6.0),
    >>  RawPoint(1500000000600000000, 7.0),
    >>  RawPoint(1500000000700000000, 8.0),
    >>  RawPoint(1500000000800000000, 9.0),
    >>  RawPoint(1500000000900000000, 10.0)]]


StreamSet.rows()
^^^^^^^^^^^^^^^^^^
By contrast, the :code:`rows` method aligns data by time rather than by stream.
Each row of data contains points for a specific time with the
:code:`None` value used if a given stream does not contain a value at that time.

Stream data is ordered according to the order of the initial UUIDs that were
used when creating the StreamSet.

.. code-block:: python

    for row in streams.rows():
        print(row)
    >> (None, RawPoint(1500000000000000000, 1.0), RawPoint(1500000000000000000, 1.0), RawPoint(1500000000000000000, 1.0))
    >> (RawPoint(1500000000100000000, 2.0), None, RawPoint(1500000000100000000, 2.0), RawPoint(1500000000100000000, 2.0))
    >> (None, RawPoint(1500000000200000000, 3.0), None, RawPoint(1500000000200000000, 3.0))
    >> (RawPoint(1500000000300000000, 4.0), None, RawPoint(1500000000300000000, 4.0), RawPoint(1500000000300000000, 4.0))
    >> (None, RawPoint(1500000000400000000, 5.0), RawPoint(1500000000400000000, 5.0), RawPoint(1500000000400000000, 5.0))
    >> (RawPoint(1500000000500000000, 6.0), None, None, RawPoint(1500000000500000000, 6.0))
    >> (None, RawPoint(1500000000600000000, 7.0), RawPoint(1500000000600000000, 7.0), RawPoint(1500000000600000000, 7.0))
    >> (RawPoint(1500000000700000000, 8.0), None, RawPoint(1500000000700000000, 8.0), RawPoint(1500000000700000000, 8.0))
    >> (None, RawPoint(1500000000800000000, 9.0), RawPoint(1500000000800000000, 9.0), RawPoint(1500000000800000000, 9.0))
    >> (RawPoint(1500000000900000000, 10.0), None, RawPoint(1500000000900000000, 10.0), RawPoint(1500000000900000000, 10.0))


Transforming to Other Formats
-----------------------------
A number of transformation features have been added so that you can work in the
tools and APIs you are most comfortable and productive with.  At the moment, we
support the `numpy` and `pandas` libraries if you have them installed and
available to be imported.

Keep in mind that calling these methods will materialize the requested data in
memory.  A few examples follow but please visit the API documentation to see the
full list of transformation methods available.

.. code-block:: python

    # materialize data as tuple of numpy arrays
    conn.streams(*UUIDs).filter(start, end).to_array()
    >> (array([RawPoint(1500000000100000000, 2.0),
    >>        RawPoint(1500000000300000000, 4.0),
    >>        RawPoint(1500000000500000000, 6.0),
    >>        RawPoint(1500000000700000000, 8.0),
    >>        RawPoint(1500000000900000000, 10.0)], dtype=object),
    >>  array([RawPoint(1500000000000000000, 1.0),
    >>        RawPoint(1500000000200000000, 3.0),
    >>        RawPoint(1500000000400000000, 5.0),
    >>        RawPoint(1500000000600000000, 7.0),
    >>        RawPoint(1500000000800000000, 9.0)], dtype=object),
    >> ...

    # materialize data as list of pandas Series
    conn.streams(*UUIDs).filter(start, end).to_series()
    >> [1500000000100000000     2.0
    >> 1500000000300000000     4.0
    >> 1500000000500000000     6.0
    >> 1500000000700000000     8.0
    >> 1500000000900000000    10.0
    >> dtype: float64,
    >>  1500000000000000000    1.0
    >> 1500000000200000000    3.0
    >> 1500000000400000000    5.0
    >> 1500000000600000000    7.0
    >> 1500000000800000000    9.0
    >> dtype: float64,
    >> ...

    # materialize data as pandas DataFrame
    conn.streams(*UUIDs).filter(start, end).to_dataframe()
    >>                   time         sensors/stream0         sensors/stream1
    >> 0  1500000000000000000                     NaN                     1.0
    >> 1  1500000000100000000                     2.0                     NaN
    >> 2  1500000000200000000                     NaN                     3.0
    >> 3  1500000000300000000                     4.0                     NaN
    >> 4  1500000000400000000                     NaN                     5.0
    >> 5  1500000000500000000                     6.0                     NaN
    >> 6  1500000000600000000                     NaN                     7.0
    >> 7  1500000000700000000                     8.0                     NaN
    >> 8  1500000000800000000                     NaN                     9.0
    >> 9  1500000000900000000                    10.0                     NaN


Serializing Data
----------------
If you would like to save your data to disk for later use or to import into
another program, we have several options available with more planned in the
future.

Most serialization methods will save to disk however there is also a
:code:`to_table` method which produces a tabular view of your data as a string for
display or printing.  Some examples are shown below.

.. code-block:: python

    # export data and save as CSV
    streams.to_csv("export.csv")

    # convert table of data as a string
    print(streams.to_table())
    >>                time    sensors/stream0    sensors/stream1
    >> -------------------  -----------------  -----------------
    >> 1500000000000000000                                     1
    >> 1500000000100000000                  2
    >> 1500000000200000000                                     3
    >> 1500000000300000000                  4
    >> 1500000000400000000                                     5
    >> 1500000000500000000                  6
    >> 1500000000600000000                                     7
    >> 1500000000700000000                  8
    >> 1500000000800000000                                     9
    >> 1500000000900000000                 10
