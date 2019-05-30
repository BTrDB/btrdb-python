Viewing Stream Data
=====================

At a high level, there are two options available when you are ready to retrieve
the time series data in a stream.  You may view the values directly by timestamp
or you can view a window of data at a resolution of your choice.  When viewing
by window, there are further options available with different arguments and
related performance benefits.

View Individual Data Points
----------------------------
To view the values directly, call the :code:`values` method which will
fully materialize the stream values at the stream version you specify (use the
default value of zero as the latest version).  A :code:`start` and :code:`end`
argument is required when making this request.

Calling :code:`values` will return a series of :code:`tuple`, with each item containing a
:code:`RawPoint`, and version of the stream (:code:`int`).  As described in the
API reference, a :code:`RawPoint` has both a :code:`time` and :code:`value`
property.

.. code-block:: python

    start = 1500000000000000000
    end = 1547241923338098176

    for point, _ in stream.values(start=start, end=end, version=133):
        print(point)
    >> RawPoint(1500000000000000000, 2.35)
    >> RawPoint(1500000000100000000, 2.41)
    >> RawPoint(1500000000200000000, 2.8)
    >> RawPoint(1500000000300000000, 3.66)
    ...


View Windows of Data
--------------------
If you don't need to view every single point of data, then it is faster to view
higher order representations of the data.  BTrDB stores data in
a tree structure such that the leaves of the tree contain actual values and higher
nodes store statistical data (min, max, mean, etc.) summaries.  In this schema
viewing summaries of data involves reading from higher levels of the tree and
therefore less nodes need to be read from disk.

This use case of wanting a high level summary of data is quite common.  For
example, when rendering the plot of a time series it will often be useful to
present a view at the resolution of one hour, one day, or perhaps one year.  With
samples that occur at greater than 1Hz this requires you to summarize the values
and plot the average (or min, max, etc.) values rather than each individual value.

Because BTrDB is usually providing summaries of data when windowing, it returns
instances of :code:`StatPoint` rather than :code:`RawPoint`.  A :code:`StatPoint`
contains statistical information about a range of time and specifically provides
properties for :code:`min`, :code:`mean`, :code:`max`, :code:`count`,
:code:`stddev`, and the start :code:`time` for which the statistical summaries
cover.

aligned_windows
^^^^^^^^^^^^^^^^
For statistical aggregates of your data, the :code:`aligned_windows` method is
the fastest way to query your data. Each point returned is a statistical
aggregate of all the raw data within a window of width 2^pointwidth
nanoseconds.

Note that :code:`start` is inclusive, but :code:`end` is exclusive. That is, results
will be returned for all windows that start in the interval [start, end).
If end < start+2^pointwidth you will not get any results. If start and
end are not powers of two, the bottom pointwidth bits will be cleared.
Each window will contain statistical summaries of the window. Statistical points
with count == 0 will be omitted.

.. code-block:: python

    start = 1500000000000000000
    end = 1500000001000000000

    # view underlying data for comparison
    for point, _ in stream.values(start=start, end=end):
        print(point)
    >> RawPoint(1500000000000000000, 1.0)
    >> RawPoint(1500000000100000000, 2.0)
    >> RawPoint(1500000000200000000, 3.0)
    >> RawPoint(1500000000300000000, 4.0)
    >> RawPoint(1500000000400000000, 5.0)
    >> RawPoint(1500000000500000000, 6.0)
    >> RawPoint(1500000000600000000, 7.0)
    >> RawPoint(1500000000700000000, 8.0)
    >> RawPoint(1500000000800000000, 9.0)
    >> RawPoint(1500000000900000000, 10.0)

    # aggregate over 2^28 nanoseconds (268,435,456)
    pointwidth = 28

    # view data aggregates
    for point, _ in stream.aligned_windows(start=start, end=end,
                                           pointwidth=pointwidth):
        print(point)
    >> StatPoint(1499999999814008832, 1.0, 1.0, 1.0, 1, 0.0)
    >> StatPoint(1500000000082444288, 2.0, 3.0, 4.0, 3, 0.816496580927726)
    >> StatPoint(1500000000350879744, 5.0, 6.0, 7.0, 3, 0.816496580927726)
    >> StatPoint(1500000000619315200, 8.0, 8.5, 9.0, 2, 0.5)


windows
^^^^^^^^
The :code:`windows` method of a Stream allows you to request windows of data
while specifying the precision of the data you require.  Each window will cover
:code:`width` nanoseconds in length.  Precision of the result is determined by
the :code:`depth` parameter such that each window will be accurate to
2^depth nanoseconds.

Using a larger depth value will result in faster query execution from the
database.  For instance, if you are viewing a 24 hours of data you may only require
a precision of +/- 1 second and so a depth of 30 may be appropriate.  A chart
of sample depths are provided below.

+-------+-------------+---------------------------+-----------------+
| Depth | Calculation | Precision in Nanoseconds  | Time            |
+=======+=============+===========================+=================+
| 0     | 2^0         | 1                         | 1 nanosecond    |
+-------+-------------+---------------------------+-----------------+
| 10    | 2^10        | 1024                      | ~1 microsecond  |
+-------+-------------+---------------------------+-----------------+
| 20    | 2^20        | 1048576                   | ~1 millesecond  |
+-------+-------------+---------------------------+-----------------+
| 30    | 2^30        | 1073741824                | ~1 second       |
+-------+-------------+---------------------------+-----------------+

As usual when querying data from BTrDB, the :code:`start` time is inclusive
while the :code:`end` time is exclusive.  Note that if your last window spans
across the end time then it will not be included in the results.

.. code-block:: python

    start = 1500000000000000000
    end = 1500000001000000000

    # view underlying data for comparison
    for point, _ in stream.values(start=start, end=end):
        print(point)
    >> RawPoint(1500000000000000000, 1.0)
    >> RawPoint(1500000000100000000, 2.0)
    >> RawPoint(1500000000200000000, 3.0)
    >> RawPoint(1500000000300000000, 4.0)
    >> RawPoint(1500000000400000000, 5.0)
    >> RawPoint(1500000000500000000, 6.0)
    >> RawPoint(1500000000600000000, 7.0)
    >> RawPoint(1500000000700000000, 8.0)
    >> RawPoint(1500000000800000000, 9.0)
    >> RawPoint(1500000000900000000, 10.0)

    # each window spans 300 milleseconds
    width = 300000000

    # request a precision of roughly 1 millesecond
    depth = 20

    # view windowed data
    for point, _ in stream.windows(start=start, end=end,
                                   width=width, depth=depth):
    >> StatPoint(1500000000000000000, 1.0, 2.0, 3.0, 3, 0.816496580927726)
    >> StatPoint(1500000000300000000, 4.0, 5.0, 6.0, 3, 0.816496580927726)
    >> StatPoint(1500000000600000000, 7.0, 8.0, 9.0, 3, 0.816496580927726)
