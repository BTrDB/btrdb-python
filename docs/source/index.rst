Welcome to btrdb docs!
======================

.. image:: https://img.shields.io/travis/BTrDB/btrdb-python/master.svg
    :target: https://travis-ci.org/BTrDB/btrdb-python

.. image:: https://readthedocs.org/projects/btrdb/badge/?version=latest
    :target: https://btrdb.readthedocs.io/en/latest/

.. image:: https://img.shields.io/pypi/pyversions/btrdb.svg
    :target: https://pypi.org/project/btrdb/

.. image:: https://img.shields.io/badge/License-BSD%203--Clause-blue.svg
    :target: https://opensource.org/licenses/BSD-3-Clause

.. image:: https://img.shields.io/pypi/v/btrdb.svg
    :target: https://pypi.python.org/project/btrdb/

.. note::

  Starting with the 5.0 release, btrdb-python will be Python 3 only!  This decision
  was not made lightly but is necessary to keep compatibility with underlying
  packages.

  In addition, this software is only compatible with version 5.x of the BTrDB
  server.  To communicate with a 4.x server, please install an earlier version
  of this software.

Welcome to btrdb-python's documentation.  We provide Python access to the Berkeley
Tree Database (BTrBD) along with some select convenience methods.  If you are
familiar with other NoSQL libraries such as pymongo then you will likely feel
right at home using this library.

BTrDB is a very, very fast timeseries database.  Specifically, it is a time partitioned,
version annotated, clustered solution for high density univariate data.  It's also
incredibly easy to use.  Checkout out our :doc:`installing` page to get setup and
then visit :doc:`quick-start` for a brief tour.  Some sample code is below to whet
your appetite.

  .. code-block:: python

      import btrdb
      from btrdb.utils.timez import to_nanoseconds

      # establish connection to server
      conn = btrdb.connect("192.168.1.101:4410")

      # search for streams and view metadata
      streams = conn.streams_in_collection("USEAST_NOC1/90807")
      for stream in streams:
          print(stream.collection, stream.name, stream.tags())

      # retrieve a single stream
      stream = conn.stream_from_uuid("07d28a44-4991-492d-b9c5-2d8cec5aa6d4")

      # print one hour of time series data starting at 1/1/2018 12:30:00 UTC
      start = to_nanoseconds(datetime(2018,1,1,12,30))
      end = start + (60 * 60 * 1e9)
      for point, _ in stream.values(start, end):
          print(point.time, point.value)


User Guide
----------

The remaining documentation can be found below.  If there is anything you'd like
added or corrected, please feel free to submit a pull request or open an issue
in Github!

.. toctree::
   :maxdepth: 2

   quick-start
   installing
   concepts
   working
   explained


API Reference
-------------

.. toctree::
   :maxdepth: 2

   api/package
   api/conn
   api/streams
   api/points
   api/exceptions
   api/transformers
   api/utils-timez


Maintainers
-----------

* :doc:`maintainers/anaconda`





Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
