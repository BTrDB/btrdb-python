Managing Stream Data
====================
BTrDB allows you to insert data and delete data using Stream objects.

Inserting Data
---------------
You can insert data into a Stream at any time - even for times that already exist!
As we will later see, querying data will return :code:`RawPoint` and
:code:`StatPoint` objects however inserting data requires only a time
:code:`int` and value :code:`float` within in a :code:`tuple` object
(:code:`tuple(int, float)`).

After inserting your data, the server will return a new version number for your
stream.

.. code-block:: python

    payload = [
        (1500000000000000000, 1.0), (1500000000000100000, 2.1),
        (1500000000000200000, 3.3), (1500000000000300000, 5.1),
        (1500000000000400000, 5.7), (1500000000000500000, 6.1),
    ]
    version = stream.insert(payload)




Deleting Data
---------------

To delete data from a stream you must provide a range (start/end) of time to the
:code:`delete` method.

Because you are modifying data, the version number is incremented and will be
returned from the server at the end of your call.  Keep in mind that data is
never truly gone as you can query for the deleted data using an older
version of the Stream.


.. code-block:: python

    version = stream.delete(start=1500000000000000000, end=1520000000000000000)
