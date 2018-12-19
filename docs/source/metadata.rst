==================
Metadata
==================

BTrDB supports multiple options for stream metadata including tags and annotations.


Tags
==================

Tags are key/value pairs of string which can be added onto Stream objects and can also
be used when querying for specific streams.

Adding Tags
----------------

You can easily add tags to a Stream at creation and is actually necessary in order
to name a stream.

.. code-block:: python

    import btrdb
    conn = btrdb.connect("192.168.1.101:4410")

    tags = {
        "name": "L1_MAG",
        "units": "volts",
        "poc": "salvatore.mcfesterson@example.com",
    }
    conn.create(self, uuid.UUID(), "USEAST", tags=tags)

Viewing Tags
----------------



Querying by Tags
----------------




Annotations
==================
