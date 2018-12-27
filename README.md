BTrDB Bindings for Python
=========================
These are BTrDB Bindings for Python meant to be used with Python's multithreading library. Documentation can be generated using pydoc.

Here is an example:
```python

>>> # This works for both Python 2 and Python 3
...
>>> import btrdb4
>>> import time
>>> import uuid
>>>
>>> # This is the UUID of the stream we are going to interact with
... uu = uuid.uuid4()
>>> uu
UUID('a5fba242-74c8-4e59-ad89-c1565ee3229c')
>>>
>>> # Connect to BTrDB and obtain a BTrDB handle
... conn = btrdb4.Connection("my.server:4410")
>>> # Connection with an API key. Note port 4411, the secure API
... conn = btrdb4.Connection("my.server:4411", apikey="255C59A06BB698681E3580D2")
>>> b = conn.newContext()
>>>
>>> # Connect using environment variables $BTRDB_ENDPOINTS and $BTRDB_API_KEY
>>> # Note that this returns a context directly rather than a connection.
>>> b = btrdb4.connect()
>>>
>>> # Obtain a stream handle
... s = b.streamFromUUID(uu)
>>> s.exists()
False
>>>
>>> # Create the stream
... s = b.create(uu, "a/b/c", {"created_by": b"me", "time_created": bytes(str(time.time()).encode("utf-8"))})
>>> s.exists()
True
>>>
>>> # Insert some data
... version = s.insert(((1, 10), (3, 14), (5, 19), (9, 13)))
>>> version
0
>>>
>>> # Query some data
... for rawpoint, version in s.rawValues(0, 7):
...     print("{0}: {1}".format(rawpoint, version))
...
>>>
```
