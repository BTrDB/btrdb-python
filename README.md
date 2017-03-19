BTrDB Bindings for Python
=========================
These are BTrDB Bindings for Python are meant to be used with Python's multithreading library. Documentation can be generated using pydoc.

Here is an example:
```
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
... conn = btrdb4.Connection("compound-0.cs.berkeley.edu:4410")
>>> b = conn.newContext()
>>>
>>> # Obtain a stream handle
... s = b.streamFromUUID(uu)
>>> s.exists()
False
>>>
>>> # Create the stream
... s = b.create(uu, "a/b/c", {"created_by": b"me", "time_created": bytes(str(time.time()), "ascii")})
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
