BTrDB Bindings for Python
=========================
These are BTrDB Bindings for Python are meant to be used with Python's multithreading library. Documentation can be generated using pydoc.

Here is an example:
```
>>> import uuid
>>> import btrdb4
>>>
>>> # This is the UUID of the stream we are going to interact with
... u = uuid.UUID('6390e9df-dfcb-4084-8080-8c719ce751ed')
>>>
>>> # Set up the BTrDB Connection and Endpoint Handle
... connection = btrdb4.BTrDBConnection('compound-0.cs.berkeley.edu:4410')
>>> endpoint = connection.newContext()
>>>
>>> # Create the stream
>>> import time
>>> err = endpoint.create(u, "a/b/c", {"created_by": "me"}, {"time": str(time.time())})
>>> print err
<success>
>>>
>>> # Insert some data
... # We have to use the "sync" flag because we want the insert to be committed immediately, so that we can query it right away
... version, err = endpoint.insert(u, ((1, 10), (3, 14), (5, 19), (9, 13)), sync = True)
>>> version
0L
>>> print err
<success>
>>> # Query some data
... generator = context.rawValues(u, 0, 7)
>>> for batch in generator:
...     result, version, err = batch
...     print result
...     print version
...     print err
...
[RawPoint(1L, 10.0), RawPoint(3L, 14.0), RawPoint(5L, 19.0)]
11
<success>
>>>
```
