Managing Stream Metadata
========================
BTrDB has multiple options for storing stream metadata including
collection, tags, annotations, and others.  Most metadata is returned as a
:code:`string`, or specialized object such as the UUID.  Tags and annotations
are returned as :code:`dict` objects.

There is also the concept of the "property version" which is a version counter
that applies only to the metadata and is separate from the version incremented
with changes to the data.  See the API docs for :code:`Stream.annotations` or
:code:`Stream.update` for more information.

Viewing Metadata
----------------------------
Viewing the metadata for a Stream is as simple as calling the appropriate
property or method.  In cases where the data is not expected to change
quickly, a Stream instance will provide you with cached values unless you force
it to refresh with the server.

UUID
^^^^
The :code:`uuid` property of a :code:`Stream` is read-only and will return an
instance of class :code:`UUID`.

.. code-block:: python

    stream.uuid
    >> UUID('07d28a44-4991-492d-b9c5-2d8cec5aa6d4')

Tags
^^^^
Tags are special key/value metadata that is most often used by the database for
internal purposes.  As an example, the name of a :code:`Stream` is actually
stored in the tags.  While you can update tags, it is not recommended that you
add new tags or delete existing tags. Tag values have a 255 character limit.

.. code-block:: python

    stream.tags(refresh=True)
    >> {'name': 'L1MAG', 'unit': 'volts', 'ingress': ''}


Annotations
^^^^^^^^^^^
Similar to tags, annotations are key/value pairs that are available for your use
to store extra information about the :code:`Stream`.

Because annotations may change more often than tags, a metadata version number
is also returned when asking for annotations.  This version number is incremented
whenever metadata (tags, annotations, collection, etc.) are updated but not when
making changes to the underlying time series data.

By default the method will attempt to provide a cached copy of the annotations
however you can request the latest version from the server using the `refresh`
argument. As with tags, annotations values also have a 255 character limit.

.. code-block:: python

    stream.annotations(refresh=True)
    >> ({'owner': 'Salvatore McFesterson', 'state': 'NH'}, 44)



Name and Collection
^^^^^^^^^^^^^^^^^^^
The :code:`name` and :code:`collection` properties of a Stream are read-only and
will return instances of :code:`str`.  Note that the :code:`name` property is
just a convenience as this value can also be found within the tags.

.. code-block:: python

    stream.collection
    >> 'NORTHEAST/VERMONT/Burlington'

    stream.name
    >> 'L1MAG'



Updating Metadata
----------------------------
An :code:`update` method is available if you would like to make changes to
the tags, annotations, or collection.  By default, all updates are implemented
as an UPSERT operation and a single change could result in multiple increments
to the property version (the version of the metadata).

Upon calling this method, the library will first verify that the local property version of your
stream object matches the version found on the server.  If the two versions
do not match then you will not be allowed to perform an update as this implies
that the data has already been changed by another user or process.

.. code-block:: python

    collection = 'NORTHEAST/VERMONT'
    annotations = {
        'owner': 'Salvatore McFesterson',
        'state': 'VT',
        'created': '2018-01-01 12:42:03 -0500'
    }
    property_version = stream.update(
        collection=collection,
        annotations=annotations
    )

If you would like to remove any keys from your annotations you must use the `replace=True` keyword argument.  This will ensure that the annotations dictionary you provide completely replaces the existing values rather than perform an UPSERT operation.  The example below shows how you could remove an existing key from the annotations dictionary.

.. code-block:: python

    annotations, _ = stream.anotations()
    del annotations["key_to_delete"]
    stream.update(annotations=annotations, replace=True)
