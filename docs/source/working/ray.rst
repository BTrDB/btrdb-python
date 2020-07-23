Working with Ray
================================

To use BTrDB connection, stream and streamsets objects in the parallelization library ray,
a special serializer is required. BTrDB provides  a utility function that register the serializer with ray.
An example is shown below.

Setting up the ray serializer
-----------------------------
.. code-block:: python

    import btrdb
    import ray
    from btrdb.utils.ray import register_serializer

    uuids = ["b19592fc-fb71-4f61-9d49-8646d4b1c2a1",
             "07b2cff3-e957-4fa9-b1b3-e14d5afb1e63"]
    ray.init()

    conn_params = {"profile": "profile_name"}
    
    # register serializer with the connection parameters
    register_serializer(**conn_params)

    conn = btrdb.connect(**conn_params)

    # BTrDB connection object can be passed as an argument
    # to a ray remote function
    @ray.remote
    def test_btrdb(conn):
        print(conn.info())

    # Stream object can be passed as an argument
    # to a ray remote function
    @ray.remote
    def test_stream(stream):
        print(stream.earliest())

    # StreamSet object can be passed as an argument
    # to a ray remote function
    @ray.remote
    def test_streamset(streamset):
        print(streamset.earliest())
        print(streamset)


    ids = [test_btrdb.remote(conn),
        test_stream.remote(conn.stream_from_uuid(uuids[0])),
        test_streamset.remote(conn.streams(*uuids))]

    ray.get(ids)
    # output of test_btrdb
    >>(pid=28479) {'majorVersion': 5, 'build': '5.10.5', 'proxy': {'proxyEndpoints': []}}
    # output of test_stream
    >>(pid=28482) (RawPoint(1533210100000000000, 0.0), 0)
    # output of test_streamset
    >>(pid=28481) (RawPoint(1533210100000000000, 0.0), RawPoint(1533210100000000000, 0.0))
    >>(pid=28481) StreamSet with 2 streams