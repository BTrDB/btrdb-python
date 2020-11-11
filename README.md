# BTrDB Bindings for Python

These are BTrDB Bindings for Python allowing you painless and productive access to the Berkeley Tree Database (BTrDB).  BTrDB is a time series database focusing on blazing speed with respect to univariate time series data at the nanosecond scale.


## Sample Code

Our goal is to make BTrDB as easy to use as possible, focusing on integration with other tools and the productivity of our users.  In keeping with this we continue to add new features such as easy transformation to numpy arrays, pandas Series, etc.  See the sample code below and then checkout our [documentation](https://btrdb.readthedocs.io/en/latest/) for more in depth instructions.


    import btrdb

    # connect to database
    conn = btrdb.connect("192.168.1.101:4410")

    # view time series streams found at provided collection path
    streams = conn.streams_in_collection("USEAST_NOC1/90807")
    for stream in streams:
        print(stream.name)

    # retrieve a given stream by UUID and print out data points.  In value
    # queries you will receive RawPoint instances which contain a time and value
    # attribute
    stream = conn.stream_from_uuid("71466a91-dcfe-42ea-9e88-87c51f847942")
    for point, _ in stream.values(start, end):
        print(point)
    >> RawPoint(1500000000000000000, 1.0)
    >> RawPoint(1500000000100000000, 2.0)
    >> RawPoint(1500000000200000000, 3.0)
    ...

    # view windowed data.  Each StatPoint contains the time of the window and
    # common statistical data such as min, mean, max, count, and standard
    # deviation of values the window covers.  See docs for more details.
    width = 300000000
    depth = 20
    for point, _ in stream.windows(start=start, end=end,
                                   width=width, depth=depth):
    >> StatPoint(1500000000000000000, 1.0, 2.0, 3.0, 3, 0.816496580927726)
    >> StatPoint(1500000000300000000, 4.0, 5.0, 6.0, 3, 0.816496580927726)
    >> StatPoint(1500000000600000000, 7.0, 8.0, 9.0, 3, 0.816496580927726)



You can also easily work with a group of streams for when you need to evaluate data across multiple time series or serialize to disk.

    from btrdb.utils.timez import to_nanoseconds

    start = to_nanoseconds(datetime(2018,1,1,9,0,0))
    streams = db.streams(*uuid_list)

    # convert stream data to numpy arrays
    data = streams.filter(start=start).to_array()

    # serialize stream data to disk as CSV
    streams.filter(start=start).to_csv("data.csv")

    # convert stream data to a pandas DataFrame
    streams.filter(start=start).to_dataframe()
    >>                    time  NOC_1/stream0  NOC_1/stream1
        0  1500000000000000000            NaN            1.0
        1  1500000000100000000            2.0            NaN
        2  1500000000200000000            NaN            3.0
        3  1500000000300000000            4.0            NaN
        4  1500000000400000000            NaN            5.0
        5  1500000000500000000            6.0            NaN
        6  1500000000600000000            NaN            7.0
        7  1500000000700000000            8.0            NaN
        8  1500000000800000000            NaN            9.0
        9  1500000000900000000           10.0            NaN


## Installation

See our documentation on [installing](https://btrdb.readthedocs.io/en/latest/installing.html) the bindings for more detailed instructions.  However, to quickly get started using the latest available versions you can use `pip` to install from pypi with `conda` support coming in the near future.

    $ pip install btrdb


## Tests

This project includes a suite of automated tests based upon [pytest](https://docs.pytest.org/en/latest/).  For your convenience, a `Makefile` has been provided with a target for evaluating the test suite.  Use the following command to run the tests.

    $ make test

Aside from basic unit tests, the test suite is configured to use [pyflakes](https://github.com/PyCQA/pyflakes) for linting and style checking as well as [coverage](https://coverage.readthedocs.io) for measuring test coverage.

Note that the test suite has additional dependencies that must be installed for them to successfully run: `pip install -r tests/requirements.txt`.

## Releases

This codebase uses github actions to control the release process.  To create a new release of the software, run `release.sh` with arguments for the new version as shown below.  Make sure you are in the master branch when running this script.

```
./release.sh 5 11 4
```

This will tag and push the current commit and github actions will run the test suite, build the package, and push it to pypi.  If any issues are encountered with the automated tests, the build will fail and you will have a tag with no corresponding release.

After a release is created, you can manually edit the release description through github.

## Documentation

The project documentation is written in reStructuredText and is built using Sphinx, which also includes the docstring documentation from the `btrdb` Python package. For your convenience, the `Makefile` includes a target for building the documentation:

    $ make html

This will build the HTML documentation locally in `docs/build`, which can be viewed using `open docs/build/index.html`. Other formats (PDF, epub, etc) can be built using `docs/Makefile`. The documentation is automatically built on every GitHub release and hosted on [Read The Docs](https://btrdb.readthedocs.io/en/latest/).

Note that the documentation also requires Sphix and other dependencies to successfully build: `pip install -r docs/requirements.txt`.

## Versioning

This codebases uses a form of [Semantic Versioning](http://semver.org/) to structure version numbers.  In general, the major version number will track with the BTrDB codebase to transparently maintain version compatibility.  Planned features between major versions will increment the minor version while any special releases (bug fixes, etc.) will increment the patch number.
