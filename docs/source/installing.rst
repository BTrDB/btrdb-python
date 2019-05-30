Installing
========================

The btrdb package has only a few requirements and is relatively easy to install.
A number of installation options are available as detailed below.

Installing with pip
-------------------

We recommend using pip to install btrdb-python on all platforms:

.. code-block:: bash

    $ pip install btrdb

To get a specific version of btrdb-python supply the version number.  The major
version of this library is pegged to the major version of the BTrDB database as
in the 4.x bindings are best used to speak to a 4.x BTrDB database.

.. code-block:: bash

    $ pip install btrdb==4.11.2


To upgrade using pip:

.. code-block:: bash

    $ pip install --upgrade btrdb


Installing with Anaconda
------------------------

If you'd like to use Anaconda, you'll need to download the library from the pingthings
channel as shown below.

Note however that only the version 5 bindings are available in Anaconda Cloud.  If you'd
like to install the version 4 bindings you will need to use `pip` as shown above.

.. code-block:: bash

    $ conda install -c pingthings btrdb


