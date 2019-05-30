Anaconda Package Creation Walkthrough
========================================

This walkthrough documents the process to create and upload the btrdb package to Anaconda Cloud.  At a high level, this will guide you through the following steps:

1. Activate your local environment and install dependenciess
2. Create and edit the `meta.yaml` file
3. Create local system builds for 3.6 and 3.7
4. Create builds for other platforms using conda's conversion command
5. Upload the packages to Anaconda Cloud

Activate an Anaconda environment
---------------------------------

I use `pyenv` and so the following was used to create a new environment for this work.  In general, you need to activate the Anaconda python and then use `conda` to create the virtual environment.  Your system may be configured differently so perform the necesssary steps as they apply to you.

  .. code-block:: bash

    $ pyenv activate anaconda3-2018.12
    $ conda create -n btrdb-conda python=3.7
    $ conda activate btrdb-conda
    $ conda info --env

Install build tools
---------------------------------

Ensure you have the Anaconda build tools and upload client.

  .. code-block:: bash

    $ conda install conda-build anaconda-client
    > Collecting package metadata: done
    > Solving environment: done
    >
    > # All requested packages already installed.


Create the meta.yaml file
---------------------------------

The build process is governed by a `meta.yaml` file which can be created manually or automatically from an existing pypi package.

We will choose the automated process though it does require some editing.  We will first create a `conda.recipe` subfolder to work in as shown below.

NOTE: It is asssumed you are starting in the root of the `master` branch of the `btrdb-python` codebase.

  .. code-block:: bash

    $ mkdir conda.recipe
    $ cd conda.recipe/
    $ conda skeleton pypi btrdb
    > Warning, the following versions were found for btrdb
    > 0.1
    > 0.2
    > 4.11.2
    > 5.2
    > 5.2.1
    > 5.2.2
    > Using 5.2.2
    > Use --version to specify a different version.
    > Using url https://files.pythonhosted.org/packages/99/82/475e1fdaab5c00b7b79a2a5c04c2e4ef7beb56d4ad38a083fbe0defffa16/btrdb-5.2.2.tar.gz (111 KB) for btrdb.
    > Downloading btrdb
    > PyPI URL:  https://files.pythonhosted.org/packages/99/82/475e1fdaab5c00b7b79a2a5c04c2e4ef7beb56d4ad38a083fbe0defffa16/btrdb-5.2.2.tar.gz
    > Unpacking btrdb...
    > done
    > working in /var/folders/bw/zscq42yd1_94c82m461qpvch0000gn/T/tmpg5rjjpe2conda_skeleton_btrdb-5.2.2.tar.gz
    > Collecting package metadata: ...working... done
    > Solving environment: ...working... done
    >
    > ## Package Plan ##
    >
    > environment location: ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/skeleton_1556636438675/_h_env_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_p
    >
    >
    > The following NEW packages will be INSTALLED:
    >
    >     ca-certificates: 2019.1.23-0
    >     certifi:         2019.3.9-py37_0
    >     libcxx:          4.0.1-hcfea43d_1
    >     libcxxabi:       4.0.1-hcfea43d_1
    >     libedit:         3.1.20181209-hb402a30_0
    >     libffi:          3.2.1-h475c297_4
    >     ncurses:         6.1-h0a44026_1
    >     openssl:         1.1.1b-h1de35cc_1
    >     pip:             19.1-py37_0
    >     python:          3.7.3-h359304d_0
    >     pyyaml:          5.1-py37h1de35cc_0
    >     readline:        7.0-h1de35cc_5
    >     setuptools:      41.0.1-py37_0
    >     sqlite:          3.28.0-ha441bb4_0
    >     tk:              8.6.8-ha441bb4_0
    >     wheel:           0.33.1-py37_0
    >     xz:              5.2.4-h1de35cc_4
    >     yaml:            0.1.7-hc338f04_2
    >     zlib:            1.2.11-h1de35cc_3
    >
    > Preparing transaction: ...working... done
    > Verifying transaction: ...working... done
    > Executing transaction: ...working... done
    > WARNING: symlink_conda() is deprecated.
    > Applying patch: '/var/folders/bw/zscq42yd1_94c82m461qpvch0000gn/T/tmpg5rjjpe2conda_skeleton_btrdb-5.2.2.tar.gz/pypi-distutils.patch'
    > Trying to apply patch as-is
    > INFO:conda_build.source:Trying to apply patch as-is
    > patching file core.py
    > Hunk #1 succeeded at 168 with fuzz 2 (offset 1 line).
    > Writing recipe for btrdb
    > --dirty flag and --keep-old-work not specified. Removing build/test folder after successful build/test.
    >
    > INFO:conda_build.config:--dirty flag and --keep-old-work not specified. Removing build/test folder after successful build/test.


This will create a `btrdb` subfolder containing the `meta.yaml` file we need.  At this point you should move it to the root of the `btrdb-python` codebase and delete the temporary work folder.  This folder was needed as the skeleton process will want to create a `btrdb` directory and will error if it already sees one.  In theory you could have just done this in `/tmp`.

  .. code-block:: bash

    $ mv btrdb/meta.yaml ../
    $ cd ../
    $ rm -rf conda.recipe/

Modify the requirements file
-----------------------------

At this time, the codebase has pinned the grpc dependencies at `1.12.1` which needs to be modified by hand as that version doesnt exist in Anaconda.  A diff is provided below so you can edit or patch the file.

  .. code-block:: diff

    diff --git a/requirements.txt b/requirements.txt
    index f504f26..403b1d4 100644
    --- a/requirements.txt
    +++ b/requirements.txt
    @@ -1,6 +1,6 @@
    # GRPC / Protobuff related
    -grpcio==1.12.1
    -grpcio-tools==1.12.1
    +grpcio==1.16.1
    +grpcio-tools==1.16.1

    # Time related utils
    pytz


Modify the meta.yaml file
--------------------------

A few changes will be needed in the `meta.yaml` file.  If you needed to update the requirements file then you will need to make the corresponding changes here.  We also need to:

* tell conda-build to use the local codebase rather than fetch from pypi again
* enter our maintainer username
* add `pytest-runner` as a dependency
* remove btrdb4 from test imports

You may wish to update the build number as well.  A patch is supplied below to guide you through the edits.

  .. code-block:: diff

    --- meta.yaml	2019-04-30 11:00:47.000000000 -0400
    +++ ../meta.yaml	2019-04-30 11:18:20.000000000 -0400
    @@ -6,8 +6,7 @@
    version: "{{ version }}"

    source:
    -  url: https://pypi.io/packages/source/{{ name[0] }}/{{ name }}/{{ name }}-{{ version }}.tar.gz
    -  sha256: 7a282ec39f887d336fe90977c0909c44bb9735e04ba6a6777d603e8922286b11
    +  path: .

    build:
    number: 0
    @@ -15,23 +14,24 @@

    requirements:
    host:
    -    - grpcio ==1.12.1
    -    - grpcio-tools ==1.12.1
    +    - grpcio ==1.16.1
    +    - grpcio-tools ==1.16.1
        - pip
        - python
        - pytz
    +    - pytest-runner
    run:
    -    - grpcio ==1.12.1
    -    - grpcio-tools ==1.12.1
    +    - grpcio ==1.16.1
    +    - grpcio-tools ==1.16.1
        - python
        - pytz
    +    - pytest-runner

    test:
    imports:
        - btrdb
        - btrdb.grpcinterface
        - btrdb.utils
    -    - btrdb4
        - tests.btrdb
    requires:
        - pytest
    @@ -40,11 +40,11 @@
    home: http://btrdb.io/
    license: BSD
    license_family: BSD
    -  license_file:
    +  license_file:
    summary: Bindings to interact with the Berkeley Tree Database using gRPC.
    -  doc_url:
    -  dev_url:
    +  doc_url:
    +  dev_url:

    extra:
    recipe-maintainers:
    -    - your-github-id-here
    +    - looselycoupled


Turn off automatic upload
---------------------------------

Anaconda will normally ask to upload the build right away and we want to turn that off.

  .. code-block:: bash

    $ conda config --set anaconda_upload no


Create local builds for Python 3.6 & 3.7
------------------------------------------

Create the initial builds using `conda-build . --python 3.6` and `conda-build . --python 3.7`.  The output should be almost identical and each should create an archive file to upload to Anaconda.  Creation for python 3.6 is shown below - BE SURE TO DO BOTH.

  .. code-block:: bash

    $ conda-build . --python 3.6
    > No numpy version specified in conda_build_config.yaml.  Falling back to default numpy value of 1.11
    > WARNING:conda_build.metadata:No numpy version specified in conda_build_config.yaml.  Falling back to default numpy value of 1.11
    > Adding in variants from internal_defaults
    > INFO:conda_build.variants:Adding in variants from internal_defaults
    > Adding in variants from config.variant
    > INFO:conda_build.variants:Adding in variants from config.variant
    > Attempting to finalize metadata for btrdb
    > INFO:conda_build.metadata:Attempting to finalize metadata for btrdb
    > Collecting package metadata: ...working... done
    > Solving environment: ...working... done
    > Collecting package metadata: ...working... done
    > Solving environment: ...working... done
    > BUILD START: ['btrdb-5.2.2-py36_0.tar.bz2']
    > Collecting package metadata: ...working... done
    > Solving environment: ...working... done
    >
    > ## Package Plan ##
    >
    > environment location: ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/_h_env_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_plac
    >
    >
    > The following NEW packages will be INSTALLED:
    >
    >     atomicwrites:    1.3.0-py36_1
    >     attrs:           19.1.0-py36_1
    >     c-ares:          1.15.0-h1de35cc_1
    >     ca-certificates: 2019.1.23-0
    >     certifi:         2019.3.9-py36_0
    >     grpcio:          1.16.1-py36h044775b_1
    >     grpcio-tools:    1.16.1-py36h0a44026_0
    >     libcxx:          4.0.1-hcfea43d_1
    >     libcxxabi:       4.0.1-hcfea43d_1
    >     libedit:         3.1.20181209-hb402a30_0
    >     libffi:          3.2.1-h475c297_4
    >     libprotobuf:     3.6.1-hd9629dc_0
    >     more-itertools:  7.0.0-py36_0
    >     ncurses:         6.1-h0a44026_1
    >     openssl:         1.1.1b-h1de35cc_1
    >     pip:             19.1-py36_0
    >     pluggy:          0.9.0-py36_0
    >     protobuf:        3.6.1-py36h0a44026_0
    >     py:              1.8.0-py36_0
    >     pytest:          4.4.1-py36_0
    >     pytest-runner:   4.4-py_0
    >     python:          3.6.8-haf84260_0
    >     pytz:            2019.1-py_0
    >     readline:        7.0-h1de35cc_5
    >     setuptools:      41.0.1-py36_0
    >     six:             1.12.0-py36_0
    >     sqlite:          3.28.0-ha441bb4_0
    >     tk:              8.6.8-ha441bb4_0
    >     wheel:           0.33.1-py36_0
    >     xz:              5.2.4-h1de35cc_4
    >     zlib:            1.2.11-h1de35cc_3
    >
    > Preparing transaction: ...working... done
    > Verifying transaction: ...working... done
    > Executing transaction: ...working... done
    > WARNING: symlink_conda() is deprecated.
    > Collecting package metadata: ...working... done
    > Solving environment: ...working... done
    > WARNING: symlink_conda() is deprecated.
    > Copying ~/Projects/btrdb-python to ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/work
    > source tree in: ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/work
    > export PREFIX=~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/_h_env_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_plac
    > export BUILD_PREFIX=~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/_build_env
    > export SRC_DIR=~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/work
    > Ignoring indexes: https://pypi.org/simple
    > Created temporary directory: /private/tmp/pip-ephem-wheel-cache-gtjimxin
    > Created temporary directory: /private/tmp/pip-req-tracker-edac7b_8
    > Created requirements tracker '/private/tmp/pip-req-tracker-edac7b_8'
    > Created temporary directory: /private/tmp/pip-install-_1_opxxt
    > Processing $SRC_DIR
    > Created temporary directory: /private/tmp/pip-req-build-k_m_ndib
    > Added file://$SRC_DIR to build tracker '/private/tmp/pip-req-tracker-edac7b_8'
    >     Running setup.py (path:/private/tmp/pip-req-build-k_m_ndib/setup.py) egg_info for package from file://$SRC_DIR
    >     Running command python setup.py egg_info
    >     running egg_info
    >     creating pip-egg-info/btrdb.egg-info
    >     writing pip-egg-info/btrdb.egg-info/PKG-INFO
    >     writing dependency_links to pip-egg-info/btrdb.egg-info/dependency_links.txt
    >     writing entry points to pip-egg-info/btrdb.egg-info/entry_points.txt
    >     writing requirements to pip-egg-info/btrdb.egg-info/requires.txt
    >     writing top-level names to pip-egg-info/btrdb.egg-info/top_level.txt
    >     writing manifest file 'pip-egg-info/btrdb.egg-info/SOURCES.txt'
    >     reading manifest file 'pip-egg-info/btrdb.egg-info/SOURCES.txt'
    >     reading manifest template 'MANIFEST.in'
    >     warning: no files found matching '*.rst'
    >     no previously-included directories found matching 'docs/build'
    >     warning: no previously-included files matching '__pycache__' found anywhere in distribution
    >     warning: no previously-included files matching '.ipynb_checkpoints' found anywhere in distribution
    >     warning: no previously-included files matching '.DS_Store' found anywhere in distribution
    >     warning: no previously-included files matching '.env' found anywhere in distribution
    >     warning: no previously-included files matching '.coverage.*' found anywhere in distribution
    >     writing manifest file 'pip-egg-info/btrdb.egg-info/SOURCES.txt'
    > Source in /private/tmp/pip-req-build-k_m_ndib has version 5.2.2, which satisfies requirement btrdb==5.2.2 from file://$SRC_DIR
    > Removed btrdb==5.2.2 from file://$SRC_DIR from build tracker '/private/tmp/pip-req-tracker-edac7b_8'
    > Building wheels for collected packages: btrdb
    > Created temporary directory: /private/tmp/pip-wheel-p0c4kbcr
    > Building wheel for btrdb (setup.py): started
    > Destination directory: /private/tmp/pip-wheel-p0c4kbcr
    > Running command ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/_h_env_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_plac/bin/python -u -c 'import setuptools, tokenize;__file__='"'"'/private/tmp/pip-req-build-k_m_ndib/setup.py'"'"';f=getattr(tokenize, '"'"'open'"'"', open)(__file__);code=f.read().replace('"'"'\r\n'"'"', '"'"'\n'"'"');f.close();exec(compile(code, __file__, '"'"'exec'"'"'))' bdist_wheel -d /private/tmp/pip-wheel-p0c4kbcr --python-tag cp36
    > running bdist_wheel
    > running build
    > running build_py
    > creating build
    > creating build/lib
    > creating build/lib/btrdb4
    > copying btrdb4/__init__.py -> build/lib/btrdb4
    > creating build/lib/btrdb
    > copying btrdb/version.py -> build/lib/btrdb
    > copying btrdb/transformers.py -> build/lib/btrdb
    > copying btrdb/__init__.py -> build/lib/btrdb
    > copying btrdb/stream.py -> build/lib/btrdb
    > copying btrdb/point.py -> build/lib/btrdb
    > copying btrdb/endpoint.py -> build/lib/btrdb
    > copying btrdb/exceptions.py -> build/lib/btrdb
    > copying btrdb/conn.py -> build/lib/btrdb
    > creating build/lib/tests
    > creating build/lib/tests/btrdb
    > copying tests/btrdb/test_point.py -> build/lib/tests/btrdb
    > copying tests/btrdb/__init__.py -> build/lib/tests/btrdb
    > copying tests/btrdb/test_base.py -> build/lib/tests/btrdb
    > copying tests/btrdb/test_transformers.py -> build/lib/tests/btrdb
    > copying tests/btrdb/test_conn.py -> build/lib/tests/btrdb
    > copying tests/btrdb/test_stream.py -> build/lib/tests/btrdb
    > creating build/lib/btrdb/grpcinterface
    > copying btrdb/grpcinterface/__init__.py -> build/lib/btrdb/grpcinterface
    > copying btrdb/grpcinterface/btrdb_pb2_grpc.py -> build/lib/btrdb/grpcinterface
    > copying btrdb/grpcinterface/btrdb_pb2.py -> build/lib/btrdb/grpcinterface
    > creating build/lib/btrdb/utils
    > copying btrdb/utils/conversion.py -> build/lib/btrdb/utils
    > copying btrdb/utils/__init__.py -> build/lib/btrdb/utils
    > copying btrdb/utils/buffer.py -> build/lib/btrdb/utils
    > copying btrdb/utils/timez.py -> build/lib/btrdb/utils
    > copying btrdb/utils/general.py -> build/lib/btrdb/utils
    > copying btrdb/grpcinterface/btrdb.proto -> build/lib/btrdb/grpcinterface
    > installing to build/bdist.macosx-10.7-x86_64/wheel
    > running install
    > running install_lib
    > creating build/bdist.macosx-10.7-x86_64
    > creating build/bdist.macosx-10.7-x86_64/wheel
    > creating build/bdist.macosx-10.7-x86_64/wheel/btrdb4
    > copying build/lib/btrdb4/__init__.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb4
    > creating build/bdist.macosx-10.7-x86_64/wheel/tests
    > creating build/bdist.macosx-10.7-x86_64/wheel/tests/btrdb
    > copying build/lib/tests/btrdb/test_point.py -> build/bdist.macosx-10.7-x86_64/wheel/tests/btrdb
    > copying build/lib/tests/btrdb/__init__.py -> build/bdist.macosx-10.7-x86_64/wheel/tests/btrdb
    > copying build/lib/tests/btrdb/test_base.py -> build/bdist.macosx-10.7-x86_64/wheel/tests/btrdb
    > copying build/lib/tests/btrdb/test_transformers.py -> build/bdist.macosx-10.7-x86_64/wheel/tests/btrdb
    > copying build/lib/tests/btrdb/test_conn.py -> build/bdist.macosx-10.7-x86_64/wheel/tests/btrdb
    > copying build/lib/tests/btrdb/test_stream.py -> build/bdist.macosx-10.7-x86_64/wheel/tests/btrdb
    > creating build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > creating build/bdist.macosx-10.7-x86_64/wheel/btrdb/grpcinterface
    > copying build/lib/btrdb/grpcinterface/__init__.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/grpcinterface
    > copying build/lib/btrdb/grpcinterface/btrdb_pb2_grpc.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/grpcinterface
    > copying build/lib/btrdb/grpcinterface/btrdb_pb2.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/grpcinterface
    > copying build/lib/btrdb/grpcinterface/btrdb.proto -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/grpcinterface
    > copying build/lib/btrdb/version.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > copying build/lib/btrdb/transformers.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > copying build/lib/btrdb/__init__.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > creating build/bdist.macosx-10.7-x86_64/wheel/btrdb/utils
    > copying build/lib/btrdb/utils/conversion.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/utils
    > copying build/lib/btrdb/utils/__init__.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/utils
    > copying build/lib/btrdb/utils/buffer.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/utils
    > copying build/lib/btrdb/utils/timez.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/utils
    > copying build/lib/btrdb/utils/general.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb/utils
    > copying build/lib/btrdb/stream.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > copying build/lib/btrdb/point.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > copying build/lib/btrdb/endpoint.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > copying build/lib/btrdb/exceptions.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > copying build/lib/btrdb/conn.py -> build/bdist.macosx-10.7-x86_64/wheel/btrdb
    > running install_egg_info
    > running egg_info
    > creating btrdb.egg-info
    > writing btrdb.egg-info/PKG-INFO
    > writing dependency_links to btrdb.egg-info/dependency_links.txt
    > writing entry points to btrdb.egg-info/entry_points.txt
    > writing requirements to btrdb.egg-info/requires.txt
    > writing top-level names to btrdb.egg-info/top_level.txt
    > writing manifest file 'btrdb.egg-info/SOURCES.txt'
    > reading manifest file 'btrdb.egg-info/SOURCES.txt'
    > reading manifest template 'MANIFEST.in'
    > warning: no files found matching '*.rst'
    > no previously-included directories found matching 'docs/build'
    > warning: no previously-included files matching '__pycache__' found anywhere in distribution
    > warning: no previously-included files matching '.ipynb_checkpoints' found anywhere in distribution
    > warning: no previously-included files matching '.DS_Store' found anywhere in distribution
    > warning: no previously-included files matching '.env' found anywhere in distribution
    > warning: no previously-included files matching '.coverage.*' found anywhere in distribution
    > writing manifest file 'btrdb.egg-info/SOURCES.txt'
    > Copying btrdb.egg-info to build/bdist.macosx-10.7-x86_64/wheel/btrdb-5.2.2-py3.6.egg-info
    > running install_scripts
    > creating build/bdist.macosx-10.7-x86_64/wheel/btrdb-5.2.2.dist-info/WHEEL
    > creating '/private/tmp/pip-wheel-p0c4kbcr/btrdb-5.2.2-cp36-none-any.whl' and adding 'build/bdist.macosx-10.7-x86_64/wheel' to it
    > adding 'btrdb/__init__.py'
    > adding 'btrdb/conn.py'
    > adding 'btrdb/endpoint.py'
    > adding 'btrdb/exceptions.py'
    > adding 'btrdb/point.py'
    > adding 'btrdb/stream.py'
    > adding 'btrdb/transformers.py'
    > adding 'btrdb/version.py'
    > adding 'btrdb/grpcinterface/__init__.py'
    > adding 'btrdb/grpcinterface/btrdb.proto'
    > adding 'btrdb/grpcinterface/btrdb_pb2.py'
    > adding 'btrdb/grpcinterface/btrdb_pb2_grpc.py'
    > adding 'btrdb/utils/__init__.py'
    > adding 'btrdb/utils/buffer.py'
    > adding 'btrdb/utils/conversion.py'
    > adding 'btrdb/utils/general.py'
    > adding 'btrdb/utils/timez.py'
    > adding 'btrdb4/__init__.py'
    > adding 'tests/btrdb/__init__.py'
    > adding 'tests/btrdb/test_base.py'
    > adding 'tests/btrdb/test_conn.py'
    > adding 'tests/btrdb/test_point.py'
    > adding 'tests/btrdb/test_stream.py'
    > adding 'tests/btrdb/test_transformers.py'
    > adding 'btrdb-5.2.2.dist-info/LICENSE.txt'
    > adding 'btrdb-5.2.2.dist-info/METADATA'
    > adding 'btrdb-5.2.2.dist-info/WHEEL'
    > adding 'btrdb-5.2.2.dist-info/entry_points.txt'
    > adding 'btrdb-5.2.2.dist-info/top_level.txt'
    > adding 'btrdb-5.2.2.dist-info/RECORD'
    > removing build/bdist.macosx-10.7-x86_64/wheel
    > Building wheel for btrdb (setup.py): finished with status 'done'
    > Stored in directory: /private/tmp/pip-ephem-wheel-cache-gtjimxin/wheels/ec/f7/a1/3002672c4bb16c0f3cb50d506bba07c39ed7ca2f9d7e76f1b6
    > Removing source in /private/tmp/pip-req-build-k_m_ndib
    > Successfully built btrdb
    > Installing collected packages: btrdb
    >
    > Successfully installed btrdb-5.2.2
    > Cleaning up...
    > Removed build tracker '/private/tmp/pip-req-tracker-edac7b_8'
    >
    > Resource usage statistics from building btrdb:
    > Process count: 1
    > CPU time: Sys=0:00:00.0, User=0:00:00.0
    > Memory: 1.3M
    > Disk usage: 2.8K
    > Time elapsed: 0:00:02.0
    >
    > Packaging btrdb
    > INFO:conda_build.build:Packaging btrdb
    > Packaging btrdb-5.2.2-py36_0
    > INFO:conda_build.build:Packaging btrdb-5.2.2-py36_0
    > compiling .pyc files...
    > number of files: 54
    > WARNING (btrdb): dso library package defaults::python-3.6.8-haf84260_0 in requirements/run but it is not used (i.e. it is overdepending or perhaps statically linked? If that is what you want then add it to `build/ignore_run_exports`)
    > INFO (btrdb): plugin library package defaults::grpcio-tools-1.16.1-py36h0a44026_0 in requirements/run but it is not used (i.e. it is overdepending or perhaps statically linked? If that is what you want then add it to `build/ignore_run_exports`)
    > INFO (btrdb): plugin library package defaults::grpcio-1.16.1-py36h044775b_1 in requirements/run but it is not used (i.e. it is overdepending or perhaps statically linked? If that is what you want then add it to `build/ignore_run_exports`)
    > Fixing permissions
    > Compressing to /var/folders/bw/zscq42yd1_94c82m461qpvch0000gn/T/tmpgenfk0tv/btrdb-5.2.2-py36_0.tar.bz2
    > Package verification results:
    > -----------------------------
    > /var/folders/bw/zscq42yd1_94c82m461qpvch0000gn/T/tmpgenfk0tv/btrdb-5.2.2-py36_0.tar.bz2: C1139 Found pyc file "info/recipe/btrdb/__pycache__/__init__.cpython-37.pyc" in invalid directory
    > TEST START: ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py36_0.tar.bz2
    > Adding in variants from /var/folders/bw/zscq42yd1_94c82m461qpvch0000gn/T/tmpeszvwlz8/info/recipe/conda_build_config.yaml
    > INFO:conda_build.variants:Adding in variants from /var/folders/bw/zscq42yd1_94c82m461qpvch0000gn/T/tmpeszvwlz8/info/recipe/conda_build_config.yaml
    > Renaming work directory,  ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/work  to  ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/work_moved_btrdb-5.2.2-py36_0_osx-64
    > Collecting package metadata: ...working... done
    > Solving environment: ...working... done
    >
    > ## Package Plan ##
    >
    > environment location: ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/_test_env_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_p
    >
    >
    > The following NEW packages will be INSTALLED:
    >
    >     atomicwrites:    1.3.0-py36_1
    >     attrs:           19.1.0-py36_1
    >     btrdb:           5.2.2-py36_0            local
    >     c-ares:          1.15.0-h1de35cc_1
    >     ca-certificates: 2019.1.23-0
    >     certifi:         2019.3.9-py36_0
    >     grpcio:          1.16.1-py36h044775b_1
    >     grpcio-tools:    1.16.1-py36h0a44026_0
    >     libcxx:          4.0.1-hcfea43d_1
    >     libcxxabi:       4.0.1-hcfea43d_1
    >     libedit:         3.1.20181209-hb402a30_0
    >     libffi:          3.2.1-h475c297_4
    >     libprotobuf:     3.6.1-hd9629dc_0
    >     more-itertools:  7.0.0-py36_0
    >     ncurses:         6.1-h0a44026_1
    >     openssl:         1.1.1b-h1de35cc_1
    >     pip:             19.1-py36_0
    >     pluggy:          0.9.0-py36_0
    >     protobuf:        3.6.1-py36h0a44026_0
    >     py:              1.8.0-py36_0
    >     pytest:          4.4.1-py36_0
    >     pytest-runner:   4.4-py_0
    >     python:          3.6.8-haf84260_0
    >     pytz:            2019.1-py_0
    >     readline:        7.0-h1de35cc_5
    >     setuptools:      41.0.1-py36_0
    >     six:             1.12.0-py36_0
    >     sqlite:          3.28.0-ha441bb4_0
    >     tk:              8.6.8-ha441bb4_0
    >     wheel:           0.33.1-py36_0
    >     xz:              5.2.4-h1de35cc_4
    >     zlib:            1.2.11-h1de35cc_3
    >
    > Preparing transaction: ...working... done
    > Verifying transaction: ...working... done
    > Executing transaction: ...working... done
    > WARNING: symlink_conda() is deprecated.
    > export PREFIX=~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/_test_env_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_placehold_p
    > export SRC_DIR=~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/test_tmp
    > import: 'btrdb'
    > import: 'btrdb.grpcinterface'
    > import: 'btrdb.utils'
    > import: 'tests.btrdb'
    > import: 'btrdb'
    > import: 'btrdb.grpcinterface'
    > import: 'btrdb.utils'
    > import: 'tests.btrdb'
    >
    > Resource usage statistics from testing btrdb:
    > Process count: 1
    > CPU time: Sys=0:00:00.0, User=0:00:00.0
    > Memory: 1.3M
    > Disk usage: 32B
    > Time elapsed: 0:00:02.0
    >
    > TEST END: ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py36_0.tar.bz2
    > Renaming work directory,  ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/work  to  ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/btrdb_1556638144248/work_moved_btrdb-5.2.2-py36_0_osx-64_main_build_loop
    > # Automatic uploading is disabled
    > # If you want to upload package(s) to anaconda.org later, type:
    >
    > anaconda upload ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py36_0.tar.bz2
    >
    > # To have conda build upload to anaconda.org automatically, use
    > # $ conda config --set anaconda_upload yes
    >
    > anaconda_upload is not set.  Not uploading wheels: []
    > ####################################################################################
    > Resource usage summary:
    >
    > Total time: 0:00:31.5
    > CPU usage: sys=0:00:00.0, user=0:00:00.0
    > Maximum memory usage observed: 1.3M
    > Total disk usage observed (not including envs): 2.8K
    >
    >
    > ####################################################################################
    > Source and build intermediates have been left in ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld.
    > There are currently 1 accumulated.
    > To remove them, you can run the ```conda build purge``` command



Note where your build packages were created - they should be within a `conda-bld` folder with a subfolder matching your local platform designation (`osx-64` for me).  For me this is at:

.. code-block:: bash

    $ ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py36_0.tar.bz2



Create remaining platform packages
------------------------------------


Now that we have our locally built packages, we can easily create the remaining platform packages usiing `conda convert`.  Be sure to supply an output directory (ex: `-o platform-builds`) to keep them organized.

  .. code-block:: bash

    $ conda convert -f  --platform all ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py37_0.tar.bz2 -o platform-builds/
    > Source platform 'osx-64' and target platform 'osx-64' are identical. Skipping conversion.
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to linux-32
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to linux-64
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to linux-ppc64le
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to linux-armv6l
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to linux-armv7l
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to linux-aarch64
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to win-32
    > Converting btrdb-5.2.2-py37_0.tar.bz2 from osx-64 to win-64


    $ conda convert -f  --platform all ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py36_0.tar.bz2 -o platform-builds/
    > Source platform 'osx-64' and target platform 'osx-64' are identical. Skipping conversion.
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to linux-32
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to linux-64
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to linux-ppc64le
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to linux-armv6l
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to linux-armv7l
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to linux-aarch64
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to win-32
    > Converting btrdb-5.2.2-py36_0.tar.bz2 from osx-64 to win-64



Login to Anaconda Cloud
---------------------------------

Ensure you are logged in with the Anaconda Cloud utility program.

  .. code-block:: bash

    $ anaconda login
    > Using Anaconda API: https://api.anaconda.org
    > Username: loosely.coupled
    > loosely.coupled's Password:
    > login successful


Upload the install packages to Anaconda Cloud
----------------------------------------------

Upload both the Python 3.6 and 3.7 packages.  As these were created directly for the local system, we will upload them separately from the remaining platform packagees.

Be sure to use the `--user pingthings` argument to ensure it goes to the correct channel.

  .. code-block:: bash

    $ anaconda upload --user pingthings ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py37_0.tar.bz2
    > Using Anaconda API: https://api.anaconda.org
    > Using "pingthings" as upload username
    > Processing '~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py37_0.tar.bz2'
    > Detecting file type...
    > File type is "conda"
    > Extracting conda package attributes for upload
    > Creating package "btrdb"
    > Creating release "5.2.2"
    > Uploading file "pingthings/btrdb/5.2.2/osx-64/btrdb-5.2.2-py37_0.tar.bz2"
    > uploaded 168 of 168Kb: 100.00% ETA: 0.0 minutes
    > Upload complete
    >
    > conda package located at:
    > https://anaconda.org/pingthings/btrdb

    $ anaconda upload --user pingthings ~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py36_0.tar.bz2
    > Using Anaconda API: https://api.anaconda.org
    > Using "pingthings" as upload username
    > Processing '~/.pyenv/versions/anaconda3-2018.12/envs/btrdb-conda/conda-bld/osx-64/btrdb-5.2.2-py36_0.tar.bz2'
    > Detecting file type...
    > File type is "conda"
    > Extracting conda package attributes for upload
    > Creating package "btrdb"
    > Creating release "5.2.2"
    > Uploading file "pingthings/btrdb/5.2.2/osx-64/btrdb-5.2.2-py36_0.tar.bz2"
    > uploaded 1525 of 1525Kb: 100.00% ETA: 0.0 minutes
    > Upload complete
    >
    > conda package located at:
    > https://anaconda.org/pingthings/btrdb


The remaining install packages were created using `conda convert` and so are easily found in the `platform-builds` directory we created earliier.  Use `find` to locate these files and upload them to Anaconda cloud.

  .. code-block:: bash

    $ find platform-builds/ -name *.bz2 -exec anaconda upload --user pingthings {} \;
    > Using Anaconda API: https://api.anaconda.org
    > Using "pingthings" as upload username
    > Processing 'platform-builds//linux-64/btrdb-5.2.2-py36_0.tar.bz2'
    > Detecting file type...
    > File type is "conda"
    > Extracting conda package attributes for upload
    > Creating package "btrdb"
    > Creating release "5.2.2"
    > Uploading file "pingthings/btrdb/5.2.2/linux-64/btrdb-5.2.2-py36_0.tar.bz2"
    >  uploaded 1556 of 1556Kb: 100.00% ETA: 0.0 minutes
    > Upload complete
    >
    > conda package located at:
    > https://anaconda.org/pingthings/btrdb
    >
    > Using Anaconda API: https://api.anaconda.org
    > Using "pingthings" as upload username
    > Processing 'platform-builds//linux-64/btrdb-5.2.2-py37_0.tar.bz2'
    > Detecting file type...
    > File type is "conda"
    > Extracting conda package attributes for upload
    > Creating package "btrdb"
    > Creating release "5.2.2"
    > Uploading file "pingthings/btrdb/5.2.2/linux-64/btrdb-5.2.2-py37_0.tar.bz2"
    >  uploaded 172 of 172Kb: 100.00% ETA: 0.0 minutes
    > Upload complete
    >
    > conda package located at:
    > https://anaconda.org/pingthings/btrdb
    ...


Test the deployment
---------------------------------

You should probably test again to make sure everything worked as planned though
the build process does this to an extent by performing some test imports.

While not listed below, I tested by creating a clean virtual environment,
installing from anaconda cloud `conda install -c pingthings btrdb`, connecting
to a server, and calling `info()` on the connection object.

If you have any integration scripts now would be a good time to run them until
we can add integration testing to the CI test suite.

Alternatively, you can probably test the archive file before uploading to
Anaconda by using it as a source archive.
