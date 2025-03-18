.. _installation-page:

===================================================
Installing Copernicus Marine Toolbox
===================================================

There are several ways to install or use the Copernicus Marine Toolbox:

* via pip (see `PyPI repository <https://pypi.org/project/copernicusmarine/>`_)
* via mamba | conda (see `conda-forge channel <https://anaconda.org/conda-forge/copernicusmarine>`_)
* via docker (see `dockerhub repository <https://hub.docker.com/r/copernicusmarine/copernicusmarine>`_)

Alternatively, you can use a binary.

.. note::

    Requires Python ``>=3.9``.

.. note::

    Note that the use of ``xarray<2024.7.0`` with ``numpy>=2.0.0`` leads to inconsistent results.
    See :ref:`later on this page <installation-page-dependencies>` for more details.

Via pip
**************

If you already have an environment (safer to clone it), the package can be installed using the ``pip`` command from the Python Package Index:

.. code-block:: bash

    python -m pip install copernicusmarine

And to **upgrade the package** to the newest available version, run:

.. code-block:: bash

    python -m pip install copernicusmarine --upgrade


Via mamba | conda (conda-forge channel)
**********************************************

A ``conda`` package is available on `Anaconda <https://anaconda.org/conda-forge/copernicusmarine>`_.

You can install it using ``mamba`` (or conda) through the ``conda-forge`` channel with the following command:

.. code-block:: bash

    mamba install conda-forge::copernicusmarine --yes

To upgrade the toolbox with mamba (or conda):

.. code-block:: bash

    mamba update --name copernicusmarine copernicusmarine --yes

Or using ``conda``:

.. code-block:: bash

    conda install -c conda-forge copernicusmarine

Via docker
**************

A Docker image is also available on the `copernicusmarine dockerhub repository <https://hub.docker.com/r/copernicusmarine/copernicusmarine>`_.

First step is to pull the container image:

.. code-block:: bash

    docker pull copernicusmarine/copernicusmarine:latest

Then run it:

.. code-block:: bash

    docker run -it --rm copernicusmarine/copernicusmarine --version


Create an account on Copernicus Marine website
***********************************************

To be able to download Copernicus Marine data, you need to have an account on the Copernicus Marine website. You can create an account on the `Copernicus Marine registration page <https://data.marine.copernicus.eu/register>`_.



Use the Copernicus Marine Toolbox binaries
***********************************************

In the `release page <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases>`_ you can access the binaries of the latest releases.

To download directly the latest stable releases:

- MacOS arm64: |download_macos_arm64|
- MacOS x86_64: |download_macos_x86|
- Linux (with glibc 2.35): |download_linux_235|
- Linux (with glibc 2.39): |download_linux_239|
- Windows: |download_windows|


Once downloaded for the specific platform, you can use the Toolbox by running the binary as follows:

In mac-os or linux:

.. code-block:: bash

    ./copernicusmarine_macos-x86_64.cli describe

(``describe`` or any other command)

You might have to update the permissions of the binary to be able to execute it with linux:

.. code-block:: bash

    chmod +rwx copernicusmarine_linux-glibc-2.35.cli

And from a Windows os (cmd):

.. code-block:: bash

    copernicusmarine.exe describe

(``describe`` or any other command)

.. note::

    For the **Linux** binaries you need a version of Glibc higher than the one the binary has. To check your version of Glibc, use ``ldd --version`` in your terminal.

.. note::

    The **Linux** binaries are tested with Ubuntu machines.

.. note::

    The **MacOS** binaries might have to be authorized to run in your system. Follow the steps in the popup window to authorize the binary.

.. _installation-page-dependencies:

Dependencies
**************
The Copernicus Marine Toolbox has the following dependencies:

- `Python <https://www.python.org/>`__ (3.9 or later)
- `click <https://click.palletsprojects.com/>`__ (8.0.4 or later)
- `requests <https://docs.python-requests.org/en/latest/>`__ (2.27.1 or later)
- `setuptools <https://setuptools.pypa.io/en/latest/>`__ (68.2.2 or later)
- `xarray <https://xarray.pydata.org/>`__ (2023.4.0 or later)
- `tqdm <https://tqdm.github.io/>`__ (4.65.0 or later)
- `zarr <https://zarr.readthedocs.io/en/stable/>`__ (2.13.3 or later)
- `dask <https://www.dask.org/>`__ (2022 or later)
- `boto3 <https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>`__ (1.26 or later)
- `semver <https://python-semver.readthedocs.io/en/latest/>`__ (0.2 or later)
- `pystac <https://pystac.readthedocs.io/en/stable/>`__ (1.8.3 or later)
- `lxml <https://lxml.de/>`__ (4.9.0 or later)
- `numpy <https://www.numpy.org/>`__ (1.23 or later)
- `pydantic <https://docs.pydantic.dev/>`__ (2.9.1 or later)
- `h5netcdf <https://h5netcdf.org>`__ (1.4.0 or later)
- `arcosparse <https://pypi.org/project/arcosparse/>`__ (0.2.0 or later)


The Copernicus Marine Toolbox uses the xarray library to handle the data when using the ``subset`` command in the majority of cases.
There are some compatibility issues with the latest versions of xarray and numpy:

- ``xarray<2024.7.0`` with ``numpy>=2.0.0`` leads to inconsistent results. See this issue: `xarray issue 1 <https://github.com/pydata/xarray/issues/9179>`_.
- ``xarray<2024.10.0`` with ``numpy>=2.0.0`` leads to some time overhead. See this issue: `xarray issue 2 <https://github.com/pydata/xarray/issues/9545>`_.

Also to convert subsetted data to NetCDF format the toolbox uses the `xarray.Dataset.to_netcdf <https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_netcdf.html>`_
and ``h5netcdf`` library as the backend.

.. note::

    The ``h5netcdf`` library is not compatible with the NetCDF3 format.
    If you need to save files in NetCDF3 format please just manually install ``netcdf4``
    library (see also `netCDF4 page <https://unidata.github.io/netcdf4-python/>`_):

    .. code-block:: bash

        # with conda | mamba | micromamba
        conda install -c conda-forge netCDF4
        # or add it to you environment.yml file

        # with pip
        python -m pip install netCDF4



    The docker image of the toolbox should already have the ``netcdf4`` library installed.


Domains required by the Copernicus Marine Toolbox
********************************************************
To be able to use the Copernicus Marine Services, you need to be able to access those domains:

- ``https://cmems-cas.cls.fr``: for the old authentication process.
- ``https://auth.marine.copernicus.eu``: for the new authentication process.
- ``https://s3.waw3-1.cloudferro.com``: for the data.

To check if you are able to access ``https://s3.waw3-1.cloudferro.com`` the way the toolbox is doing it you can do the following steps.

First, open a Python console in the same environment as you would run your script:

.. code-block:: bash

    python

Then, run a requests and check that the result is as expected:

.. code-block:: python

    import requests

    # you can pass here proxies and ssl configuration if needed
    response = requests.get(
        "https://s3.waw3-1.cloudferro.com/mdl-metadata/mdsVersions.json"
    )
    response.raise_for_status()

    print(response.json())

    # you should get something like:
    # {'systemVersions': {'mds': '1.0.0', [..] 'mds/serverlessArco/meta': '>=1.2.2'}}


For the authentication, check that you can run the ``login`` command.
If you have an error related to HTTP calls or internet connection,
please check with your IT support.
