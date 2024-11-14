.. _installation-page:

===================================================
Installing Copernicus Marine toolbox
===================================================

There are several ways to install or use the Copernicus Marine toolbox:

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

To upgrade the Toolbox with mamba (or conda):

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



Use the Copernicus Marine toolbox binaries
***********************************************

In the `release page <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases>`_ you can access the binaries of the latest releases.

To download directly the latest stable releases:

- MacOS arm64: `copernicusmarine_macos-arm64 <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v2.0.0a3/copernicusmarine_macos-arm64.cli>`_
- MacOS x86_64: `copernicusmarine_macos-x86_64 <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v2.0.0a3/copernicusmarine_macos-x86_64.cli>`_
- Linux: `copernicusmarine_linux <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v2.0.0a3/copernicusmarine_linux.cli>`_
- Windows: `copernicusmarine <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases/download/v2.0.0a3/copernicusmarine.exe>`_

Once downloaded for the specific platform, you can use the toolbox by running the binary as follows:

In mac-os or linux:

.. code-block:: bash

    ./copernicusmarine_macos-latest.cli describe

(``describe`` or any other command)

You might have to update the permissions of the binary to be able to execute it with linux:

.. code-block:: bash

    chmod +rwx cmt_ubuntu-latest.cli

And from a Windows os (cmd):

.. code-block:: bash

    copernicusmarine_windows-latest.exe describe

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

The Copernicus Marine toolbox uses the xarray library to handle the data when using the ``subset`` command.
There are some compatibility issues with the latest versions of xarray and numpy:

- ``xarray<2024.7.0`` with ``numpy>=2.0.0`` leads to inconsistent results. See this issue: `xarray issue 1 <https://github.com/pydata/xarray/issues/9179>`_.
- ``xarray<2024.10.0`` with ``numpy>=2.0.0`` leads to some time overhead. See this issue: `xarray issue 2 <https://github.com/pydata/xarray/issues/9545>`_.

Also to convert subsetted data to NetCDF format the toolbox uses the `xarray.Dataset.to_netcdf <https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_netcdf.html>`_
and ``h5netcdf`` library as the backend.

The ``h5netcdf`` library is not compatible with the NetCDF3 format.
If you want to use it please make sure to install the ``netcdf4`` library:

.. code-block:: bash

    python -m pip install netcdf4

.. note::

    The image of the toolbox should already have the ``netcdf4`` library installed.
