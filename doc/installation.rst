===================================================
Installing Copernicus Marine toolbox
===================================================

There are several ways to install or use the Copernicus Marine Toolbox:

* via pip (see `PyPI repository <https://pypi.org/project/copernicusmarine/>`_)
* via mamba | conda (see `conda-forge channel <https://anaconda.org/conda-forge/copernicusmarine>`_)
* via docker (see `dockerhub repository <https://hub.docker.com/r/copernicusmarine/copernicusmarine>`_)

Alternatively, you can use a binary.

.. note::

    Requires Python ``>=3.9`` and ``<3.13``.

.. note::

    Note that the use of ``xarray<2024.7.0`` with ``numpy>=2.0.0`` leads to inconsistent results. See this issue: `xarray issue <https://github.com/pydata/xarray/issues/9179>`_.

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

In the `release page <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases>`_ you can access the binaries of the latest releases. Once downloaded for the specific platform, you can use the toolbox by running the binary as follows:

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
