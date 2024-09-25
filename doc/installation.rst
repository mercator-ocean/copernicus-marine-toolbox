===================================================
Installing Copernicus Marine Toolbox
===================================================

There are several ways to install or use the Copernicus Marine Toolbox:

* via pip `pypi`_
* via conda `conda-forge`_
* via docker `dockerhub`_

Alternatively, you can use a binary.

.. note::

    Note that the use of ``xarray<2024.7.0`` with ``numpy>=2.0.0`` leads to inconsistent results. See this issue: `xarray issue <https://github.com/pydata/xarray/issues/9179>`_.

Via pip
**************

If you already have an environment (safer to clone it), the package can be installed using the ``pip`` command from the Python Package Index (`pypi`_):

.. code-block:: bash

    python -m pip install copernicusmarine

And to **upgrade the package** to the newest available version, run:

.. code-block:: bash

    python -m pip install copernicusmarine --upgrade


.. note:: Requires Python 3.9 or higher.

Via Mamba | Conda (conda-forge channel)
**********************************************

A ``conda`` package is available on `Anaconda`_.

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

A Docker image is also available here: `dockerhub`_.

First step is to pull the container image:

.. code-block:: bash

    docker pull copernicusmarine/copernicusmarine:latest

Then run it:

.. code-block:: bash

    docker run -it --rm copernicusmarine/copernicusmarine --version


Create an account on Copernicus Marine website
***********************************************

To be able to use the Copernicus Marine Toolbox, you need to have an account on the Copernicus Marine website. You can create an account here `copernicusmarine-register`_.

.. _pypi: https://pypi.org/project/copernicusmarine/
.. _conda-forge: https://anaconda.org/conda-forge/copernicusmarine
.. _dockerhub: https://hub.docker.com/r/copernicusmarine/copernicusmarine
.. _copernicusmarine-register: https://data.marine.copernicus.eu/register
.. _Anaconda: https://www.anaconda.com/products/individual


Use the CopernicusMarine Toolbox binaries
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
