===================================================
Installing Copernicus Marine Toolbox
===================================================

There are several ways to install or use the Copernicus Marine Toolbox:

* via pip `pypi`_
* via conda `conda-forge`_
* via docker `dockerhub`_

Alternatively, you can use a binary.

Via pip
**************

.. code-block:: bash

    pip install copernicusmarine

.. note:: Requires Python 3.9 or higher.

Via conda (conda-forge channel)
******************************************

.. code-block:: bash

    conda install -c conda-forge copernicusmarine

Via docker
**************

.. code-block:: bash

    docker run -it copernicusmarine/copernicusmarine

Create an account on Copernicus Marine website
***********************************************

To be able to use the Copernicus Marine Toolbox, you need to have an account on the Copernicus Marine website. You can create an account here `copernicusmarine-register`_.

.. _pypi: https://pypi.org/project/copernicusmarine/
.. _conda-forge: https://anaconda.org/conda-forge/copernicusmarine
.. _dockerhub: https://hub.docker.com/r/copernicusmarine/copernicusmarine
.. _copernicusmarine-register: https://data.marine.copernicus.eu/register

Use the CopernicusMarine Toolbox binaries
***********************************************

In the `release page <https://github.com/mercator-ocean/copernicus-marine-toolbox/releases>`_ you can access the binaries of the latest releases. Once downloaded for the specific platform, you can use the toolbox by running the binary as follows:

In mac-os or linux:

.. code-block:: bash

    ./copernicusmarine_macos-arm64.cli describe

(``describe`` or any other command)

You might have to update the permissions of the binary to be able to execute it with linux:

.. code-block:: bash

    chmod +rwx cmt_ubuntu-latest.cli

And from a Windows os (cmd):

.. code-block:: bash

    copernicusmarine.exe describe

(``describe`` or any other command)
