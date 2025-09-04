=======================================================================
Versioning and End-of-Life Policy
=======================================================================

Important Information
-------------------------

Conda no longer supports Python 3.9, so the upcoming toolbox release (``v2.3``) cannot be installed via conda when using Python 3.9.

.. warning::
   :class: big-warning

   ``v1`` is **no longer supported** and should be replaced with ``v2``.

Purpose
-------

This document defines the versioning scheme and end-of-life (EOL) timeline for the **Copernicus Marine Toolbox**. It helps users plan upgrades and understand support timelines.

----

Versioning Scheme
-----------------

The Copernicus Marine Toolbox follows `semantic versioning <https://semver.org>`_:

MAJOR.MINOR.PATCH
^^^^^^^^^^^^^^^^^

- **MAJOR** – Breaking changes (incompatible APIs, major architectural changes)
- **MINOR** – Backward-compatible features and improvements
- **PATCH** – Backward-compatible bug fixes

Example: ``2.1.4`` → major=2, minor=1, patch=4.

----

Support Policy
--------------

Patches will be applied to the latest minor version of supported major versions.

If you detect a bug, first update to the latest minor version of your current major version, if it is still supported. Otherwise, update to the next major version if possible. If the bug persists, please report it, and a bugfix will be applied to that minor version.

----

Release and End-of-Life Timeline
---------------------------------

+---------------------+--------------+----------------+-------------------+------------------------------------------------------+
| Version             | Release Date | End of Support | Latest version    |Notes                                                 |
+=====================+==============+================+===================+======================================================+
| 1.x                 | 2023-06      | 2025-07        | 1.3.6             | **[OBSOLETE]** First public release of new toolbox   |
+---------------------+--------------+----------------+-------------------+------------------------------------------------------+
| 2.x                 | 2025-01      | Not planned yet| 2.2.2             | Current stable version                               |
+---------------------+--------------+----------------+-------------------+------------------------------------------------------+


.. note::
   Dates are indicative. For the latest updates, visit the `Copernicus Marine User Support Portal <https://marine.copernicus.eu>`_.


Dependencies and Versions
----------------------------
For a more detailed overview of the allowed dependency versions, you can refer to the documentation for each version of the Copernicus Marine Toolbox. The version listed here always corresponds to the latest patch. Below is a summary of the dependency versions for some of the most relevant libraries used:

================  ========  ===========  =================  ========
Copernicusmarine  Python    Xarray       Zarr               Dask
================  ========  ===========  =================  ========
2.0.z             >=3.9     >=2023.4.0   >=2.13.3           >=2022
2.1.z             >=3.9     >=2023.4.0   >=2.13.3, <=3.0.8  >=2022
2.2.z             >=3.9     >=2023.4.0   >=2.13.3           >=2022
================  ========  ===========  =================  ========

.. note::
   For complete dependency specifications including optional packages,
   see the ``requirements.txt`` file for each version.
