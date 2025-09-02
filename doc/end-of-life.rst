=======================================================================
Versioning and End-of-Life Policy
=======================================================================

Purpose
-------

This document defines the versioning scheme and end-of-life (EOL) timelines for the **Copernicus Marine Toolbox** (CLI and Python package). It helps users plan upgrades and understand support timelines.

----

Versioning Scheme
-----------------

The Copernicus Marine Toolbox follows **semantic versioning**:

MAJOR.MINOR.PATCH
^^^^^^^^^^^^^^^^^

- **MAJOR** – Breaking changes (incompatible APIs, major architectural changes)
- **MINOR** – Backward-compatible features and improvements
- **PATCH** – Backward-compatible bug fixes

Example: ``2.1.4`` → major=2, minor=1, patch=4.

----

Support Policy
--------------

- Only the **latest minor/patch** in a major branch is supported.
- After EOL, no further fixes, updates, or security patches will be released.

The v1 is **no longer supported** and should be replaced with the ``v2``.

----

Release and End-of-Life Timeline
---------------------------------

+---------------------+--------------+----------------+-------------------+------------------------------------------------------+
| Version             | Release Date | End of Support | Latest version    |Notes                                                 |
+=====================+==============+================+===================+======================================================+
| 1.x                 | 2023-06      | 2025-07        | 1.3.6             | **[DEPRECATED]** First public release of new toolbox |
+---------------------+--------------+----------------+-------------------+------------------------------------------------------+
| 2.0                 | 2025-01      | TBD            | 2.0.1             | Major release with new features and improvements     |
+---------------------+--------------+----------------+-------------------+------------------------------------------------------+
| 2.1                 | 2025-05      | TBD            | 2.1.3             | New important features                               |
+---------------------+--------------+----------------+-------------------+------------------------------------------------------+
| 2.2                 | 2025-07      | TBD            | 2.2.1             | Current stable version                               |
+---------------------+--------------+----------------+-------------------+------------------------------------------------------+
| 2.3                 | 2025-09      | TBD            | Not yet released  | Coming soon                                          |
+---------------------+--------------+----------------+-------------------+------------------------------------------------------+


.. note::
   Dates are indicative. For the latest updates, visit the `Copernicus Marine User Support Portal <https://marine.copernicus.eu>`_.


Dependencies and Versions
----------------------------
Although for a more thorough view of the dependency versions allowed you can check the documentation of each version, here is a summary of the dependency versions for some of the more relevant libraries used:

========  ========  ===========  ===========
Version   Python    Xarray       Zarr
========  ========  ===========  ===========
2.0       3.9+      2023.4.0+    2.13.3+
2.1       3.9+      2023.4.0+    2.13.3 to 3.0.8
2.2       3.9+      2023.4.0+    2.13.3+
========  ========  ===========  ===========

.. note::
   For complete dependency specifications including optional packages,
   see the ``requirements.txt`` file for each version.
