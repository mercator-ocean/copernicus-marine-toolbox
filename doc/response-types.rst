.. _response-types:

=================
Response types
=================

The Copernicus Marine toolbox commands return some information when downloading.
It can contain useful metadata for the user.

For the :ref:`command line interface <command-line-interface>`, all the returned data will be in a form of a json sent to stdout.
You can easily save it locally by doing, for example:

.. code-block:: bash

    copernicusmarine get -i cmems_mod_glo_phy-thetao_anfc_0.083deg_P1M-m > my_json.json


.. note::
   For the "get" and "subset" commands you can get those metadata without
   downloading anything by using the ``dry_run`` option (or ``--dry-run`` flag for the CLI).

-------------------
Commands Response
-------------------

.. autoclass:: copernicusmarine.ResponseGet()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.ResponseSubset()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

--------------
Subtypes
--------------

.. autoclass:: copernicusmarine.FileGet()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.DatasetCoordinatesExtent()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource


.. autoclass:: copernicusmarine.GeographicalExtent()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.TimeExtent()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource
