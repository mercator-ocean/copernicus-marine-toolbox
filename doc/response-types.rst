.. _response-types:

=================
Response types
=================

The Copernicus Marine Toolbox commands return some information when downloading.
It can contain useful metadata for the user.

For the :ref:`command line interface <command-line-interface>`, all the returned data will
be in a form of a JSON sent to stdout (whereas the logs are sent to stderr).
You can easily save it locally by doing, for example:

.. code-block:: bash

    copernicusmarine get -i cmems_mod_glo_phy-thetao_anfc_0.083deg_P1M-m > my_json.json


.. note::
   For the "get" and "subset" commands you can get those metadata without
   downloading anything by using the ``dry_run`` option (or ``--dry-run`` flag for the command line interface).

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

.. autoclass:: copernicusmarine.CopernicusMarineCatalogue()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, filter_only_official_versions_and_parts
    :member-order: bysource

--------------
Subtypes
--------------

.. autoclass:: copernicusmarine.FileGet()
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

.. autoclass:: copernicusmarine.CopernicusMarineProduct()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, sort_versions, parse_dataset_metadata_items
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarineDataset()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, get_version, sort_versions, parse_dataset_metadata_items
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarineVersion()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, get_part, sort_parts
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarinePart()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, from_metadata_item, get_service_by_service_name
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarineService()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, from_metadata_item, get_axis_coordinate_id_mapping
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarineVariable()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, from_metadata_item
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarineCoordinate()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, from_metadata_item
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarineServiceNames()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.CoperniusMarineServiceShortNames()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.CopernicusMarineServiceFormat()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.StatusCode()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.StatusMessage()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields
    :member-order: bysource

.. autoclass:: copernicusmarine.FileStatus()
    :members:
    :undoc-members:
    :exclude-members: model_computed_fields, model_config, model_fields, get_status
    :member-order: bysource
