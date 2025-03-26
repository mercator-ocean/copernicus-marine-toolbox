.. _describe-page:

=====================
Command ``describe``
=====================

The ``describe`` command retrieves metadata information about all products and datasets, displaying it as a JSON output.

**Usage:**

.. code-block:: bash

    copernicusmarine describe

Here the first 2 products are shown:

.. code-block:: json

    {
    "products": [
        {
        "title": "Antarctic Sea Ice Extent from Reanalysis",
        "product_id": "ANTARCTIC_OMI_SI_extent",
        "thumbnail_url": "https://catalogue.marine.copernicus.eu/documents/IMG/ANTARCTIC_OMI_SI_extent.png",
        "digital_object_identifier": "10.48670/moi-00186",
        "sources": [
            "Numerical models"
        ],
        "processing_level": null,
        "production_center": "Mercator Oc\u00e9an International"
        },
        {
        "title": "Antarctic Monthly Sea Ice Extent from Observations Reprocessing",
        "product_id": "ANTARCTIC_OMI_SI_extent_obs",
        "thumbnail_url": "https://catalogue.marine.copernicus.eu/documents/IMG/ANTARCTIC_OMI_SI_extent_obs.png",
        "digital_object_identifier": "10.48670/moi-00187",
        "sources": [
            "Satellite observations"
        ],
        "processing_level": null,
        "production_center": "MET Norway"
        },
    ]
    }


By default, the command only shows the products. To include the datasets, you can use the ``--return-fields datasets`` option.

To save the JSON output to a file, you can use the following command:

.. code-block:: bash

    copernicusmarine describe > all_products_copernicus_marine_service.json

.. note::
    At some point in the future, the command will be updated to include everything by default.
    Then the user will be able to use the ``--return-fields`` option or ``--exclude-fields`` to filter the output.

.. note::
    In the case of the Python interface, the describe will return directly the :class:`copernicusmarine.CopernicusMarineCatalogue` object.
    The catalogue will be complete except if you use the ``dataset_id``, ``product_id`` or ``contains`` arguments.

.. _return-fields:

``--return-fields`` and ``--exclude-fields`` options
----------------------------------------------------------------

You can use the ``--return-fields`` (``-r``) and ``--exclude-fields`` (``-e``) options to select the fields you want to see in the output.
The options allow to select respectively the fields you want to include or exclude from the output.
You just need to add them as a comma-separated list.

For example if you want only the URI of the services, you can use the following command:

**Example:**

.. code-block:: bash

    copernicusmarine describe --return-fields uri,product_id,dataset_id,service_name

The output will be something like this (only the first product is shown):

.. code-block:: json

    {
    "products": [
        {
        "product_id": "ANTARCTIC_OMI_SI_extent",
        "datasets": [
            {
            "dataset_id": "antarctic_omi_si_extent",
            "versions": [
                {
                "parts": [
                    {
                    "services": [
                        {
                            "service_name": "original-files",
                            "uri": "https://s3.waw3-1.cloudferro.com/mdl-native-10/native/ANTARCTIC_OMI_SI_extent/antarctic_omi_si_extent_202207/antarctic_omi_si_extent_19930115_P20220328.nc"
                        },
                        {
                            "service_name": "omi-arco",
                            "uri": "https://s3.waw3-1.cloudferro.com/mdl-arco-time-001/arco/ANTARCTIC_OMI_SI_extent/antarctic_omi_si_extent_202207/omi.zarr"
                        }
                    ]
                    }
                ]
                }
            ]
            }
        ]
        }
        ]
    }


.. note::
    You can use ``--return-fields all`` to show all fields.


``--contains`` option
----------------------

You also have the option to filter the output by using the ``--contains`` option. It will perform a search on all the text fields of the output.

**Example:**

If you want, for example, the ``cmems_obs-ins_glo_phy-temp-sal_my_cora_irr`` dataset only, you can use the following command:

.. code-block:: bash

    copernicusmarine describe --return-fields datasets --contains cmems_obs-ins_glo_phy-temp-sal_my_cora_irr

The output will be something like this:

.. code-block:: json

    {
        "products": [
            {
            "datasets": [
                {
                "dataset_id": "cmems_obs-ins_glo_phy-temp-sal_my_cora_irr",
                "dataset_name": "cmems_obs-ins_glo_phy-temp-sal_my_cora_irr_202311",
                "versions": [
                    {
                    "label": "202411",
                    "parts": [
                        {
                        "name": "default",
                        "services": [
                            {
                            "service_name": "original-files",
                            "service_short_name": "files",
                            "uri": "https://s3.waw3-1.cloudferro.com/mdl-native-03/native/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001/cmems_obs-ins_glo_phy-temp-sal_my_cora_irr_202411",
                            "variables": []
                            }
                        ],
                        "released_date": "2024-11-26T13:00:00.000Z"
                        }
                    ]
                    }
                ]
                }
            ]
            }
        ]
    }


``dataset_id`` and ``product_id`` options
-----------------------------------------

Calling the whole catalogue can be time-consuming. If you know the dataset or product you are looking for, you can use the ``dataset_id`` or ``product_id`` options.
It will drastically reduce the time to get the information you need.
You can either use the ``--dataset_id`` option, the ``--product_id`` option or both.

**Example:**

Let's filter to exclude services and return only datasets and product_id for simplicity.

.. code-block:: bash

    copernicusmarine describe -i cmems_mod_glo_phy_my_0.083deg_P1D-m -e services -r datasets,product_id

The output will be something like this:

.. code-block:: json

    {
        "products": [
            {
            "product_id": "GLOBAL_MULTIYEAR_PHY_001_030",
            "datasets": [
                {
                "dataset_id": "cmems_mod_glo_phy_my_0.083deg_P1D-m",
                "dataset_name": "daily mean fields from Global Ocean Physics Analysis and Forecast updated Daily",
                "versions": [
                    {
                    "label": "202311",
                    "parts": [
                        {
                        "name": "default",
                        "released_date": "2023-11-30T11:00:00.000Z"
                        }
                    ]
                    }
                ]
                }
            ]
            }
        ]
    }

``--show-all-versions`` option
-------------------------------

The ``describe`` command will show you only one version of a dataset, prioritising the most recent version and the ones that are not planned to be retired.
If you want to see all versions, you can use the ``--show-all-versions`` option.
It allows in some cases to access the metadata of datasets that are to be released or datasets that will be retired soon.

``arco_updating_start_date`` and ``arco_updated_date`` fields
---------------------------------------------------------------

These fields on the :class:`copernicusmarine.CopernicusMarinePart` can help to know if when the requested data has been updated and if it is still being updated.
It only concerns the ARCO services i.e. all services for the subsetting. It is not meant to indicate when the original data has been updated.

``arco_updated_date`` is the date when the ARCO data has been updated for the last time.
For example, if ``arco_updated_date=="2025-03-26T08:50:15.873Z"`` it means that the last update of the dataset was on the 26th of March 2025.

``arco_updating_start_date`` is the time point of the dataset from which the data is being updated.
For example, if ``arco_updating_start_date=="1990-05-16T08:50:15.873Z"`` it means that the dataset is being updated from the time point: 16th of May 1990.
See `the raise-if-updating <raise-if-updating>`_ option to be sure your requested data is up-to-date.

.. warning::

    ``arco_updating_start_date`` is a date that designate a value in the dataset contrary to ``arco_updated_date`` which is a "real life" date.
