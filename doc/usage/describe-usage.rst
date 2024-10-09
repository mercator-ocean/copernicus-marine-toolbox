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


By default, the command only shows the products. To include the datasets, you can use the ``--include-datasets`` option.

**Example:**

.. code-block:: bash

    copernicusmarine describe --include-datasets

To save the JSON output to a file, you can use the following command:

.. code-block:: bash

    copernicusmarine describe --include-datasets > all_datasets_copernicusmarine.json

``--contains`` option
----------------------

You also have the option to filter the output by using the ``--contains`` option. It will perform a search on all the text fields of the output.

**Example:**

If you want, for example, the ``cmems_obs-ins_glo_phy-temp-sal_my_cora_irr`` dataset only, you can use the following command:

.. code-block:: bash

    copernicusmarine describe --include-datasets --contains cmems_obs-ins_glo_phy-temp-sal_my_cora_irr

The output will be something like this:

.. code-block:: json

    {
    "products": [
        {
        "title": "Global Ocean- CORA- In-situ Observations Yearly Delivery in Delayed Mode",
        "product_id": "INSITU_GLO_PHY_TS_DISCRETE_MY_013_001",
        "thumbnail_url": "https://mdl-metadata.s3.waw3-1.cloudferro.com/metadata/thumbnails/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001.jpg",
        "digital_object_identifier": "10.17882/46219",
        "sources": [
            "In-situ observations"
        ],
        "processing_level": "Level 2",
        "production_center": "OceanScope (France)",
        "datasets": [
            {
            "dataset_id": "cmems_obs-ins_glo_phy-temp-sal_my_cora_irr",
            "dataset_name": "cmems_obs-ins_glo_phy-temp-sal_my_cora_irr_202311",
            "versions": [
                {
                "label": "202311",
                "parts": [
                    {
                    "name": "default",
                    "services": [
                        {
                        "service_type": {
                            "service_name": "original-files",
                            "short_name": "files"
                        },
                        "service_format": null,
                        "uri": "https://s3.waw3-1.cloudferro.com/mdl-native-03/native/INSITU_GLO_PHY_TS_DISCRETE_MY_013_001/cmems_obs-ins_glo_phy-temp-sal_my_cora_irr_202311",
                        "variables": []
                        }
                    ],
                    "retired_date": null,
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

``--include-versions`` option
-----------------------------

The describe command will show you only one version of a dataset, prioritising the most recent version and the ones that are not planned to be retired.
If you want to see all versions, you can use the ``--include-versions`` option.
It allows in some cases to access the metadata of datasets that are to be released or datasets that will be retired soon.
