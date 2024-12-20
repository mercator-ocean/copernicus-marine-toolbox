Shared options
=========================

Both ``subset`` and ``get`` commands provide these options. Some options are available in other functions as well, in the latter it will be indicated:

Option ``--overwrite`` and ``--skip-existing``
************************************************

By default, if the files already exist at the destination, new files will be created with a unique index (eg 'filename_(1).nc').

When ``--overwrite`` is specified, existing files will be overwritten.

When ``--skip-existing`` is specified, the download of files that already exist at the output destination will be skipped.

The ``status`` and ``message`` in the response can indicate if the toolbox has overwritten or skipped a file.
See in :ref:`Response types documentation <response-types>` the ``status`` and ``message`` fields for more information about request statuses.

.. note::
    The two options are mutually exclusive i.e. they cannot be used together.

Option ``--create-template``
*********************************

This option creates a file in your current directory containing the request parameters.
If specified, no other action will be performed. The file created will depend on the command used:

- ``subset``

  **Example:**

  .. code-block:: bash

     copernicusmarine subset --create-template

  **Returns:**

  .. code-block:: text

     INFO - 2024-04-04T14:38:09Z - Template created at: subset_template.json

- ``get``

  **Example:**

  .. code-block:: bash

     copernicusmarine get --create-template

  **Returns:**

  .. code-block:: text

     INFO - 2024-04-04T14:38:09Z - Template created at: get_template.json

Option ``--request-file``
*********************************

This option allows you to specify request parameters in a provided JSON file, which is useful for batch processing. You can use the following templates or create new ones with the ``--create-template`` option.

- Template for ``subset`` data request:

  .. code-block:: text

    {
        "dataset_id": "cmems_mod_glo_phy_myint_0.083deg_P1M-m",
        "start_datetime": "2023-10-07",
        "end_datetime": "2023-10-12",
        "minimum_longitude": -85,
        "maximum_longitude": -10,
        "minimum_latitude": 35,
        "maximum_latitude": 43,
        "minimum_depth": 1,
        "maximum_depth": 10,
        "variables": [
            "so",
            "thetao"
        ],
        "output_directory": "copernicusmarine_data",
        "force_service": false,
        "request_file": false,
        "motu_api_request": false,
        "overwrite": false
    }

  **Example:**

  .. code-block:: bash

     copernicusmarine subset --request-file subset_template.json

- Template for ``get`` data request:

  .. code-block:: text

    {
        "dataset_id": "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
        "dataset_version": null,
        "dataset_part": null,
        "username": null,
        "password": null,
        "no_directories": false,
        "filter": "*01yav_200[0-2]*",
        "regex": null,
        "output_directory": "copernicusmarine_data",
        "file_list": null,
        "sync": false,
        "sync_delete": false,
        "index_parts": false,
        "disable_progress_bar": false,
        "overwrite": false,
        "log_level": "INFO"
    }

  **Example:**

  .. code-block:: bash

     copernicusmarine get --request-file get_template.json

Option ``--credentials-file``
*********************************

You can use the ``--credentials-file`` option to specify a credentials file. The file can be either ``.copernicusmarine-credentials``, ``motuclient-python.ini``, ``.netrc``, or ``_netrc``.

When using the option ``--check-credentials-valid`` with the ``login`` command, the ``--credentials-file`` option can be used the same way as with the ``subset`` and ``get`` commands.

.. _dataset version:

Option ``--dataset-version``
*********************************

This option allows you to fetch a specific version of the dataset, which is particularly useful to maintain operational consistency when updates affect the chosen dataset.
The version of dataset can be found through the ``describe`` command.

**Example:**

.. code:: bash

    copernicusmarine get -i cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m --dataset-version 202012

**Returns:**

.. code:: text

    INFO - 2024-10-07T08:53:18Z - You forced selection of dataset version "202012"

Option ``--dataset-part``
*********************************

Use this option to fetch a specific part of the chosen dataset version.
The parts of the dataset can be found through the ``describe`` command.

**Example:**

.. code:: bash

    copernicusmarine get -i cmems_obs-ins_blk_phybgcwav_mynrt_na_irr --dataset-part history

**Returns:**

.. code:: text

    INFO - 2024-10-07T08:53:18Z - You forced selection of dataset part "history"


Option ``--response-fields`` or ``-r``
***********************************************

This option allows to choose the fields that will be included in the metadata of the query. The fields are separated by commas.
It is useful to reduce the size of the amount of information in the response.
If requested ``-r all``, all fields will be included. If requested ``-r none``, no fields will be included.

.. note::
    There are several behavior depending on the options used:

    - If ``--response-fields`` is used then the requested fieds will be returned in the response.
    - If ``--dry-run`` is used, all the fields will be returned in the response.
    - By default, the response will include only the status code and status message of the request.

**Example:**

.. code:: bash

    copernicusmarine get -i cmems_mod_nws_bgc-pft_myint_7km-3D-diato_P1M-m --dry-run -r https_url,file_size

**Returns:** (Only the two first files are shown)

.. code:: bash

    {
    "files": [
      {
        "https_url": "https://s3.waw3-1.cloudferro.com/mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-pft_myint_7km-3D-diato_P1M-m_202105/2022/metoffice_foam1_amm7_NWS_DIATO_CPWC_mm202207.nc",
        "file_size": 3.1228113174438477
      },
      {
        "https_url": "https://s3.waw3-1.cloudferro.com/mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-pft_myint_7km-3D-diato_P1M-m_202105/2022/metoffice_foam1_amm7_NWS_DIATO_CPWC_mm202208.nc",
        "file_size": 3.133638381958008
      },
      ]
    }

.. note::
  This option is only relevant for the command line interface.

.. _dry-run:

Option ``--dry-run``
*********************************

This option allows you to simulate the command without actually executing it.
It is useful for checking the command's behavior before running it and to be able to access some metadata about the request.
When used, the toolbox will by default, send the full return response of the command.

**Example:**

.. code:: bash

      copernicusmarine subset -i cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m -v thetao --dry-run

**Returns:**

.. code:: text

    INFO - 2024-10-07T08:47:46Z - Dataset version was not specified, the latest one was selected: "202012"
    INFO - 2024-10-07T08:47:46Z - Dataset part was not specified, the first one was selected: "default"
    INFO - 2024-10-07T08:47:47Z - Service was not specified, the default one was selected: "arco-geo-series"
    INFO - 2024-10-07T08:47:48Z - Downloading using service arco-geo-series...
    INFO - 2024-10-07T08:47:50Z - Estimated size of the dataset file is 210828.202 MB
    Estimated size of the data that needs to be downloaded to obtain the result: 210888 MB
    This a very rough estimation and usually its higher than the actual size of the data that needs to be downloaded.
    {
      "output": "cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m_thetao_19.00W-5.00E_26.00N-56.00N_0.51-5698.06m_1993-01-01-2021-12-28.nc",
      "size": 210828.20248091602,
      "data_needed": 210887.9328244275,
      "coodinates_extent": {
        "longitude": {
          "minimum": -19.0,
          "maximum": 4.999999046325684
        },
        "latitude": {
          "minimum": 26.0,
          "maximum": 56.0
        },
        "time": {
          "minimum": "1993-01-01T00:00:00Z",
          "maximum": "2021-12-28T00:00:00Z"
        },
        "depth": {
          "minimum": 0.5057600140571594,
          "maximum": 5698.060546875
        }
      }
    }

See :ref:`Response types documentation <response-types>` for more information about the response you can expect.


Option ``--log-level``
*********************************

Set the level of detail printed to the console by the command, based on the standard logging library. Available values are: ``[DEBUG|INFO|WARN|ERROR|CRITICAL|QUIET]``.

Also available for the ``login`` and the ``describe`` function.

All logs of the library are by default logged in stderr. The outputs of the commands which details can be found in :ref:`the response types documentation <response-types>`, are sent to stdout.

.. note::
    For versions <=1.2.4, all logs are sent to stdout by default.
