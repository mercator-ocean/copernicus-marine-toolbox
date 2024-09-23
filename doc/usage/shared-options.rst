Shared Options
=========================

Both ``subset`` and ``get`` (and also some concern other options) commands provide these options:

Option ``--overwrite-output-data``
**********************************

When specified, existing files will be overwritten. If not, and the files already exist at the destination, new files will be created with a unique index once the download is accepted (or if ``--force-download`` is provided).

Option ``--create-template``
*********************************

This option creates a file in your current directory containing the request parameters. If specified, no other action will be performed. The file created will depend on the command used:

- **For ``subset``:**

  **Example:**

  .. code-block:: bash

     copernicusmarine subset --create-template

  **Returns:**

  .. code-block:: text

     INFO - 2024-04-04T14:38:09Z - Template created at: subset_template.json

- **For ``get``:**

  **Example:**

  .. code-block:: bash

     copernicusmarine get --create-template

  **Returns:**

  .. code-block:: text

     INFO - 2024-04-04T14:38:09Z - Template created at: get_template.json

Option ``--request-file``
*********************************

This option allows you to specify request parameters in a provided ``.json`` file, which is useful for batch processing. You can use the following templates or create new ones with the ``--create-template`` option.

- **Template for ``subset`` data request:**

  .. code-block:: text

    {
        "dataset_id": "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
        "start_datetime": "2022-04-11",
        "end_datetime": "2023-08-11",
        "minimum_longitude": -182.79,
        "maximum_longitude": -179.69,
        "minimum_latitude": -40,
        "maximum_latitude": -36,
        "minimum_depth": 0,
        "maximum_depth": 0,
        "variables": ["thetao"],
        "output_directory": "./data/",
        "force_download": true
    }

  **Example:**

  .. code-block:: bash

     copernicusmarine subset --request-file template_subset_data_request.json

- **Template for ``get`` data request:**

  .. code-block:: text

    {
        "dataset_id": "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
        "filter": "*01yav_200[0-2]*",
        "force_download": false,
        "log_level": "INFO",
        "no_directories": false,
        "output_directory": "./data/",
        "overwrite_output_data": false,
        "show_outputnames": true
    }

  **Example:**

  .. code-block:: bash

     copernicusmarine get --request-file template_get_data_request.json

Option ``--credentials-file``
*********************************

You can use the ``--credentials-file`` option to specify a credentials file. The file can be either ``.copernicusmarine-credentials``, ``motuclient-python.ini``, ``.netrc``, or ``_netrc``.

Option ``--dataset-version``
*********************************

This option allows you to fetch a specific version of the dataset, which is particularly useful to maintain operational consistency when updates affect the chosen dataset.

Option ``--dataset-part``
*********************************

Use this option to fetch a specific part of the chosen dataset version.

Option ``--log-level``
*********************************

Set the level of detail printed to the console by the command, based on the standard logging library. Available values are: ``[DEBUG|INFO|WARN|ERROR|CRITICAL|QUIET]``.

All logs of the library are by default logged in stderr, except the output of the ``describe`` command and the output of the ``--show-outputnames`` option, which are sent to stdout.

.. note::
    For versions <=1.2.4, all logs are sent to stdout by default.
