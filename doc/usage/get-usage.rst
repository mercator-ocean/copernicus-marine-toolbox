.. _get-page:

===============
Command ``get``
===============

Download the dataset file(s) as originally produced, based on the dataset ID or the path to files.

**Example:**

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m

**Returns:**

.. code-block:: bash

    INFO - 2025-11-27T11:20:55Z - Selected dataset version: "202511"
    INFO - 2025-11-27T11:20:55Z - Selected dataset part: "default"
    INFO - 2025-11-27T11:20:55Z - Listing files on remote server...
    1it [00:00, 10.68it/s]
    Downloading files: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 30/30 [00:32<00:00,  1.08s/it]
    {
    "number_of_files_to_download": 30,
    "total_size": 1354.8411445617676,
    "status": "000",
    "message": "The request was successful."
    }

**By default:**

- Files are downloaded to the current directory while maintaining the original folder structure. To avoid this behavior, add ``--no-directories`` and specify a destination with ``-o/--output-directory``.
- Option ``--create-file-list`` creates a file containing the names of the targeted files instead of downloading them. You must input a file name, e.g., ``--create-file-list my_files.txt``. The format must be ``.txt`` or ``.csv``.
- This info can be accessed also using the ``--dry-run`` option without downloading anything.

If the user inputs a filename that ends in ``.txt``, it will contain only the full S3 path to the targeted files, compatible with the ``--file-list`` option.

**Example:**

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m --filter "*2021*" --create-file-list selected_files_for_2021.txt

The content of ``selected_files_for_2021.txt`` would be:

.. code-block:: text

    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m_202511/2021/CMEMS_v6r1_IBI_PHY_MY_NL_01mav_temp_20210101_20210131_R20251125_RE01.nc
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m_202511/2021/CMEMS_v6r1_IBI_PHY_MY_NL_01mav_temp_20210201_20210228_R20251125_RE01.nc
    [...]

If the user inputs a filename that ends in ``.csv``, it will contain columns separated by a comma: ``filename``, ``size`` (in Bytes), ``last_modified_datetime``, and ``etag``. It is **not** directly compatible with the ``--file-list`` option and would require post-processing.

**Example:**

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m --filter "*2021*" --create-file-list selected_files_for_2021.csv

The content of ``selected_files_for_2021.csv`` would be:

.. code-block:: text

    filename,size,last_modified_datetime,etag
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m_202511/2021/CMEMS_v6r1_IBI_PHY_MY_NL_01mav_temp_20210101_20210131_R20251125_RE01.nc,49558406.0,2025-10-02T12:01:01.149000+00:00,"f04e74284d48a11234bf25231fbdee15-6"
    [...]

About ``--sync`` option
------------------------

The ``--sync`` option downloads original files only if they do not exist or are not up to date. The toolbox checks the destination folder against the source folder and can be combined with filters. If set with ``--overwrite``, the latter will be ignored. The ``--sync-delete`` option works like ``--sync`` but also deletes any local files not found on the remote server.

**Limitations:**

- ``--sync`` only works with ``--dataset-version``. (see :ref:`dataset-version <dataset version>` option )

About filtering options
------------------------

The ``--filter`` option allows specifying a Unix shell-style wildcard pattern to select specific files.

**Example** To download only files that contains "2000", "2001", or "2002":

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m --filter "*01yav_temp_200[0-2]*"

Option ``--regex`` allows specifying a regular expression for more advanced file selection.

**Example** To download only files that contains "2000", "2001", or "2002" using a regular expression:

.. code-block:: bash

    copernicusmarine get -i cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m --regex ".*01yav_temp_20(00|01|02).*.nc"

About ``--file-list`` option
-----------------------------

The ``--file-list`` option allows specifying a list of files for advanced selection. The file can contain complete absolute paths or only a partial path defined by the user.

By default, the ``get`` functionality lists all files on the remote server to select requested ones. The file list option will directly download files and avoid listings if all listed files are found.

**Example** of ``file_list.txt`` with paths that would be directly downloaded:

.. code-block:: text

    # correct paths
    > s3://mdl-native-01/native/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/history/BO/AR_PR_BO_58JM.nc
    > INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/history/BO/AR_PR_BO_58JM.nc
    > cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/history/BO/AR_PR_BO_58JM.nc
    > history/BO/AR_PR_BO_58JM.nc
    > index_history.txt
    # incorrect paths
    # version is missing
    > INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr/history/BO/AR_PR_BO_58JM.nc
    # only the file name and not the path to the file
    > AR_PR_BO_58JM.nc
    # not the same dataset
    > another_dataset/history/BO/AR_PR_BO_58JM.nc


**Example** of ``file_list.txt`` with absolute paths:

.. code-block:: text

    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m_202511/2021/CMEMS_v6r1_IBI_PHY_MY_NL_01mav_temp_20210101_20210131_R20251125_RE01.nc
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m_202511/2021/CMEMS_v6r1_IBI_PHY_MY_NL_01mav_temp_20210201_20210228_R20251125_RE01.nc
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m_202511/2021/CMEMS_v6r1_IBI_PHY_MY_NL_01mav_temp_20210301_20210331_R20251125_RE01.nc

Note that a path to a file can include wildcards or regular expressions.
