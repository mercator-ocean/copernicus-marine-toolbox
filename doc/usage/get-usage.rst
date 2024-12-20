.. _get-page:

===============
Command ``get``
===============

Download the dataset file(s) as originally produced, based on the dataset ID or the path to files.

**Example:**

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --log-level DEBUG

**Returns:**

.. code-block:: bash

    INFO - 2024-04-03T11:39:18Z - Dataset version was not specified, the latest one was selected: "202211"
    INFO - 2024-04-03T11:39:18Z - Dataset part was not specified, the first one was selected: "default"
    INFO - 2024-04-03T11:39:18Z - Service was not specified, the default one was selected: "original-files"
    INFO - 2024-04-03T11:39:18Z - Downloading using service original-files...
    DEBUG - 2024-04-03T11:39:19Z - You requested the download of the following files:
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_19930101_19931231_R20221101_RE01.nc - 8.83 MB - 2023-11-12T23:47:13Z
    [...]
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20120101_20121231_R20221101_RE01.nc - 8.62 MB - 2023-11-12T23:47:14Z
    Printed 20 out of 29 files

    Total size of the download: 252.94 MB

**By default:**

- Files are downloaded to the current directory while maintaining the original folder structure. To avoid this behavior, add ``--no-directories`` and specify a destination with ``-o/--output-directory``.
- Option ``--create-file-list`` creates a file containing the names of the targeted files instead of downloading them. You must input a file name, e.g., ``--create-file-list my_files.txt``. The format must be ``.txt`` or ``.csv``.
- This info can be accessed also using the ``--dry-run`` option without downloading anything.

If the user inputs a filename that ends in ``.txt``, it will contain only the full S3 path to the targeted files, compatible with the ``--file-list`` option.

**Example:**

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m --filter "*2021*" --create-file-list selected_files_for_2021.txt

The content of ``selected_files_for_2021.txt`` would be:

.. code-block:: text

    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210101_20210131_R20230101_RE01.nc
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210201_20210228_R20230101_RE01.nc
    [...]

If the user inputs a filename that ends in ``.csv``, it will contain columns separated by a comma: ``filename``, ``size`` (in Bytes), ``last_modified_datetime``, and ``etag``. It is **not** directly compatible with the ``--file-list`` option and would require post-processing.

**Example:**

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m --filter "*2021*" --create-file-list selected_files_for_2021.csv

The content of ``selected_files_for_2021.csv`` would be:

.. code-block:: text

    filename,size,last_modified_datetime,etag
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210101_20210131_R20230101_RE01.nc,12295906,2023-11-12 23:47:05.466000+00:00,"e8a7e564f676a08bf601bcdeaebdc563"
    [...]

About ``--sync`` option
------------------------

The ``--sync`` option downloads original files only if they do not exist or are not up to date. The toolbox checks the destination folder against the source folder and can be combined with filters. If set with ``--overwrite``, the latter will be ignored. The ``--sync-delete`` option works like ``--sync`` but also deletes any local files not found on the remote server.

**Limitations:**

- ``--sync`` is not compatible with ``--no-directories``.
- ``--sync`` only works with ``--dataset-version``. (see :ref:`dataset-version <dataset version>` option )

About filtering options
------------------------

The ``--filter`` option allows specifying a Unix shell-style wildcard pattern to select specific files.

**Example** To download only files that contains "2000", "2001", or "2002":

.. code-block:: bash

    copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --filter "*01yav_200[0-2]*"

Option ``--regex`` allows specifying a regular expression for more advanced file selection.

**Example** To download only files that contains "2000", "2001", or "2002" using a regular expression:

.. code-block:: bash

    copernicusmarine get -i cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --regex ".*01yav_20(00|01|02).*.nc"

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

    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210101_20210131_R20230101_RE01.nc
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210201_20210228_R20230101_RE01.nc
    s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210301_20210331_R20230101_RE01.nc

Note that a path to a file can include wildcards or regular expressions.
