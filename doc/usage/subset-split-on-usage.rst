.. _subset-split:

===========================
Command ``subset-split-on``
===========================

The split-on functionality for the subset allows users to divide the data in several files.
This can be particularly useful when dealing with large datasets or
when the hardware resources are limited.

.. note::

   This is a command of the ``subset`` module that you can use like the following:

    .. code-block:: bash

        copernicusmarine subset [SUBSET-OPTIONS] split-on [OPTIONS]

    Set all the options for the ``subset`` **before** the ``split-on`` command.
    For more information about the ``subset`` module, please refer to :ref:`subset-page`.

.. warning::

    The ``split-on`` functionality is not supported for sparse datasets i.e. datasets accessed via the sqlite ARCO format.

The ``split-on`` command or ``subset_split_on`` function allows you to split
the subsetted dataset into multiple files based on a specific dimension.
This option enables parallel downloading or can reduce the load on the system.
This can be useful for managing large datasets and improving performance.

The following split options are available:

* On variables: creates one file per variable. The selected variables are the intersection of the dataset variables and the requested variables.
* On time dimension: creates one file per time unit in the selected time range. Available frequencies are: ``year``, ``month``, ``day``, ``hour``.

.. code-block:: python

  response = copernicusmarine.subset_split_on(
      dataset_id="cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
      start_datetime="2021-01-01",
      end_datetime="2025-01-03",
      on_time="year",
      minimum_latitude=19,
      maximum_latitude=20,
      minimum_longitude=19,
      maximum_longitude=20,
      minimum_depth=0,
      maximum_depth=1,
  )
  # this will create one file per year in the selected time range, here: dataset_id_2021-01-01-2021-12-31.nc, dataset_id_2022-01-01-2022-12-31.nc, dataset_id_2023-01-01-2023-12-31.nc

.. code-block:: bash

  copernicusmarine subset --dataset-id cmems_mod_glo_phy-all_my_0.25deg_P1D-m -x -9 -X -7 -y 34 -Y 38 -z 0.5 -Z 2 -t 2022-01-01 -T 2023-05-01 split-on --on-variables
  # this will create one file per variable in the selected variables, here 24, named as if the --on-variables option was used for each variable
  # e.g. cmems_mod_glo_phy-all_my_0.25deg_P1D-m_mlotst_cglo_9.00W-7.00W_34.00N-38.00N_2022-01-01-2023-05-01.nc, cmems_mod_glo_phy-all_my_0.25deg_P1D-m_thetao_cglo_9.00W-7.00W_34.00N-38.00N_0.51-1.56m_2022-01-01-2023-05-01.nc

To use parallel downloading when splitting on several files, you can use the ``concurrent-processes`` option to specify the number of concurrent processes to run simultaneously.
The Toolbox uses Python's ``concurrent.futures.ProcessPoolExecutor`` to manage parallel execution.

.. code-block:: python

  response = copernicusmarine.subset_split_on(
      dataset_id="cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
      start_datetime="2021-01-01",
      end_datetime="2025-01-03",
      on_time="year",
      concurrent_processes=4,
      maximum_latitude=20,
      minimum_latitude=19,
      maximum_longitude=20,
      minimum_longitude=19,
      maximum_depth=1,
      minimum_depth=0,
  )
  # this will create one file per year in the selected time range, here: dataset_id_2021-01-01-2021-12-31.nc, dataset_id_2022-01-01-2022-12-31.nc, dataset_id_2023-01-01-2023-12-31.nc
  # up to 4 files will be downloaded in parallel
