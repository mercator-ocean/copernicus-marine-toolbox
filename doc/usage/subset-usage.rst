.. _subset-page:

===================
Command ``subset``
===================

The ``subset`` command allows you to remotely subset a dataset based on variable names, geographical parameters, and time ranges.

**Example:**

.. code-block:: bash

    copernicusmarine subset --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m --variable thetao --variable so --start-datetime 2021-01-01 --end-datetime 2021-01-03 --minimum-longitude 0.0 --maximum-longitude 0.1 --minimum-latitude 28.0 --maximum-latitude 28.1

**Returns:**

.. code-block:: bash

  INFO - 2025-07-10T13:18:03Z - Selected dataset version: "202012"
  INFO - 2025-07-10T13:18:03Z - Selected dataset part: "default"
  INFO - 2025-07-10T13:18:05Z - Starting download. Please wait...
  100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 44/44 [00:10<00:00,  4.03it/s]
  INFO - 2025-07-10T13:18:16Z - Successfully downloaded to cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m_thetao-so_0.08E_28.00N-28.08N_0.51-5698.06m_2021-01-01-2021-01-03.nc
  {
    "file_size": 0.01414503816793893,
    "data_transfer_size": 422.5758778625954,
    "status": "000",
    "message": "The request was successful."
  }

Using log level DEBUG, a summary of the dataset subset is displayed. It is by default displayed when using the ``--dry-run`` option.

.. _sparse-subset:

Sparse data subsetting
-----------------------

On the one hand, some of the datasets available on Copernicus Marine are gridded datasets, benefiting from all the features of the Copernicus Marine Toolbox.
On the other hand, certain datasets are time series for a given platform; these are called sparse or in-situ datasets. These datasets are processed and formatted differently within the ARCO data framework. See the `in-situ datasets <https://data.marine.copernicus.eu/products?facets=sources%7EIn-situ+observations>`_ for example.

We can download all the time series of a given geographical area and time period via the ``subset``. Options can also be used to choose the platforms, variables or depth ranges we are interested in. It will return the data in a tabular format, such as a Pandas DataFrame, a CSV file, or a Parquet database.

**Example:**

.. code-block:: bash

  copernicusmarine subset -i cmems_obs-ins_arc_phybgcwav_mynrt_na_irr -y 45 -Y 90 -x -146.99 -X 180 -z 0 -Z 10 --start-datetime "2023-11-25T00:00:00" -T "2024-11-26T03:00:00" --dataset-part history --platform-id B-Sulafjorden___MO --platform-id F-Vartdalsfjorden___MO

This dataset can be opened with pandas:

.. code-block:: python

  import pandas as pd

  df = pd.read_csv(
      "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_multi-vars_B-Sulafjorden___MO-F-Vartdalsfjorden___MO_146.99W-180.00E_45.00N-90.00N_0.00-10.00m_2023-11-25-2024-11-26.csv"
  )

It is also possible to load the Pandas DataFrame directly using the :func:`~copernicusmarine.read_dataframe` function:

.. code-block:: python

  import copernicusmarine

  df = copernicusmarine.read_dataframe(
      dataset_id="cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
      minimum_latitude=45,
      maximum_latitude=90,
      minimum_longitude=-146.99,
      maximum_longitude=180,
      minimum_depth=0,
      maximum_depth=10,
      start_datetime="2023-11-25T00:00:00",
      end_datetime="2024-11-26T03:00:00",
      dataset_part="history",
      platform_ids=["B-Sulafjorden", "F-Vartdalsfjorden"],
  )

The output will contain the following columns:

- ``variable``: The variable name.
- ``platform_id``: The platform ID.
- ``platform_type``: The platform type.
- ``time``: The timestamp of the measurement in seconds.
- ``longitude``: The longitude of the measurement in degrees.
- ``latitude``: The latitude of the measurement in degrees.
- ``depth`` or ``elevation``: The depth of the measurement in meters, or 'elevation' if selected with the ``vertical-axis`` option.
- ``pressure``: The measurement pressure in decibars (not always available).
- ``is_depth_from_producer``: Indicates whether the pressure value was used to calculate elevation/depth when converting the data to a format that can be subsetted. The conversion used is ``pressure in decibar = depth in m``.
- ``value``: The measurement value.
- ``value_qc``: The quality control indicator of the value.
- ``institution``: The institution that produced the data and is affiliated with the platform.
- ``doi``: The DOI of the data, based on the originating institution.
- ``product_doi``: Product's DOI in the Copernicus Marine Catalog.

These datasets have specific options and outputs:

- The ``--file-format`` option can be used to specify 'parquet' or 'csv'. The default format is 'csv'.
- The ``--platform-id`` option enables filtering data by platform ID. Note, that you can also add the type of platform by adding "___" (e.g., ``--platform-id B-Sulafjorden___MO`` will select platform ID "B-Sulafjorden" and type "MO" for this platform). Otherwise, all the platform types available will be selected.

There are also some options that behave differently or are not available for sparse datasets:

- The 'netcdf' and 'zarr' formats are not available for sparse datasets.
- Manually forcing the use of a specific service is not possible; the toolbox will automatically select the preferred service.
- The :class:`copernicusmarine.ResponseSubset` object does not include coordinate extents, file size, or data transfer size information.
- For the :ref:`coordinate-selection-method <coordinates-selection-method>` option, only the 'inside' and 'strict-inside' values are relevant.
- The default naming convention for output files differs slightly. For sparse datasets, the file name will reflect the requested extents rather than the actual extents of the resulting subset.

Additional options
------------------

About longitude range
""""""""""""""""""""""

The ``--minimum-longitude`` and ``--maximum-longitude`` options work as follows:

- If the result of ``--maximum-longitude`` minus ``--minimum-longitude`` is greater than or equal to 360, the entire dataset will be returned.
- If the requested longitude range:

  * **Does not cross** the antemeridian, the dataset between -180 and 180 is returned.
  * **Crosses** the antemeridian, the dataset between 0 and 360 is returned.

Note that any longitudes can be requested. The system applies a modulus operation to bring the result between -180° and 360°. For example, a request for [530, 560] will return data for longitudes [170, 200].

Option ``--netcdf-compression-level``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If writing data to a NetCDF file (the default format), the ``--netcdf-compression-level`` option can be set to compress the downloaded file. This reduces file size but increases writing time. Without this option, the file is written faster but with a larger size. For Zarr format ('.zarr' extension), the default compression of the Copernicus Marine Data Store is applied, making the download fast and compressed without using ``--netcdf-compression-level``.

Default NetCDF compression settings for the Toolbox are:

.. code-block:: text

    {'zlib': True, 'complevel': 1, 'contiguous': False, 'shuffle': True}

Set the ``--netcdf-compression-level`` to a custom compression level between 0 (no compression, by default) and 9 (maximum compression).

Option ``--netcdf3-compatible``
""""""""""""""""""""""""""""""""""""""""

The ``--netcdf3-compatible`` option enables compatibility with the netCDF3 format.
This uses the ``format="NETCDF3_CLASSIC"`` setting in the xarray `to_netcdf` method. (cf. `xarray documentation <https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_netcdf.html>`_.)

.. _coordinates-selection-method:

Option ``--coordinates-selection-method``
""""""""""""""""""""""""""""""""""""""""""""""""""

The ``--coordinates-selection-method`` option lets you specify how the requested interval selects data points:

- **inside** (default): Returns points strictly within the requested area.
- **strict-inside**: Returns points strictly within the requested area. Fails with an error if the requested area is outside the dataset.
- **nearest**: Returns points closest to the requested interval.
- **outside**: Returns all points covering the requested area.

This applies to all dimensions: longitude, latitude, time, and depth.

**Example of longitude requests:**
Imagine a dataset with longitudes from 5.0 to 36.5, with steps of 0.5.

- ``--coordinates-selection-method`` = **inside**, with requested interval = [0.4, 35.9]:

  - Returns longitudes within the range: [0.5, 35.5]

- ``--coordinates-selection-method`` = **strict-inside**, with requested interval = [0.4, 35.9]:

  - Returns longitudes within the range: [0.5, 35.5]

- ``--coordinates-selection-method`` = **strict-inside**, with requested interval = [0.0, 40]:

  - Returns an error, see :class:`copernicusmarine.CoordinatesOutOfDatasetBounds`.

- ``--coordinates-selection-method`` = **outside**, with requested interval = [0.4, 35.9]:

  - Returns longitudes within the range: [0.0, 36.0]

- ``--coordinates-selection-method`` = **nearest**, with requested interval = [0.4, 35.9]:

  - Returns longitudes within the range: [0.5, 36.0]

If you request a single point, the nearest point in that dimension will be returned.

.. _chunk-size-limit:

Option ``--chunk-size-limit``
""""""""""""""""""""""""""""""""""""""""""

.. warning::
  This option might help for some very specific cases but it is still experimental and might not work as expected in all cases.

The Copernicus Marine Toolbox uses ``xarray`` to open and subset datasets.
In the backend, it uses ``dask`` to handle large datasets.
Those are powerful tools for handling large datasets and will work directly in most cases.
You can read more about it on the `xarray documentation page <https://docs.xarray.dev/en/stable/user-guide/dask.html>`_.

However, in some cases the default chunk size might not be optimal for your use case. Indeed, by default,
the Copernicus Marine ARCO datasets are organised in chunks of around 1MB.
This might create a lot of overhead if you are working with a lot of small chunks and ``dask``.
Please see the `dask documentation <https://docs.dask.org/en/stable/best-practices.html#avoid-very-large-graphs>`_ for the details.

The default is ``-1`` and when set, it tries to infere the optimum chunk size limit to use based on the size of the download.

In some cases, you might want to change this behaviour. For example, if you have a really large dataset
to download and you have great computing power you might want to increase the chunk size.

If you prefer not using dask, for small datasets you can set the chunk size to 0.

For now, it does not seem like there is a one-size-fits-all solution and you might have to experiment a bit.

.. note::

  The progress bar showed when using the ``subset`` command will be correlated to the chunk size used.
  The lower the chunk size, the more tasks you will see in the progress bar.

To sum up, the ``--chunk-size-limit`` option allows you to play with the chunk size of the process.
The bigger the chunk size, the bigger the individual process will be (in terms of memory usage) and the bigger the ressources needed.
If the chunk size is too small, many tasks are being created and handled by dask which means a consequent dask graph need to be handled.
The latter can lead to huge overhead and slow down the process.

.. _raise-if-updating:

Option ``--raise-if-updating``
""""""""""""""""""""""""""""""""""""""""""

.. note::
  This option only applies to ARCO services (``arco-geo-series`` and ``arco-time-series``) and not native files (``original-files`` service).

When a dataset is being updated, data after a certain date may become unreliable. If this flag is set, the toolbox will raise an error if the requested subset interval extends beyond the updating start date.
 By default, the flag is not set and the toolbox will only emit a warning. See ``arco_updating_start_date`` in class :class:`copernicusmarine.CopernicusMarinePart` and custom exception :class:`copernicusmarine.DatasetUpdating`.

.. code-block:: python

  try:
      dataset = copernicusmarine.subset(
          dataset_id=dataset_id,
          start_datetime="2021-01-01",
          end_datetime="2025-01-03",
          raise_if_updating=True,
      )
  except copernicusmarine.DatasetUpdating as e:
      # add retries here if needed
      logging.error(e)

.. _stereographic-subset-usage:

Options for Arco with Original-grid
""""""""""""""""""""""""""""""""""""""""""

For ARCO services in Original-grid part datasets, the following options are available to bound the subsetted area:

  - ``--minimum-x`` or ``-x`` : The minimum x-axis coordinate.
  - ``--maximum-x``or ``-X`` : The maximum x-axis coordinate.
  - ``--minimum-y`` or ``-y`` : The minimum y-axis coordinate.
  - ``--maximum-y`` or ``-Y`` : The maximum y-axis coordinate.

For more context and examples, check the  :ref:`Original-grid page <stereographic-subsetting-page>`.

.. note:

  When using these options, the dataset part should be set to originalGrid: ``--dataset-part originalGrid``.
