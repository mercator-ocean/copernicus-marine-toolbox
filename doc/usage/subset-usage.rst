.. _subset-page:

===================
Command ``subset``
===================

The ``subset`` command allows you to remotely subset a dataset based on variable names, geographical parameters, and time ranges.

**Example:**

.. code-block:: bash

    copernicusmarine subset --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m --variable thetao --variable so --start-datetime 2021-01-01 --end-datetime 2021-01-03 --minimum-longitude 0.0 --maximum-longitude 0.1 --minimum-latitude 28.0 --maximum-latitude 28.1 --log-level DEBUG

**Returns:**

.. code-block:: bash

    DEBUG - 2024-04-03T10:18:18Z - <xarray.Dataset> Size: 3kB
    Dimensions:    (depth: 50, latitude: 2, longitude: 1, time: 3)
    Coordinates:
      * depth      (depth) float32 200B 0.5058 1.556 2.668 ... 5.292e+03 5.698e+03
      * latitude   (latitude) float32 8B 28.0 28.08
      * longitude  (longitude) float32 4B 0.08333
      * time       (time) datetime64[ns] 24B 2021-01-01 2021-01-02 2021-01-03
    Data variables:
        thetao     (time, depth, latitude, longitude) float32 1kB dask.array<chunksize=(3, 1, 2, 1), meta=np.ndarray>
        so         (time, depth, latitude, longitude) float32 1kB dask.array<chunksize=(3, 1, 2, 1), meta=np.ndarray>
    Attributes: (12/20)
        Conventions:               CF-1.0
        bulletin_date:             2020-12-01
        references:                http://marine.copernicus.eu
        copernicusmarine_version:  1.1.0
    INFO - 2024-04-03T10:18:18Z - Estimated size of the dataset file is 0.002 MB.
    Estimated size of the data that needs to be downloaded to obtain the result: 207 MB
    This a very rough estimation and usually its higher than the actual size of the data that needs to be downloaded.

Using log level DEBUG, a summary of the dataset subset is displayed. It is by default displayed when using the ``--dry-run`` option.

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

Hence, by default the Copernicus Marine Toolbox will try to optimise the chunk size and
will use a chunk size of 100 times the original chunk size. So approximately 100MB.
If the subset is small enough it won't even use ``dask`` at all.

In some cases, you might want to change this behaviour. For example, if you have a really large dataset
to download and you have great computing power you might want to increase the chunk size.
You can also not use ``dask`` at all by setting the chunk size to 0.
For now, it does not seem like there is a one-size-fits-all solution and you might have to experiment a bit.

.. note::

  The progress bar showed when using the ``subset`` command will be correlated to the chunk size used.
  The lower the chunk size, the more tasks you will see in the progress bar.

To sum up, the ``--chunk-size-limit`` option allows you to play with the chunk size of the process.
The bigger the chunk size, the bigger the individual process will be (in terms of memory usage) and the bigger the ressources needed.
If the chunk size is too small, many tasks are being created and handled by dask which means a consequent dask graph need to be handled.
The latter can lead to huge overhead and slow down the process.

Option ``--raise-if-updating``
""""""""""""""""""""""""""""""""""""""""""

.. note::
  This option only applies to ARCO services (``arco-geo-series`` and ``arco-time-series``) and not native files (``original-files`` service).

When a dataset is being updated, it can happen that data after a certain date becomes unreliable. When setting this flag,
the toolbox will raise an error if the subset requested interval overpasses the updating start date. By default, the flag is not set
and the toolbox will only emit a warning. See ``updating_start_date`` in class :class:`copernicusmarine.CopernicusMarinePart` and custom exception :class:`copernicusmarine.DatasetUpdating`.

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
