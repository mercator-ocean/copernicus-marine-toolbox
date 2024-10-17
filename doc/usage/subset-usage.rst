Command ``subset``
===================

The ``subset`` command allows you to remotely subset a dataset based on variable names, geographical parameters, and time ranges.

**Example:**

.. code-block:: bash

    copernicusmarine subset --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m --variable thetao --variable so --start-datetime 2021-01-01 --end-datetime 2021-01-03 --minimum-longitude 0.0 --maximum-longitude 0.1 --minimum-latitude 28.0 --maximum-latitude 28.1

**Returns:**

.. code-block:: bash

    INFO - 2024-04-03T10:18:18Z - <xarray.Dataset> Size: 3kB
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

    Do you want to proceed with download? [Y/n]:

By default, a summary of the dataset subset is displayed, and a download confirmation is prompted. You can skip this confirmation by using the ``--force-download`` option.

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

About ``--netcdf-compression-level`` options
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If writing data to a NetCDF file (the default format), the ``--netcdf-compression-level`` option can be set to compress the downloaded file. This reduces file size but increases writing time. Without this option, the file is written faster but with a larger size. For Zarr format (`.zarr` extension), the default compression of the Copernicus Marine Data Store is applied, making the download fast and compressed without using ``--netcdf-compression-level``.

Default NetCDF compression settings for xarray:

.. code-block:: text

    {'zlib': True, 'complevel': 1, 'contiguous': False, 'shuffle': True}

Set the ``--netcdf-compression-level`` to a custom compression level between 0 (no compression, by default) and 9 (maximum compression).

About ``--netcdf3-compatible`` option
""""""""""""""""""""""""""""""""""""""""

The ``--netcdf3-compatible`` option enables compatibility with the netCDF3 format.
This uses the ``format="NETCDF3_CLASSIC"`` setting in the xarray `to_netcdf` method. (cf. `xarray documentation <https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_netcdf.html>`_.)

About ``--coordinates-selection-method`` option
""""""""""""""""""""""""""""""""""""""""""""""""""

The ``--coordinates-selection-method`` option lets you specify how the requested interval selects data points:

- **inside** (default): Returns points strictly within the requested area.
- **strict-inside**: Returns points strictly within the requested area. Fails with an error if the requested area is outside the dataset.
- **nearest**: Returns points closest to the requested interval.
- **outside**: Returns all points covering the requested area.

This applies to all dimensions: longitude, latitude, time, and depth.

**Example of longitude requests:**
Imagine a dataset with longitudes from 5.0 to 36.5, with steps of 0.5.

- ``--coordinates-selection-method``= **inside**, with requested interval = [0.4, 35.9]:
  - Returns longitudes within the range: [0.5, 35.5]

- ``--coordinates-selection-method``= **strict-inside**, with requested interval = [0.4, 35.9]:
  - Returns longitudes within the range: [0.5, 35.5]

- ``--coordinates-selection-method``= **strict-inside**, with requested interval = [0.0, 40]:
  - Returns an error

- ``--coordinates-selection-method``= **outside**, with requested interval = [0.4, 35.9]:
  - Returns longitudes within the range: [0.0, 36.0]

- ``--coordinates-selection-method``= **nearest**, with requested interval = [0.4, 35.9]:
  - Returns longitudes within the range: [0.5, 36.0]

If you request a single point, the nearest point in that dimension will be returned.
