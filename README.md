
<h1 align="center">Copernicus Marine Service Toolbox (CLI & Python)</h1>
<div align="center">
  <a href="https://pypi.org/project/copernicusmarine/"><img src="https://img.shields.io/pypi/v/copernicusmarine.svg?style=flat-square" alt="PyPI" /></a>
  <a href="https://pypi.org/project/copernicusmarine/"><img src="https://img.shields.io/pypi/pyversions/copernicusmarine.svg?style=flat-square" alt="PyPI Supported Versions" /></a>
  <a href="https://pypi.org/project/copernicusmarine/"><img src="https://img.shields.io/badge/platform-windows | linux | macos-lightgrey?style=flat-square" alt="Supported Platforms" /></a>
  <a href="https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12"><img src="https://img.shields.io/badge/licence-EUPL-lightblue?style=flat-square" alt="Licence" /></a>
</div>

![Copernicus Marine Service and Mercator Ocean international logos](https://www.mercator-ocean.eu/wp-content/uploads/2022/05/Cartouche_CMEMS_poisson_MOi.png)

## This is a test title
And this is a test paragraph
And another one

## Features

The `copernicusmarine` offers capabilities through both **Command Line Interface (CLI)** and **Python API**:

- **Metadata Information**: List and retrieve metadata information on all variables, datasets, products, and their associated documentation.
- **Subset Datasets**: Subset datasets to extract only the parts of interest, in preferred format, such as Analysis-Ready Cloud-Optimized (ARCO) Zarr or NetCDF file format.
- **Advanced Filters**: Apply simple or advanced filters to get multiple files, in original formats like NetCDF/GeoTIFF, via direct Marine Data Store connections.
- **No Quotas**: Enjoy no quotas, neither on volume size nor bandwidth.

## Installation

For installation, multiple options are available depending on your setup:

### Mamba | Conda

A `conda` package is available on [Anaconda](https://anaconda.org/conda-forge/copernicusmarine).

You can install it using `mamba` (or conda) through the `conda-forge` channel with the following command:

```bash
mamba install conda-forge::copernicusmarine --yes
```

To upgrade the Toolbox with mamba (or conda):

```bash
mamba update --name copernicusmarine copernicusmarine --yes
```

### Docker

A docker image is also available here: [https://hub.docker.com/r/copernicusmarine/copernicusmarine](https://hub.docker.com/r/copernicusmarine/copernicusmarine)

First step is to pull the container image:

```bash
docker pull copernicusmarine/copernicusmarine:latest
```

Then run it:

```bash
docker run -it --rm copernicusmarine/copernicusmarine --version
```

### Pip

Otherwise, if you already have an environment (safer to clone it), the package can be installed using the `pip` command:

```bash
python -m pip install copernicusmarine
```

And to **upgrade the package** to the newest available version, run:

```bash
python -m pip install copernicusmarine --upgrade
```

## User Guide

For more comprehensive details on how to use the `copernicusmarine` Toolbox, please refer to our [Help Center](https://help.marine.copernicus.eu/en/collections/9080063-copernicus-marine-toolbox). It ensures a smooth migration for existing users of legacy services such as MOTU, OPeNDAP or FTP.

### General configuration

#### Cache Usage

Cachier library is used for caching part of the requests (as result of `describe` or `login`). By default, the cache will be located in the home folder. If you need to change the location of the cache, you can set the environment variable `COPERNICUSMARINE_CACHE_DIRECTORY` to point to the desired directory:

- on **UNIX** platforms: `export COPERNICUSMARINE_CACHE_DIRECTORY=<PATH>`
- on **Windows** platforms: `set COPERNICUSMARINE_CACHE_DIRECTORY=<PATH>`

### Network configuration

#### Disable SSL

A global SSL context is used when making HTTP calls using the `copernicusmarine` Toolbox. For some reason, it can lead to unexpected behavior depending on your network configuration. You can set the `COPERNICUSMARINE_DISABLE_SSL_CONTEXT` environment variable to any value to globally disable the usage of SSL in the toolbox:

- on **UNIX** platforms: `export COPERNICUSMARINE_DISABLE_SSL_CONTEXT=True`
- on **Windows** platforms: `set COPERNICUSMARINE_DISABLE_SSL_CONTEXT=True`

#### Trust Env for python libraries

To do HTTP calls, the Copernicus Marine Toolbox uses two python libraries: requests and aiohttp. By default, those libraries will have `trust_env` values set to `True`. If you want to deactivate this, you can set `COPERNICUSMARINE_TRUST_ENV=False` (default `True`). This can be useful for example if you don't want those libraries to read your `.netrc` file as it has been reported that having a `.netrc` with a line: "default login anonymous password user@site" is incompatible with S3 connection required by the toolbox.

#### Proxy

To use proxies, as describe in the [aiohttp documentation](https://docs.aiohttp.org/en/stable/client_advanced.html#proxy-support) you can use two options:

- set the `HTTPS_PROXY` variable. For eg: `HTTPS_PROXY="http://user:pass@some.proxy.com"`. It should work even with `COPERNICUSMARINE_TRUST_ENV=False`.
- use a `.netrc` file but be aware that having a line: "default login anonymous password user@site" is incompatible with S3 connection required by the toolbox. Also note that if you have `COPERNICUSMARINE_TRUST_ENV=True` (the default value) then if `NETRC` environment variable is set with a specified location, the `.netrc` file will be read from the specified location there rather than from `~/.netrc`.

#### Number of concurrent requests

The toolbox makes many requests to STAC to be able to parse the full marine data store STAC catalog. For that, it uses asynchronous calls. It can be problematic to do too many requests at the same time. To limit the number of requests at the same time you can use: `COPERNICUSMARINE_MAX_CONCURRENT_REQUESTS`. The default value is `15` and minimum value is `1`.

Note, that this concerns only the catalog parsing step so the describe command and the start of the get and subset command. It does not apply when downloading files or listing files from the get command or when requesting the data chunks for the subset command.

For the `get` command, you can use the `COPERNICUSMARINE_GET_CONCURRENT_DOWNLOADS` to set the number of threads open to download in parallel. There are no default value. By default the toolbox uses the python `multiprocessing.pool.ThreadPool`. You can set the environment variable to 0 if you don't want to use the `multiprocessing` library at all, the download will be used only through `boto3`.

## Command Line Interface (CLI)

### The `--help` option

To discover commands and their available options, consider appending `--help` on any command line.

Example:

```bash
copernicusmarine --help
```

Returns:

```bash
Usage: copernicusmarine [OPTIONS] COMMAND [ARGS]...

Options:
  -V, --version  Show the version and exit.
  -h, --help     Show this message and exit.

Commands:
  describe  Print Copernicus Marine catalog as JSON.
  get       Download originally produced data files.
  login     Create a configuration file with your Copernicus Marine credentials.
  subset    Download subsets of datasets as NetCDF files or Zarr stores.
```

### Command `describe`

Retrieve metadata information about all products/datasets and display as JSON output:

```bash
copernicusmarine describe --include-datasets
```

The JSON output can also be saved as follows:

```bash
copernicusmarine describe --include-datasets > all_datasets_copernicusmarine.json
```

### Command `login`

Create a single configuration file `.copernicusmarine-credentials` allowing to access all Copernicus Marine Data Store data services. By default, the file is saved in user's home directory.

Example:

```bash
> copernicusmarine login
username : johndoe
password :
INFO - Configuration files stored in /Users/foo/.copernicusmarine
```

If `.copernicusmarine-credentials` already exists, the user is asked for confirmation to overwrite (`--overwrite`/`--overwrite-configuration-file`).

You can use the `--skip-if-user-logged-in` option to skip the configuration file overwrite if the user is already logged in.

#### Access points migration and evolution

If you still have a configuration for legacy services (e.g. `~/motuclient/motuclient-python.ini`, `~/.netrc` or `~/_netrc`) in your home directory, it will automatically be taken into account with commands `get` and `subset` without the need for running the `login` command.
If the configuration files are already available in another directory, when running commands `subset` or `get`, you can use the `--credentials-file` option to point to the files.

### Command `subset`

Remotely subset a dataset, based on variable names, geographical and temporal parameters.

Example:

```bash
copernicusmarine subset --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m --variable thetao --variable so --start-datetime 2021-01-01 --end-datetime 2021-01-03 --minimum-longitude 0.0 --maximum-longitude 0.1 --minimum-latitude 28.0 --maximum-latitude 28.1
```

Returns:

```bash
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
    ...                        ...
    references:                http://marine.copernicus.eu
    copernicusmarine_version:  1.1.0
INFO - 2024-04-03T10:18:18Z - Estimated size of the dataset file is 0.002 MB.

Do you want to proceed with download? [Y/n]:
```

By default, after the display of the summary of the dataset subset, a download confirmation is asked. To skip this confirmation, use the option `--force-download`.

#### Note about `--subset-method` option

By default, the `subset` feature uses the `nearest` method of xarray. By specifying `--subset-method strict`, you can only request dimension strictly inside the dataset, useful for **operational use-case**.

#### Note about longitude range

Options `--minimum-longitude` and `--maximum-longitude` work as follows:

- If the result of the substraction ( `--maximum-longitude` minus `--minimum-longitude` ) is superior or equal to 360, then return the full dataset.
- If the requested longitude range:
  - **does not cross** the antemeridian, then return the dataset between range -180 and 180.
  - **does cross** the antemeridian, then return the dataset between range 0 and 360.

Note that you can request any longitudes you want. A modulus is applied to bring the result between -180° and 360°. For example, if you request [530, 560], the result dataset will be in [170, 200].

#### Note about `--netcdf-compression-enabled` and `--netcdf-compression-level` options

When subsetting data, if you decide to write your data as a NetCDF file (which is the default behavior), then you can provide the extra option `--netcdf-compression-enabled`. The downloaded file will be lighter but it will take more time to write it (because of the compression task). If you don't provide it, the task will be faster, but the file heavier.
Otherwise, if you decide to write your data in Zarr format (`.zarr` extension), the original compression used in the Copernicus Marine Data Store will be applied, which means that the download task will be fast **and** the file compressed. In that case, you cannot use the `--netcdf-compression-enabled`.

Here are the default parameters added to xarray in the background when using the option: `{'zlib': True, 'complevel': 1, 'contiguous': False, 'shuffle': True}`

In addition to this option, you can also provide the `--netcdf-compression-level` option and customize the NetCDF compression level between 0 (no compression) and 9 (maximal compression).

#### Note about `--netcdf3-compatible` option

The `--netcdf3-compatible` option has been added to allow the downloaded dataset to be compatible with the netCDF3 format. It uses the `format="NETCDF3_CLASSIC"` of the xarray [to_netcdf](https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_netcdf.html) method.

### Command `get`

Download the dataset file(s) as originally produced, based on the datasetID or the path to files.

Example:

```bash
copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --service original-files
```

Returns:

```bash
INFO - 2024-04-03T11:39:18Z - Dataset version was not specified, the latest one was selected: "202211"
INFO - 2024-04-03T11:39:18Z - Dataset part was not specified, the first one was selected: "default"
INFO - 2024-04-03T11:39:18Z - Service was not specified, the default one was selected: "original-files"
INFO - 2024-04-03T11:39:18Z - Downloading using service original-files...
INFO - 2024-04-03T11:39:19Z - You requested the download of the following files:
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_19930101_19931231_R20221101_RE01.nc - 8.83 MB - 2023-11-12T23:47:13Z
[... truncated for brevity..]
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20120101_20121231_R20221101_RE01.nc - 8.62 MB - 2023-11-12T23:47:14Z
Printed 20 out of 29 files

Total size of the download: 252.94 MB
Do you want to proceed with download? [Y/n]:
```

By default:

- After the header displays a summary of the request, a download confirmation is asked. To skip this user's action, add option `--force-download`.
- Files are downloaded to the current directory applying the original folder structure. To avoid this behavior, add `--no-directories` and specify a destination with `-o/--output-directory`.

Option `--show-outputnames` displays the full paths of the output files, if required.

Option `--create-file-list` only creates a file containing the names of the targeted files instead of downloading them. You have to input a file name, e.g. `--create-file-list my_files.txt`. The format needs to be `.txt` or `.csv`:

- If the user inputs a filename that ends in `.txt`, then the file contains only the full s3 path to the targeted files and is compatible with the `--file-list` option.

Example:

```bash
copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m --filter "*2021*" --create-file-list selected_files_for_2021.txt
```

The content of `selected_files_for_2021.txt` would be:

```txt
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210101_20210131_R20230101_RE01.nc
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210201_20210228_R20230101_RE01.nc
[... truncated for brevity..]
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20211101_20211130_R20230101_RE01.nc
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20211201_20211231_R20230101_RE01.nc
```

- If the user inputs a filename that ends in `.csv` the file contains the following columns, separated by a comma: `filename`, `size` (in Bytes), `last_modified_datetime`, and `etag`. It is **not** compatible "as is" with the `--file-list` option and would need further post-processing from user's side.

Example:

```bash
copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m --filter "*2021*" --create-file-list selected_files_for_2021.csv
```

 The content of `selected_files_for_2021.csv` would be:

```txt
filename,size,last_modified_datetime,etag
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210101_20210131_R20230101_RE01.nc,12295906,2023-11-12 23:47:05.466000+00:00,"e8a7e564f676a08bf601bcdeaebdc563"
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210201_20210228_R20230101_RE01.nc,12436177,2023-11-12 23:47:05.540000+00:00,"d4a22dfb6c7ed85860c4a122c45eb953"
[... truncated for brevity..]
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20211101_20211130_R20230101_RE01.nc,12386940,2023-11-12 23:47:06.358000+00:00,"ea15d1f70fcc7f2ce404184d983530ff"
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20211201_20211231_R20230101_RE01.nc,12398208,2023-11-12 23:47:06.456000+00:00,"585f49867aaefa2ce9d6e68dd468b5e1"
```

If specified, no other action will be performed.

#### Note about sync option

Option `--sync` allows to download original files only if not exist and not up to date. The Toolbox checks the destination folder against the source folder. It can be combined with filters. Note that if set with `--overwrite-output-data`, the latter will be ignored.
The logic is largely inspired from [s5cmd package sync command](https://github.com/peak/s5cmd#sync).
Option `--sync-delete` will work as `--sync` with the added fonctionnality that it deletes any local file that has not been found on the remote server. Note that the files found on the server are also filtered. Hence, a file present locally might be deleted even if it is on the server because, for example, the executed `get` command contains a filter that excludes this specific file.

Limitations:

- `--sync` is not compatible with `--no-directories`.
- `--sync` only works with `--dataset-version`.
- `--sync` functionality is not available for datasets with several parts (like INSITU or static datasets for example).

#### Note about filtering options

Option `--filter` allows to specify a Unix shell-style wildcard pattern (see [fnmatch — Unix filename pattern matching](https://docs.python.org/3/library/fnmatch.html)) and select specific files:

```bash
copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --filter "*01yav_200[0-2]*"
```

Returns:

```bash
INFO - 2024-04-03T11:51:15Z - Dataset version was not specified, the latest one was selected: "202211"
INFO - 2024-04-03T11:51:15Z - Dataset part was not specified, the first one was selected: "default"
INFO - 2024-04-03T11:51:15Z - Service was not specified, the default one was selected: "original-files"
INFO - 2024-04-03T11:51:15Z - Downloading using service original-files...
INFO - 2024-04-03T11:51:17Z - You requested the download of the following files:
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20000101_20001231_R20221101_RE01.nc - 8.93 MB
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc - 8.91 MB
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc - 8.75 MB

Total size of the download: 26.59 MB
Do you want to proceed with download? [Y/n]:
```

Option `--regex` allows to specify a regular expression for more advanced files selection:

```bash
copernicusmarine get -i cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --regex ".*01yav_20(00|01|02).*.nc"
```

Returns:

```bash
INFO - 2024-04-03T11:52:43Z - Dataset version was not specified, the latest one was selected: "202211"
INFO - 2024-04-03T11:52:43Z - Dataset part was not specified, the first one was selected: "default"
INFO - 2024-04-03T11:52:43Z - Service was not specified, the default one was selected: "original-files"
INFO - 2024-04-03T11:52:43Z - Downloading using service original-files...
INFO - 2024-04-03T11:52:44Z - You requested the download of the following files:
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20000101_20001231_R20221101_RE01.nc - 8.93 MB - 2023-11-12T23:47:13Z
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc - 8.91 MB - 2023-11-12T23:47:13Z
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc - 8.75 MB - 2023-11-12T23:47:13Z

Total size of the download: 26.59 MB
Do you want to proceed with download? [Y/n]:
```

#### Notes about the file list option

Option `--file-list` allows to specify a list of files for more advanced files selection.
The file can contain complete absolute paths for each target file (default behavior) or only a partial path defined by the user, as shown below.

By default, the get functionality lists all the files on the bucket to be able to select the requested ones. This create some overhead when there are a lot of files for a specific dataset. For example, a dataset with more than 100 000 files would create an overhead of around two minutes. The file list option will directly download the files and avoid the listings if all the files listed are found.

Careful, a path can easily be mispelled or wrongly queried. The toolbox will display a warning if the file is not found on the bucket and try to find the file by listing all the files on the bucket.

Example of `file_list.txt` with paths that would be directly downloaded without the listing overhead:

```txt
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
```


Example of `file_list.txt` with absolute paths:

```txt
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210101_20210131_R20230101_RE01.nc
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210201_20210228_R20230101_RE01.nc
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210301_20210331_R20230101_RE01.nc
```

Note that a path to a file can be seen in 3 parts:

- the provenance that indicates in which bucket the data is. For example, `s3://mdl-native-10/`. It can be found in the metadata.
- the productID and datasetID. For example, `IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/`. It also contains the version when the dataset has one.
- the filename which is everything that comes after the dataset id. For example, `2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210301_20210331_R20230101_RE01.nc`. It should be considered like a filename. If any components are absent, the file name is not complete and the file cannot be directly downloaded. Thus a listing of all the files is necessary in order to download the file. For example, `2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210301_20210331_R20230101_RE01.nc` is a filename and `CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20210301_20210331_R20230101_RE01.nc` is an incomplete filename.

> **_NOTE:_** This option is compatible with the file generated by the `--create-file-list` option if you generated a ".txt" file.

Then the following command:

```bash
copernicusmarine get -i cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --file-list file_list.txt
```

Returns:

```bash
INFO - 2024-04-03T12:57:44Z - Dataset version was not specified, the latest one was selected: "202211"
INFO - 2024-04-03T12:57:44Z - Dataset part was not specified, the first one was selected: "default"
INFO - 2024-04-03T12:57:44Z - Service was not specified, the default one was selected: "original-files"
INFO - 2024-04-03T12:57:44Z - Downloading using service original-files...
INFO - 2024-04-03T12:57:45Z - You requested the download of the following files:
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20000101_20001231_R20221101_RE01.nc - 8.93 MB - 2023-11-12T23:47:13Z
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc - 8.91 MB - 2023-11-12T23:47:13Z
s3://mdl-native-10/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc - 8.75 MB - 2023-11-12T23:47:13Z

Total size of the download: 26.59 MB
Do you want to proceed with download? [Y/n]:
```

Also, there is a specific command `--index-parts` to retrieve the index files of INSITU datasets (as listed on the [Copernicus Marine File Browser](https://data.marine.copernicus.eu/product/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/files?subdataset=cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311--ext--history&path=INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034%2Fcmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311%2F)).
> **_NOTE:_** In the future, it is planned to have the index files for those datasets directly available through the `--filter`, `--regex` and/or `--file-list` options. Meanwhile, check this [Help Center article for a working example](https://help.marine.copernicus.eu/en/articles/9133855-how-to-download-insitu-data-using-index-files).

Then the following command:

```bash
copernicusmarine get --dataset-id cmems_obs-ins_blk_phybgcwav_mynrt_na_irr --index-parts
```

Returns:

```txt
INFO - 2024-04-03T12:58:40Z - Dataset version was not specified, the latest one was selected: "202311"
INFO - 2024-04-03T12:58:40Z - Dataset part was not specified, the first one was selected: "history"
INFO - 2024-04-03T12:58:40Z - You forced selection of service: original-files
INFO - 2024-04-03T12:58:40Z - Downloading using service original-files...
INFO - 2024-04-03T12:58:41Z - You requested the download of the following files:
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_history.txt - 333.13 kB - 2024-04-02T08:40:30Z
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_latest.txt - 466.38 kB - 2024-04-03T12:51:52Z
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_monthly.txt - 1.51 MB - 2024-03-05T18:09:43Z
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_platform.txt - 209.27 kB - 2024-04-03T08:33:37Z

Total size of the download: 2.52 MB
Do you want to proceed with download? [Y/n]:
```

### Shared options

Both `subset` and `get` commands provide these options:

#### Option `--overwrite-output-data`

When specified, the existing files will be overwritten.
Otherwise, if the files already exist on destination, new ones with a unique index will be created once the download has been accepted (or once `--force-download` is provided).

#### Option `--create-template`

Option to create a file in your current directory containing request parameters. If specified, no other action will be performed.
It will create the following files depending on the feature:

- `subset`

Example:

```bash
copernicusmarine subset --create-template
```

Returns:

```txt
INFO - 2024-04-04T14:38:09Z - Template created at: subset_template.json
```

- `get`
Example:

```bash
copernicusmarine get --create-template
```

Returns:

```txt
INFO - 2024-04-04T14:38:09Z - Template created at: get_template.json
```

#### Option `--request-file`

This option allows to specify request parameters but in a provided `.json` file, useful for batch processing.
You can try the following templates or use the `--create-template` option to create both `subset` or `get` template request files.

- Template for `subset` data request:

```json
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
```

Example:

```bash
copernicusmarine subset --request-file template_subset_data_request.json
```

- Template for `get` data request:

```json
{
    "dataset_id": "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
    "filter": "*01yav_200[0-2]*",
    "force_download": false,
    "log_level": "INFO",
    "no_directories": false,
    "no_metadata_cache": false,
    "output_directory": "./data/",
    "overwrite_output_data": false,
    "overwrite_metadata_cache": false,
    "show_outputnames": true
}
```

Example:

```bash
copernicusmarine get --request-file template_get_data_request.json
```

#### Option `--credentials-file`

You can use the `--credentials-file` option to point to a credentials file. The file can be either `.copernicusmarine-credentials`, `motuclient-python.ini`, `.netrc` or `_netrc`.

#### Option `--dataset-version`

You can use the `--dataset-version` option to fetch a specific dataset version. Particularly useful to keep an operational chain working when an evolution impact the chosen dataset.

#### Option `--dataset-part`

You can use the `--dataset-part` option to fecth a specific part for the chosen dataset version.

#### Option `--log-level`

Set the details printed to console by the command (based on standard logging library).
Available values are: `[DEBUG|INFO|WARN|ERROR|CRITICAL|QUIET]`

All logs of the library are by default logged in stderr except the output of the `describe` command and the output of `--show-outputnames` option that are sent to stdout.

_For versions <=1.2.4_, all logs are sent to stdout by default.

## Python package (API)

The `copernicusmarine` exposes a Python interface to allow you to [call commands as functions](https://help.marine.copernicus.eu/en/collections/9054839-main-functionalities).

## Documentation

A detailed standalone API documentation is under construction and will come at a later stage. For the moment, see the [Help Center](https://help.marine.copernicus.eu/en/collections/9080063-copernicus-marine-toolbox).

## Version management

We are using semantic versioning X.Y.Z → for example 1.0.2

- Z is bumped on minor non-breaking changes.
- Y is bumped on breaking changes.
- X is bumped on demand to highlight a new significant feature or for communication purposes (new Copernicus Marine Service release for example).

## Contribution

We welcome contributions from the community to enhance this package. If you find any issues or have suggestions for improvements, please check out our [Report Template](https://help.marine.copernicus.eu/en/articles/8218546-reporting-an-issue-or-feature-request).

## Future improvements & Roadmap

To keep up to date with the most recent and planned advancements, including revisions, corrections, and feature requests generated from users' feedback, please refer to our [Roadmap](https://help.marine.copernicus.eu/en/articles/8218641-next-milestones-and-roadmap).

## Join the community

Get in touch!

- Create your [Copernicus Marine Account](https://data.marine.copernicus.eu/register?redirect=%2Fproducts)
- [Log in](https://data.marine.copernicus.eu/login?redirect=%2Fproducts) and chat with us (bottom right corner of [Copernicus Marine Service](https://marine.copernicus.eu/))
- Join our [training workshops](https://marine.copernicus.eu/services/user-learning-services)
- Network y/our [Copernicus Stories](https://twitter.com/cmems_eu)
- Watch [our videos](https://www.youtube.com/channel/UC71ceOVy7WtVC7F04BKoEew)

## Licence

Licensed under the [EUPL](https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12)
