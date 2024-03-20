
<h1 align="center">Copernicus Marine Service toolbox (CLI & Python)</h1>
<div align="center">
  <a href="https://pypi.org/project/copernicusmarine/"><img src="https://img.shields.io/pypi/v/copernicusmarine.svg?style=flat-square" alt="PyPI" /></a>
  <a href="https://pypi.org/project/copernicusmarine/"><img src="https://img.shields.io/pypi/pyversions/copernicusmarine.svg?style=flat-square" alt="PyPI Supported Versions" /></a>
  <a href="https://pypi.org/project/copernicusmarine/"><img src="https://img.shields.io/badge/platform-windows | linux | macos-lightgrey?style=flat-square" alt="Supported Platforms" /></a>
  <a href="https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12"><img src="https://img.shields.io/badge/licence-EUPL-lightblue?style=flat-square" alt="Licence" /></a>
</div>

![Copernicus Marine Service and Mercator Ocean international logos](https://www.mercator-ocean.eu/wp-content/uploads/2022/05/Cartouche_CMEMS_poisson_MOi.png)

## Features
The `copernicusmarine` offers capabilities through both **Command Line Interface (CLI)** and **Python API**:
- **Metadata Information**: List and retrieve metadata information on all variables, datasets, products, and their associated documentation.
- **Subset Datasets**: Subset datasets to extract only the parts of interest, in preferred format, such as Analysis-Ready Cloud-Optimized (ARCO) Zarr or NetCDF file format.
- **Advanced Filters**: Apply simple or advanced filters to get multiple files, in original formats like NetCDF/GeoTIFF, via direct Marine Data Store connections.
- **No Quotas**: Enjoy no quotas, neither on volume size nor bandwidth.

## Installation
For installation, multiple options are available depending on your setup:

### Conda|Mamba

A conda package is available and has been uploaded to the conda-forge channel.

Here is the main web page of it: [https://anaconda.org/conda-forge/copernicusmarine](https://anaconda.org/conda-forge/copernicusmarine)

You can install it using conda though the conda-forge channel with the following command: `conda install copernicusmarine -c conda-forge`

### Docker

A docker image is also available here: [https://hub.docker.com/r/copernicusmarine/copernicusmarine](https://hub.docker.com/r/copernicusmarine/copernicusmarine)

Here is a basic command to run it: `docker run -it --rm copernicusmarine/copernicusmarine:1.0.5 --version`

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
For more comprehensive details on how to use the `copernicusmarine`, please refer to our [Help Center](https://help.marine.copernicus.eu/en/collections/4060068-copernicus-marine-toolbox). It ensures a smooth migration for existing users of legacy services such as MOTU, OPeNDAP, and FTP.

### General configuration

#### Cache Usage

Cachier library is used for caching part of the requests (as describe result or login). By default, the cache will be located in the home folder. If you need to change the location of the cache, you can set the environment variable `COPERNICUSMARINE_CACHE_DIRECTORY` to point to the desired directory.

#### Disable SSL

A global SSL context is used when making HTTP calls using the `copernicusmarine` toolbox. For some reason, it can lead to unexpected behavior depending on your network configuration. You can set the `COPERNICUSMARINE_DISABLE_SSL_CONTEXT` environmnent variable to any value to globally disable the usage of SSL in the client (e.g. `COPERNICUSMARINE_DISABLE_SSL_CONTEXT=True`).

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
  --help         Show this message and exit.

Commands:
  describe  Print Copernicus Marine catalog as JSON
  login     This command check the copernicusmarine credentials provided...
  get       Download originally produced data files
  subset    Downloads subsets of datasets as NetCDF files or Zarr stores
```

### Command `describe`
Retrieve metadata information about all products/datasets and display as JSON output:
```bash
copernicusmarine describe --include-datasets
```

The JSON output can also be saved like follows:
```bash
copernicusmarine describe --include-datasets > all_datasets_copernicusmarine.json
```

### Command `login`
Create a single configuration file `.copernicusmarine-credentials` allowing to access all Marine Data Store data services. By default, it saves file in user's home directory.

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

If you already have a configuration for current services (e.g. `~/motuclient/motuclient-python.ini`, `~/.netrc` or `~/_netrc`) in your home directory, it will automatically be taken into account with commands `get` and `subset` without the need for running the `login` command.
If the configuration files are already available in another directory, when running commands `subset` or `get`, you can use the `--credentials-file` option to point to the file.

### Command `subset`
Remotely subset a dataset, based on variable names, geographical and temporal parameters.

Example:
```bash
copernicusmarine subset --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --variable thetao --variable so --start-datetime 2021-01-01 --end-datetime 2021-01-03 --minimum-longitude 0.0 --maximum-longitude 0.1 --minimum-latitude 28.0 --maximum-latitude 28.1
```
Returns:
```bash
INFO     - Download through S3
<xarray.Dataset>
Dimensions:    (depth: 50, latitude: 2, longitude: 1, time: 1)
Coordinates:
  * depth      (depth) float32 0.5058 1.556 2.668 ... 5.292e+03 5.698e+03
  * latitude   (latitude) float32 28.0 28.08
  * longitude  (longitude) float32 0.08333
  * time       (time) datetime64[ns] 2021-01-01
Data variables:
    thetao     (time, depth, latitude, longitude) float32 dask.array<chunksize=(1, 50, 2, 1), meta=np.ndarray>
    so         (time, depth, latitude, longitude) float32 dask.array<chunksize=(1, 50, 2, 1), meta=np.ndarray>
Attributes: (12/19)
    Conventions:    CF-1.0
    bulletin_date:  2022-11-01
    ...             ...
    title:          CMEMS IBI REANALYSIS: YEARLY PHYSICAL PRODUCTS
Do you want to proceed with download? [Y/n]:
```

By default, after the display of the summary of the dataset subset, a download confirmation is asked. To skip this user's action, call option `--force-download`.

#### Note about longitude range
Options `--minimum-longitude` and `--maximum-longitude` work as follows:
- If the result of the substraction ( `--maximum-longitude` minus `--minimum-longitude` ) is superior or equal to 360, then return the full dataset.
- If the requested longitude range:
  - **does not cross** the antemeridian, then return the dataset between range -180° and 180°.
  - **does cross** the antemeridian, then return the dataset between range 0° and 360°.

Note that you can request any longitudes you want. A modulus is applied to bring the result between -180° and 360°. For example, if you request [530°, 560°], the result dataset will be in [170°, 200°].

#### Access point migration and evolution
The copernicus marine toolbox will download the data in the most efficient way according to your request:
- if the target dataset **is available** in ARCO version, then files are downloaded in a fresh new folder in the current working directory.
- if the target dataset **is not yet available** in ARCO version, then a file is downloaded in the current working directory.
> **_NOTE:_**  The filename will be with the following format `dataset_id-longitude_range-latitude_range-depth_range-date_range.[nc|zarr]`

However, the output directory and filename can be specified using `-o`/`--output-directory` and `-f`/`--output-filename` respectively. If the later ends with `.nc`, it will be written as a NetCDF file.

You can force the use of a specific data access service with option `--service`.

#### Note about netcdf-compression-enabled and --netcdf-compression-enabled options
When subseting data, if you decide to write your data as a NetCDF file (which is the default behavior), then you can provide the extra option "--netcdf-compression-enabled". The downloaded file will be lighter but it will take you more time to write it (because of the compression task). If you don't provide it, the task will be faster, but the file heavier.
Finally, if you decide to write your data in ZARR format (.zarr extension), then the original compression that is used in the Marine Data Store will be apply, which mean that the download task will be fast AND the file compressed. In that case, you cannot use the "netcdf-compression-enabled" as it has no sense.

Here is the default options added to xarray in the background when using the option: {'zlib': True, 'complevel': 1, 'contiguous': False, 'shuffle': True}

In addition to this option, you can also provide the `--netcdf-compression-enabled` option and customize the NetCDF compression level between 0 (no compression) and 9.

### Command `get`
Download the dataset file(s) as originally produced, based on the datasetID or the path to files.

Example:
```bash
copernicusmarine get --dataset-url ftp://my.cmems-du.eu/Core/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --service original-files
```
Returns:
```bash
INFO     - You forced selection of service: original-files
INFO     - Downloading using service original-files...
INFO     - You requested the download of the following files:
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_19930101_19931231_R20221101_RE01.nc - 8.83 MB
[... truncated for brevity..]
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20120101_20121231_R20221101_RE01.nc - 8.62 MB
Printed 20 out of 29 files

Total size of the download: 252.94 MB

Do you want to proceed with download? [Y/n]
```

You can force the use of a specific data access service with option `--service`.

By default:
- after the header displays a summary of the request, a download confirmation is asked. To skip this user's action, add option `--force-download`.
- files are downloaded to the current directory applying the original folder structure. To avoid this behavior, add `--no-directories` and specify a destination with the `--output-directory` option.

Option `--show-outputnames` displays the full paths of the output files, if required.

Option `--download-file-list` only creates a file `files_to_download.txt` containing the names of the targeted files instead of downloading them. If specified, no other action will be performed.

#### Note about sync option

Option `--sync` allows to download original files only if not exist and not up to date. The toolbox checks the destination folder against the source folder. It can be combined with filters. Note that if set with `--overwrite-output-data`, the latter will be ignored.
The logic is largely inspired from [s5mp package sync command](https://github.com/peak/s5cmd#sync)
Option `--sync-delete` will work as `--sync` with the added fonctionnality that it deletes any local file that has not been found on the remote server. Note that the files found on the server are also filtered. Hence, a file present locally might be deleted even if it is on the server because,  for example, the executed `get` command contains a filter that excludes this specific file.
Limitations:
- is not compatible with `--no-directories`.
- Version needs to be set when using sync (with `--force-dataset-version` flag)
- As for now, the sync functionality is not available for datasets with several parts (like INSITU or static datasets for example).

#### Note about filtering options
Option `--filter` allows to specify a Unix shell-style wildcard pattern (see [fnmatch — Unix filename pattern matching](https://docs.python.org/3/library/fnmatch.html)) and select specific files:
```bash
copernicusmarine get --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --filter "*01yav_200[0-2]*"
```
Returns:
```bash
INFO     - Downloading using service files...
INFO     - You requested the download of the following files:
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
INFO     - Downloading using service files...
INFO     - You requested the download of the following files:
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20000101_20001231_R20221101_RE01.nc - 8.93 MB
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc - 8.91 MB
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc - 8.75 MB

Total size of the download: 26.59 MB
Do you want to proceed with download? [Y/n]:
```

Option `--file-list` allows to specify a list of files for more advanced files selection:

An example `file_list.txt` would look like this:
```txt
CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20000101_20001231_R20221101_RE01.nc
CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc
CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc
```
> **_NOTE:_**  This option is compatible with the file generated by the `--download-file-list` option.

Then the following command:
```bash
copernicusmarine get -i cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m --file-list file_list.txt
```
Returns:
```bash
INFO     - Downloading using service files...
INFO     - You requested the download of the following files:
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20000101_20001231_R20221101_RE01.nc - 8.93 MB
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc - 8.91 MB
s3://mdl-native/native/IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc - 8.75 MB

Total size of the download: 26.59 MB
Do you want to proceed with download? [Y/n]:
```

Also, there is a specific command `--index-part` to retrieve the index files of INSITU datasets (for index files example see [this link on Copernicus Marine Service](https://data.marine.copernicus.eu/product/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/files?subdataset=cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311--ext--history&path=INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034%2Fcmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311%2F)). Note that in the future, we plan to have the index files for those datasets, directly available through the filter, regex and/or file-list options.

Then the following command:
```
copernicusmarine get --dataset-id cmems_obs-ins_blk_phybgcwav_mynrt_na_irr --index-parts
```

Returns:
```
INFO - 2024-03-13T08:08:12Z - Dataset version was not specified, the latest one was selected: "202311"
INFO - 2024-03-13T08:08:12Z - Dataset part was not specified, the first one was selected: "history"
INFO - 2024-03-13T08:08:12Z - You forced selection of service: original-files
INFO - 2024-03-13T08:08:12Z - Downloading using service original-files...
INFO - 2024-03-13T08:08:13Z - You requested the download of the following files:
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_history.txt - 244.61 kB - 2023-11-30T17:01:25Z
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_latest.txt - 572.09 kB - 2024-03-13T07:21:00Z
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_monthly.txt - 1.51 MB - 2024-03-05T18:09:43Z
s3://mdl-native-08/native/INSITU_BLK_PHYBGCWAV_DISCRETE_MYNRT_013_034/cmems_obs-ins_blk_phybgcwav_mynrt_na_irr_202311/index_platform.txt - 209.11 kB - 2024-03-13T07:21:00Z

Total size of the download: 2.53 MB


Do you want to proceed with download? [Y/n]:
```
### Shared options
Both `subset` and `get` commands provide these options:

#### Option `--overwrite-output-data`

When specified, the existing files will be overwritten.
Otherwise, if the files already exist on destination, new ones with a unique index will be created once the download has been accepted (or once `--force-download` is provided).

#### Option `--request-file`

This option allows to specify CLI options but in a provided JSON file, useful for batch processing.

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
	"output_filename": "temperature_small_pacific_2022208-202308.zarr",
	"force_download": false
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
    "service": "files",
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
You can use the `--dataset-version` option to fetch a specific dataset version.

#### Option `--dataset-part`
You can use the `--dataset-part` option to fecth a specific part for the chosen dataset version.


## Python package (API)
The `copernicusmarine` exposes a Python interface to allow you to [call commands as functions](https://marine.copernicus.eu/python-interface).

## Documentation
See the [Help Center](https://help.marine.copernicus.eu/en/collections/4060068-copernicus-marine-toolbox). A detailed standalone API documentation is under construction and will come at a later stage.

## Contribution
We welcome contributions from the community to enhance this package. If you find any issues or have suggestions for improvements, please check out our [Report Template](https://help.marine.copernicus.eu/en/articles/8218546-how-to-report-a-bug-or-suggest-new-features).

## Future improvements & Roadmap
- [ ] Make available the currently not managed Analysis-Ready Cloud-Optimized (ARCO) versions of Ocean Monitoring Indicator (OMI), in situ and static datasets.
- [ ] Allow to specify the compression level when downloading your subset as NetCDF file.
- [ ] Allow to subset variables using their `standard_name(s)` and not only their `name(s)`.

To keep up to date with the most recent and planned advancements, including revisions, corrections, and feature requests generated from users' feedback, please refer to our [Roadmap](https://help.marine.copernicus.eu/en/articles/8218641-what-are-the-next-milestones).

## Join the community
Get in touch!
- Create your [Copernicus Marine Account](https://data.marine.copernicus.eu/register)
- [Log in](https://data.marine.copernicus.eu/login?redirect=%2Fproducts) and chat with us (bottom right corner of [Copernicus Marine Service](https://marine.copernicus.eu/))
- Join our [training workshops](https://marine.copernicus.eu/services/user-learning-services)
- Network y/our [Copernicus Stories](https://twitter.com/cmems_eu)
- Watch [our videos](https://www.youtube.com/channel/UC71ceOVy7WtVC7F04BKoEew)

## Licence
Licensed under the (EUPL)[https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12]
