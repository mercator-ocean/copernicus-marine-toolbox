
<h1 align="center">Copernicus Marine Service Toolbox (CLI & Python)</h1>
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

## Documentation

The full documentation of the toolbox is available here: [Copernicusmarine Documentation](https://toolbox-docs.marine.copernicus.eu/). Please refer to it for the more exhaustive and up to date documentation.

You might also find more comprehensive details on how to use the `copernicusmarine` Toolbox, please refer to our [Help Center](https://help.marine.copernicus.eu/en/collections/9080063-copernicus-marine-toolbox). It ensures a smooth migration for existing users of legacy services such as MOTU, OPeNDAP or FTP.

## Installation

For installation, multiple options are available depending on your setup:

### Mamba | Conda

```bash
mamba install conda-forge::copernicusmarine --yes
```

or conda:

```bash
conda install -c conda-forge copernicusmarine
```

### Docker

```bash
docker pull copernicusmarine/copernicusmarine:latest
```

### Pip

```bash
python -m pip install copernicusmarine
```

### Dependencies

Note that the use of `xarray<2024.7.0` with `numpy>=2.0.0` leads to inconsistent results. See this issue: [xarray issue](https://github.com/pydata/xarray/issues/9179).

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
  describe  Print Copernicus Marine catalogue as JSON.
  get       Download originally produced data files.
  login     Create a configuration file with your Copernicus Marine credentials.
  subset    Download subsets of datasets as NetCDF files or Zarr stores.
```

## Python package (API)

The `copernicusmarine` exposes a Python interface to allow you to [call commands as functions](https://toolbox-docs.marine.copernicus.eu/).

## Version management

We are using semantic versioning X.Y.Z → MAJOR.MINOR.PATCH → for example 1.0.2. We follow the SEMVER principles:

>Given a version number MAJOR.MINOR.PATCH, increment the:
>
>- MAJOR version when you make incompatible API changes
>- MINOR version when you add functionality in a backward compatible manner
>- PATCH version when you make backward compatible bug fixes
>
>Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.

## Contribution

We welcome contributions from the community to enhance this package. If you find any issues or have suggestions for improvements, please check out our [Report Template](https://help.marine.copernicus.eu/en/articles/8218546-reporting-an-issue-or-feature-request).

You are welcome to submit issues to the GitHub repository or create a pull request; however, please be advised that we may not respond to your request or may provide a negative response.

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
