import logging
import pathlib
from typing import List, Optional

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import (
    assert_cli_args_are_not_set_except_create_template,
    force_dataset_part_option,
    force_dataset_version_option,
    tqdm_disable_option,
)
from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_COORDINATES_SELECTION_METHOD,
    DEFAULT_COORDINATES_SELECTION_METHODS,
    DEFAULT_FILE_FORMAT,
    DEFAULT_FILE_FORMATS,
    DEFAULT_VERTICAL_DIMENSION_OUTPUT,
    DEFAULT_VERTICAL_DIMENSION_OUTPUTS,
    CoordinatesSelectionMethod,
    FileFormat,
    VerticalDimensionOutput,
)
from copernicusmarine.core_functions.subset import (
    create_subset_template,
    subset_function,
)
from copernicusmarine.core_functions.utils import datetime_parser

logger = logging.getLogger("copernicusmarine")
blank_logger = logging.getLogger("copernicusmarine_blank_logger")


@click.group()
def cli_subset() -> None:
    pass


@cli_subset.command(
    "subset",
    cls=CustomClickOptionsCommand,
    short_help="Download subsets of datasets as NetCDF files or Zarr stores.",
    help=documentation_utils.SUBSET["SUBSET_DESCRIPTION_HELP"]
    + "See :ref:`describe <cli-describe>`."
    + " \n\nReturns\n "
    + documentation_utils.SUBSET["SUBSET_RESPONSE_HELP"],
    epilog="""
    Examples:

    .. code-block:: bash

        copernicusmarine subset --dataset-id cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m --variable thetao --variable so --start-datetime 2021-01-01 --end-datetime 2021-01-03 --minimum-longitude 0.0 --maximum-longitude 0.1 --minimum-latitude 28.0 --maximum-latitude 28.1 --minimum-depth 1 --maximum-depth 2

    Equivalent to:

    .. code-block:: bash

        copernicusmarine subset -i cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m -v thetao -v so -t 2021-01-01 -T 2021-01-03 -x 0.0 -X 0.1 -y 28.0 -Y 28.1 -z 1 -Z 2 \n
    """,  # noqa
)
@click.option(
    "--dataset-id",
    "-i",
    type=str,
    default=None,
    help=documentation_utils.SUBSET["DATASET_ID_HELP"],
)
@force_dataset_version_option
@force_dataset_part_option
@click.option(
    "--username",
    type=str,
    default=None,
    help=documentation_utils.SUBSET["USERNAME_HELP"],
)
@click.option(
    "--password",
    type=str,
    default=None,
    help=documentation_utils.SUBSET["PASSWORD_HELP"],
)
@click.option(
    "--variable",
    "-v",
    "variables",
    type=str,
    help=documentation_utils.SUBSET["VARIABLES_HELP"],
    multiple=True,
)
@click.option(
    "--minimum-longitude",
    "-x",
    type=float,
    help=documentation_utils.SUBSET["MINIMUM_LONGITUDE_HELP"],
)
@click.option(
    "--maximum-longitude",
    "-X",
    type=float,
    help=documentation_utils.SUBSET["MAXIMUM_LONGITUDE_HELP"],
)
@click.option(
    "--minimum-latitude",
    "-y",
    type=click.FloatRange(min=-90, max=90),
    help=documentation_utils.SUBSET["MINIMUM_LATITUDE_HELP"],
)
@click.option(
    "--maximum-latitude",
    "-Y",
    type=click.FloatRange(min=-90, max=90),
    help=documentation_utils.SUBSET["MAXIMUM_LATITUDE_HELP"],
)
@click.option(
    "--minimum-depth",
    "-z",
    type=click.FloatRange(min=0),
    help=documentation_utils.SUBSET["MINIMUM_DEPTH_HELP"],
)
@click.option(
    "--maximum-depth",
    "-Z",
    type=click.FloatRange(min=0),
    help=documentation_utils.SUBSET["MAXIMUM_DEPTH_HELP"],
)
@click.option(
    "--vertical-dimension-output",
    "-V",
    type=click.Choice(DEFAULT_VERTICAL_DIMENSION_OUTPUTS),
    default=DEFAULT_VERTICAL_DIMENSION_OUTPUT,
    help=documentation_utils.SUBSET["VERTICAL_DIMENSION_OUTPUT_HELP"],
)
@click.option(
    "--start-datetime",
    "-t",
    type=str,
    help=documentation_utils.SUBSET["START_DATETIME_HELP"]
    + "Caution: encapsulate date with “ “ to ensure valid "
    "expression for format “%Y-%m-%d %H:%M:%S”.",
)
@click.option(
    "--end-datetime",
    "-T",
    type=str,
    help=documentation_utils.SUBSET["END_DATETIME_HELP"]
    + "Caution: encapsulate date with “ “ to ensure valid "
    "expression for format “%Y-%m-%d %H:%M:%S”.",
)
@click.option(
    "--coordinates-selection-method",
    type=click.Choice(DEFAULT_COORDINATES_SELECTION_METHODS),
    default=DEFAULT_COORDINATES_SELECTION_METHOD,
    help=documentation_utils.SUBSET["COORDINATES_SELECTION_METHOD_HELP"],
)
@click.option(
    "--output-directory",
    "-o",
    type=click.Path(path_type=pathlib.Path),
    help=documentation_utils.SUBSET["OUTPUT_DIRECTORY_HELP"],
)
@click.option(
    "--credentials-file",
    type=click.Path(path_type=pathlib.Path),
    help=documentation_utils.SUBSET["CREDENTIALS_FILE_HELP"],
)
@click.option(
    "--output-filename",
    "-f",
    type=str,
    help=documentation_utils.SUBSET["OUTPUT_FILENAME_HELP"],
)
@click.option(
    "--file-format",
    type=click.Choice(DEFAULT_FILE_FORMATS),
    default=DEFAULT_FILE_FORMAT,
    help=documentation_utils.SUBSET["FILE_FORMAT_HELP"],
)
@click.option(
    "--force-download",
    is_flag=True,
    default=False,
    help=documentation_utils.SUBSET["FORCE_DOWNLOAD_HELP"],
)
@click.option(
    documentation_utils.SUBSET["OVERWRITE_LONG_OPTION"],
    documentation_utils.SUBSET["OVERWRITE_SHORT_OPTION"],
    is_flag=True,
    default=False,
    help=documentation_utils.SUBSET["OVERWRITE_OUTPUT_DATA_HELP"],
)
@click.option(
    "--service",
    "-s",
    type=str,
    help=documentation_utils.SUBSET["SERVICE_HELP"],
)
@click.option(
    "--create-template",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.SUBSET["CREATE_TEMPLATE_HELP"],
)
@click.option(
    "--request-file",
    type=click.Path(exists=True, path_type=pathlib.Path),
    help=documentation_utils.SUBSET["REQUEST_FILE_HELP"],
)
@click.option(
    "--motu-api-request",
    type=str,
    help=documentation_utils.SUBSET["MOTU_API_REQUEST_HELP"],
)
@click.option(
    "--dry-run",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.SUBSET["DRY_RUN_HELP"],
)
@tqdm_disable_option
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=documentation_utils.SUBSET["LOG_LEVEL_HELP"],
)
@click.option(
    "--staging",
    type=bool,
    default=False,
    is_flag=True,
    hidden=True,
)
@click.option(
    "--netcdf-compression-level",
    type=click.IntRange(0, 9),
    is_flag=False,
    flag_value=1,
    default=0,
    help=documentation_utils.SUBSET["NETCDF_COMPRESSION_LEVEL_HELP"]
    + " If used as a flag, the assigned value will be 1.",
)
@click.option(
    "--netcdf3-compatible",
    type=bool,
    default=False,
    is_flag=True,
    help=documentation_utils.SUBSET["NETCDF3_COMPATIBLE_HELP"],
)
@log_exception_and_exit
def subset(
    dataset_id: str,
    dataset_version: Optional[str],
    dataset_part: Optional[str],
    username: Optional[str],
    password: Optional[str],
    variables: Optional[List[str]],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_depth: Optional[float],
    maximum_depth: Optional[float],
    vertical_dimension_output: VerticalDimensionOutput,
    start_datetime: Optional[str],
    end_datetime: Optional[str],
    coordinates_selection_method: CoordinatesSelectionMethod,
    output_filename: Optional[str],
    file_format: FileFormat,
    netcdf_compression_level: int,
    netcdf3_compatible: bool,
    service: Optional[str],
    create_template: bool,
    request_file: Optional[pathlib.Path],
    output_directory: Optional[pathlib.Path],
    credentials_file: Optional[pathlib.Path],
    motu_api_request: Optional[str],
    force_download: bool,
    overwrite_output_data: bool,
    dry_run: bool,
    disable_progress_bar: bool,
    log_level: str,
    staging: bool = False,
):
    if log_level == "QUIET":
        logger.disabled = True
        logger.setLevel(level="CRITICAL")
    else:
        logger.setLevel(level=log_level)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("DEBUG mode activated")

    if create_template:
        assert_cli_args_are_not_set_except_create_template(
            click.get_current_context()
        )
        create_subset_template()
        return

    response = subset_function(
        dataset_id=dataset_id,
        force_dataset_version=dataset_version,
        force_dataset_part=dataset_part,
        username=username,
        password=password,
        variables=variables,
        minimum_longitude=minimum_longitude,
        maximum_longitude=maximum_longitude,
        minimum_latitude=minimum_latitude,
        maximum_latitude=maximum_latitude,
        minimum_depth=minimum_depth,
        maximum_depth=maximum_depth,
        vertical_dimension_output=vertical_dimension_output,
        start_datetime=(
            datetime_parser(start_datetime) if start_datetime else None
        ),
        end_datetime=datetime_parser(end_datetime) if end_datetime else None,
        coordinates_selection_method=coordinates_selection_method,
        output_filename=output_filename,
        file_format=file_format,
        force_service=service,
        request_file=request_file,
        output_directory=output_directory,
        credentials_file=credentials_file,
        motu_api_request=motu_api_request,
        force_download=force_download,
        overwrite_output_data=overwrite_output_data,
        dry_run=dry_run,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
        netcdf_compression_level=netcdf_compression_level,
        netcdf3_compatible=netcdf3_compatible,
    )
    blank_logger.info(response.model_dump_json(indent=2))
