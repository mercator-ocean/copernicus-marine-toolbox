import logging
import pathlib
from typing import List, Optional, Union

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import (
    MutuallyExclusiveOption,
    assert_cli_args_are_not_set_except_create_template,
    credentials_file_option,
    force_dataset_part_option,
    force_dataset_version_option,
    force_download_option,
    tqdm_disable_option,
)
from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
    CustomDeprecatedClickOption,
)
from copernicusmarine.core_functions.fields_query_builder import (
    build_query,
    get_queryable_requested_fields,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_COORDINATES_SELECTION_METHOD,
    DEFAULT_COORDINATES_SELECTION_METHODS,
    DEFAULT_FILE_FORMAT,
    DEFAULT_FILE_FORMATS,
    DEFAULT_VERTICAL_AXES,
    DEFAULT_VERTICAL_AXIS,
    CoordinatesSelectionMethod,
    FileFormat,
    ResponseSubset,
    VerticalAxis,
)
from copernicusmarine.core_functions.subset import (
    create_subset_template,
    subset_function,
)
from copernicusmarine.core_functions.utils import (
    datetime_parser,
    get_geographical_inputs,
)

logger = logging.getLogger("copernicusmarine")
blank_logger = logging.getLogger("copernicusmarine_blank_logger")

DEFAULT_FIELDS_TO_INCLUDE = {
    "status",
    "message",
    "file_size",
    "data_transfer_size",
}


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
    type=float,
    help=documentation_utils.SUBSET["MINIMUM_LONGITUDE_HELP"],
)
@click.option(
    "-x",
    "alias_min_x",
    type=float,
    help=documentation_utils.SUBSET["ALIAS_MIN_X_HELP"],
)
@click.option(
    "--minimum-x",
    type=float,
    help=documentation_utils.SUBSET["MINIMUM_X_HELP"],
)
@click.option(
    "--maximum-longitude",
    type=float,
    help=documentation_utils.SUBSET["MAXIMUM_LONGITUDE_HELP"],
)
@click.option(
    "-X",
    "alias_max_x",
    type=float,
    help=documentation_utils.SUBSET["ALIAS_MAX_X_HELP"],
)
@click.option(
    "--maximum-x",
    type=float,
    help=documentation_utils.SUBSET["MAXIMUM_X_HELP"],
)
@click.option(
    "--minimum-latitude",
    type=click.FloatRange(min=-90, max=90),
    help=documentation_utils.SUBSET["MINIMUM_LATITUDE_HELP"],
)
@click.option(
    "-y",
    "alias_min_y",
    type=float,
    help=documentation_utils.SUBSET["ALIAS_MIN_Y_HELP"],
)
@click.option(
    "--minimum-y",
    type=float,
    help=documentation_utils.SUBSET["MINIMUM_Y_HELP"],
)
@click.option(
    "--maximum-latitude",
    type=click.FloatRange(min=-90, max=90),
    help=documentation_utils.SUBSET["MAXIMUM_LATITUDE_HELP"],
)
@click.option(
    "-Y",
    "alias_max_y",
    type=float,
    help=documentation_utils.SUBSET["ALIAS_MAX_Y_HELP"],
)
@click.option(
    "--maximum-y",
    type=float,
    help=documentation_utils.SUBSET["MAXIMUM_Y_HELP"],
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
    "--vertical-axis",
    "-V",
    type=click.Choice(DEFAULT_VERTICAL_AXES),
    default=DEFAULT_VERTICAL_AXIS,
    help=documentation_utils.SUBSET["VERTICAL_AXIS_HELP"],
)
@click.option(
    "--start-datetime",
    "-t",
    type=str,
    help=documentation_utils.SUBSET["START_DATETIME_HELP"]
    + " Caution: encapsulate date with “ “ to ensure valid "
    "expression for format “%Y-%m-%d %H:%M:%S”.",
)
@click.option(
    "--end-datetime",
    "-T",
    type=str,
    help=documentation_utils.SUBSET["END_DATETIME_HELP"]
    + " Caution: encapsulate date with “ “ to ensure valid "
    "expression for format “%Y-%m-%d %H:%M:%S”.",
)
@click.option(
    "--platform-id",
    "-p",
    "platform_ids",
    type=str,
    help=documentation_utils.SUBSET["PLATFORM_IDS_HELP"],
    multiple=True,
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
@credentials_file_option
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
    "--overwrite",
    is_flag=True,
    default=False,
    cls=MutuallyExclusiveOption,
    help=documentation_utils.SUBSET["OVERWRITE_HELP"],
    mutually_exclusive=["skip-existing"],
)
@click.option(
    "--skip-existing",
    is_flag=True,
    type=bool,
    default=False,
    cls=MutuallyExclusiveOption,
    help=documentation_utils.SUBSET["SKIP_EXISTING_HELP"],
    mutually_exclusive=["overwrite"],
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
    cls=CustomDeprecatedClickOption,
    custom_deprecated=["--motu-api-request"],
)
@click.option(
    "--dry-run",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.SUBSET["DRY_RUN_HELP"],
)
@click.option(
    "--response-fields",
    "-r",
    type=str,
    default=None,
    help=documentation_utils.GET["RESPONSE_FIELDS_HELP"],
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
@click.option(
    "--chunk-size-limit",
    type=click.IntRange(min=-1),
    default=-1,
    help=documentation_utils.SUBSET["CHUNK_SIZE_LIMIT_HELP"],
)
@click.option(
    "--staging",
    type=bool,
    default=False,
    is_flag=True,
    hidden=True,
)
@tqdm_disable_option
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=documentation_utils.SUBSET["LOG_LEVEL_HELP"],
)
@click.option(
    "--raise-if-updating",
    type=bool,
    default=False,
    is_flag=True,
    help=documentation_utils.SUBSET["RAISE_IF_UPDATING_HELP"],
)
@force_download_option
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
    minimum_x: Optional[float],
    maximum_x: Optional[float],
    minimum_y: Optional[float],
    maximum_y: Optional[float],
    alias_min_x: Optional[float],
    alias_max_x: Optional[float],
    alias_min_y: Optional[float],
    alias_max_y: Optional[float],
    minimum_depth: Optional[float],
    maximum_depth: Optional[float],
    vertical_axis: VerticalAxis,
    start_datetime: Optional[str],
    end_datetime: Optional[str],
    platform_ids: Optional[List[str]],
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
    overwrite: bool,
    skip_existing: bool,
    dry_run: bool,
    response_fields: Optional[str],
    disable_progress_bar: bool,
    log_level: str,
    chunk_size_limit: int,
    staging: bool,
    raise_if_updating: bool,
    force_download: bool,
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

    (
        minimum_x_axis,
        maximum_x_axis,
        minimum_y_axis,
        maximum_y_axis,
    ) = get_geographical_inputs(
        minimum_longitude,
        maximum_longitude,
        minimum_latitude,
        maximum_latitude,
        minimum_x,
        maximum_x,
        minimum_y,
        maximum_y,
        dataset_part,
    )
    if dataset_part == "originalGrid":
        if (
            alias_max_x is not None
            or alias_min_x is not None
            or alias_max_y is not None
            or alias_min_y is not None
        ):
            logger.debug(
                "Because you are using an originalGrid dataset, we are considering"
                " the options -x, -X, -y, -Y to be in m/km, not in degrees."
            )

    response = subset_function(
        dataset_id=dataset_id,
        force_dataset_version=dataset_version,
        force_dataset_part=dataset_part,
        username=username,
        password=password,
        variables=variables,
        minimum_x=(
            minimum_x_axis if minimum_x_axis is not None else alias_min_x
        ),
        maximum_x=(
            maximum_x_axis if maximum_x_axis is not None else alias_max_x
        ),
        minimum_y=(
            minimum_y_axis if minimum_y_axis is not None else alias_min_y
        ),
        maximum_y=(
            maximum_y_axis if maximum_y_axis is not None else alias_max_y
        ),
        minimum_depth=minimum_depth,
        maximum_depth=maximum_depth,
        vertical_axis=vertical_axis,
        start_datetime=(
            datetime_parser(start_datetime) if start_datetime else None
        ),
        end_datetime=datetime_parser(end_datetime) if end_datetime else None,
        platform_ids=platform_ids,
        coordinates_selection_method=coordinates_selection_method,
        output_filename=output_filename,
        file_format=file_format,
        force_service=service,
        request_file=request_file,
        output_directory=output_directory,
        credentials_file=credentials_file,
        motu_api_request=motu_api_request,
        overwrite=overwrite,
        skip_existing=skip_existing,
        dry_run=dry_run,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
        netcdf_compression_level=netcdf_compression_level,
        netcdf3_compatible=netcdf3_compatible,
        chunk_size_limit=chunk_size_limit,
        raise_if_updating=raise_if_updating,
    )
    if response_fields:
        fields_to_include = set(response_fields.replace(" ", "").split(","))
    elif dry_run:
        fields_to_include = {"all"}
    else:
        fields_to_include = DEFAULT_FIELDS_TO_INCLUDE

    included_fields: Optional[Union[dict, set]]
    if "all" in fields_to_include:
        included_fields = None
    elif "none" in fields_to_include:
        included_fields = set()
    else:
        queryable_fields = get_queryable_requested_fields(
            fields_to_include, ResponseSubset, "--response-fields"
        )
        included_fields = build_query(set(queryable_fields), ResponseSubset)

    blank_logger.info(
        response.model_dump_json(
            indent=2,
            include=included_fields,
            exclude_none=True,
            exclude_unset=True,
        )
    )
