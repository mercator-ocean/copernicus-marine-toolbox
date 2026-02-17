import fnmatch
import importlib.util
import json
import logging
import os
import pathlib
import re
from datetime import datetime
from typing import Any, Type, TypeVar

import pandas as pd
from dateutil.tz import UTC
from pydantic import BaseModel, ValidationError, field_validator

from copernicusmarine.catalogue_parser.models import (
    get_version_and_part_from_full_dataset_id,
)
from copernicusmarine.core_functions.credentials_utils import (
    get_and_check_username_password,
)
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
    log_deprecated_message,
)
from copernicusmarine.core_functions.exceptions import (
    LonLatSubsetNotAvailableInOriginalGridDatasets,
    MutuallyExclusiveArguments,
    XYNotAvailableInNonOriginalGridDatasets,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_COORDINATES_SELECTION_METHOD,
    DEFAULT_FILE_EXTENSIONS,
    DEFAULT_FILE_FORMAT,
    DEFAULT_VERTICAL_AXIS,
    CoordinatesSelectionMethod,
    FileFormat,
    VerticalAxis,
)
from copernicusmarine.core_functions.utils import datetime_parser
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    TemporalParameters,
    XParameters,
    YParameters,
)
from copernicusmarine.versioner import __version__ as copernicusmarine_version

logger = logging.getLogger("copernicusmarine")


MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS: dict[str, str] = {
    "maximum_latitude": "maximum_y",
    "minimum_latitude": "minimum_y",
    "maximum_longitude": "maximum_x",
    "minimum_longitude": "minimum_x",
}

SubsetRequest_ = TypeVar("SubsetRequest_", bound="SubsetRequest")


class SubsetRequest(BaseModel):
    dataset_id: str
    username: str
    dataset_version: str | None = None
    dataset_part: str | None = None
    variables: list[str] | None = None
    minimum_x: float | None = None
    maximum_x: float | None = None
    minimum_y: float | None = None
    maximum_y: float | None = None
    minimum_depth: float | None = None
    maximum_depth: float | None = None
    vertical_axis: VerticalAxis = DEFAULT_VERTICAL_AXIS
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    platform_ids: list[str] | None = None
    coordinates_selection_method: CoordinatesSelectionMethod = (
        DEFAULT_COORDINATES_SELECTION_METHOD
    )
    output_filename: str | None = None
    file_format: FileFormat = DEFAULT_FILE_FORMAT
    service: str | None = None
    output_directory: pathlib.Path = pathlib.Path(".")
    overwrite: bool = False
    skip_existing: bool = False
    netcdf_compression_level: int = 0
    netcdf3_compatible: bool = False
    dry_run: bool = False
    raise_if_updating: bool = False
    disable_progress_bar: bool = False
    staging: bool = False
    chunk_size_limit: int = -1

    def update(self, new_dict: dict) -> "SubsetRequest":
        filtered_dict = {
            key: value
            for key, value in new_dict.items()
            if value is not None
            and not (isinstance(value, (list, tuple, str)) and not value)
        }
        data = self.model_dump(
            exclude_defaults=True,
            exclude_unset=True,
            exclude_none=True,
        )
        data.update(filtered_dict)
        return self.model_validate(data)

    @field_validator("start_datetime", "end_datetime", mode="before")
    @classmethod
    def parse_datetime(
        cls, v: datetime | pd.Timestamp | str | None
    ) -> datetime | None:
        if v is None or v == "":
            return None
        if isinstance(v, str):
            return datetime_parser(v)
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime().astimezone(UTC)
        return v.astimezone(UTC)

    @classmethod
    def from_file(
        cls: Type[SubsetRequest_],
        filepath: pathlib.Path,
        username: str | None = None,
    ) -> SubsetRequest_:
        with open(filepath) as json_file:
            json_content = json.load(json_file)
        if username:
            json_content["username"] = username
        json_content.pop("credentials_file", None)
        json_content.pop("password", None)
        transformed_data = cls._transform_deprecated_options(json_content)
        try:
            return cls(**transformed_data)
        except ValidationError as e:
            raise ValueError(f"Invalid request in file {filepath}: {e}")

    @classmethod
    def _transform_deprecated_options(
        cls: Type[SubsetRequest_], data: dict[str, Any]
    ) -> dict[str, Any]:
        transformed = {}
        for key, val in data.items():
            if key in DEPRECATED_OPTIONS:
                deprecated_option = DEPRECATED_OPTIONS[key]
                if deprecated_option.old_name == deprecated_option.new_name:
                    log_deprecated_message(deprecated_option.old_name, None)
                else:
                    log_deprecated_message(
                        deprecated_option.old_name,
                        deprecated_option.new_name,
                    )
                if deprecated_option.do_not_pass:
                    continue
                new_key = deprecated_option.new_name
                transformed[new_key] = val
            elif key in MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS:
                new_key = MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS[key]
                transformed[new_key] = val
            else:
                transformed[key] = val

        return transformed

    def get_temporal_parameters(
        self, axis_coordinate_id_mapping: dict[str, str]
    ) -> TemporalParameters:
        return TemporalParameters(
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            coordinate_id=axis_coordinate_id_mapping.get("t", "time"),
        )

    def get_geographical_parameters(
        self,
        axis_coordinate_id_mapping: dict[str, str],
        is_original_grid: bool = False,
    ) -> GeographicalParameters:
        return GeographicalParameters(
            x_axis_parameters=XParameters(
                minimum_x=self.minimum_x,
                maximum_x=self.maximum_x,
                coordinate_id=axis_coordinate_id_mapping.get("x", "longitude"),
            ),
            y_axis_parameters=YParameters(
                minimum_y=self.minimum_y,
                maximum_y=self.maximum_y,
                coordinate_id=axis_coordinate_id_mapping.get("y", "latitude"),
            ),
            projection="originalGrid" if is_original_grid else "lonlat",
        )

    def get_depth_parameters(
        self, axis_coordinate_id_mapping: dict[str, str]
    ) -> DepthParameters:
        return DepthParameters(
            minimum_depth=self.minimum_depth,
            maximum_depth=self.maximum_depth,
            vertical_axis=self.vertical_axis,
            coordinate_id=axis_coordinate_id_mapping.get("z", "depth"),
        )


def convert_motu_api_request_to_structure(
    motu_api_request: str,
    username: str,
) -> SubsetRequest:
    prefix = "python -m motuclient "
    string = motu_api_request.replace(prefix, "").replace("'", "")
    arguments = [
        substr.strip() for substr in string.split("--")[1:]
    ]  # for subsubstr in substr.split(" ", maxsplit=1)]
    arg_value_tuples = [
        tuple(substr.split(" ", maxsplit=1)) for substr in arguments
    ]
    motu_api_request_dict: dict[str, Any] = {}
    for arg, value in arg_value_tuples:
        if arg == "variable":
            # special case for variable, since it can have multiple values
            motu_api_request_dict.setdefault(arg, []).append(value)
        else:
            motu_api_request_dict[arg] = value
    subset_request = SubsetRequest(
        dataset_id="",
        username=username,
    )
    conversion_dict = {
        "product-id": "dataset_id",
        "latitude-min": "minimum_y",
        "latitude-max": "maximum_y",
        "longitude-min": "minimum_x",
        "longitude-max": "maximum_x",
        "depth-min": "minimum_depth",
        "depth-max": "maximum_depth",
        "date-min": "start_datetime",
        "date-max": "end_datetime",
        "variable": "variables",
    }
    subset_request = subset_request.update(
        {
            conversion_dict[key]: value
            for key, value in motu_api_request_dict.items()
            if key in conversion_dict.keys()
        }
    )
    return subset_request


def create_subset_request(
    dataset_id: str | None = None,
    dataset_version: str | None = None,
    dataset_part: str | None = None,
    username: str | None = None,
    password: str | None = None,
    variables: list[str] | None = None,
    minimum_depth: float | None = None,
    maximum_depth: float | None = None,
    vertical_axis: VerticalAxis = DEFAULT_VERTICAL_AXIS,
    start_datetime: datetime | pd.Timestamp | str | None = None,
    end_datetime: datetime | pd.Timestamp | str | None = None,
    platform_ids: list[str] | None = None,
    coordinates_selection_method: CoordinatesSelectionMethod = (
        DEFAULT_COORDINATES_SELECTION_METHOD
    ),
    output_filename: str | None = None,
    file_format: FileFormat | None = None,
    service: str | None = None,
    request_file: pathlib.Path | None = None,
    output_directory: pathlib.Path | None = None,
    credentials_file: pathlib.Path | None = None,
    motu_api_request: str | None = None,
    overwrite: bool = False,
    skip_existing: bool = False,
    dry_run: bool = False,
    disable_progress_bar: bool = False,
    staging: bool = False,
    netcdf_compression_level: int = 0,
    netcdf3_compatible: bool = False,
    chunk_size_limit: int = 0,
    raise_if_updating: bool = False,
    minimum_longitude: float | None = None,
    maximum_longitude: float | None = None,
    minimum_latitude: float | None = None,
    maximum_latitude: float | None = None,
    alias_min_x: float | None = None,
    alias_max_x: float | None = None,
    alias_min_y: float | None = None,
    alias_max_y: float | None = None,
    minimum_x: float | None = None,
    maximum_x: float | None = None,
    minimum_y: float | None = None,
    maximum_y: float | None = None,
) -> SubsetRequest:
    if staging:
        logger.warning(
            "Detecting staging flag for subset command. "
            "Data will come from the staging environment."
        )

    if overwrite:
        if skip_existing:
            raise MutuallyExclusiveArguments("overwrite", "skip_existing")
    if request_file and not username and not credentials_file:
        with open(request_file) as json_file:
            json_content = json.load(json_file)
        if "username" in json_content:
            username = json_content["username"]
        if "password" in json_content:
            password = json_content["password"]
        if "credentials_file" in json_content:
            credentials_file = pathlib.Path(json_content["credentials_file"])

    username, password = get_and_check_username_password(
        username,
        password,
        credentials_file,
    )
    subset_request = SubsetRequest(
        dataset_id=dataset_id or "",
        username=username,
    )
    if request_file:
        subset_request = SubsetRequest.from_file(
            request_file, username=username
        )
    if motu_api_request:
        motu_api_subset_request = convert_motu_api_request_to_structure(
            motu_api_request, username=username
        )
        subset_request = subset_request.update(
            motu_api_subset_request.__dict__
        )
    (
        subset_request.dataset_id,
        subset_request.dataset_version,
    ) = process_dataset_id_and_dataset_version(
        subset_request.dataset_id,
        dataset_version or subset_request.dataset_version,
    )
    if netcdf3_compatible:
        documentation_url = (
            f"https://toolbox-docs.marine.copernicus.eu"
            f"/en/v{copernicusmarine_version}/installation.html#dependencies"
        )
        assert importlib.util.find_spec("netCDF4"), (
            "To enable the NETCDF3_COMPATIBLE option, the 'netCDF4' "
            f"package is required. "
            f"Please see {documentation_url}."
        )
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
    if dataset_part == "originalGrid" and (
        alias_max_x is not None
        or alias_min_x is not None
        or alias_max_y is not None
        or alias_min_y is not None
    ):
        logger.debug(
            "Because you are using an originalGrid dataset, we are considering"
            " the options -x, -X, -y, -Y to be in m/km, not in degrees."
        )

    request_update_dict = {
        "dataset_version": dataset_version,
        "dataset_part": dataset_part,
        "variables": variables,
        "minimum_x": (
            minimum_x_axis if minimum_x_axis is not None else alias_min_x
        ),
        "maximum_x": (
            maximum_x_axis if maximum_x_axis is not None else alias_max_x
        ),
        "minimum_y": (
            minimum_y_axis if minimum_y_axis is not None else alias_min_y
        ),
        "maximum_y": (
            maximum_y_axis if maximum_y_axis is not None else alias_max_y
        ),
        "minimum_depth": minimum_depth,
        "maximum_depth": maximum_depth,
        "vertical_axis": vertical_axis,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "platform_ids": platform_ids,
        "output_filename": output_filename,
        "file_format": file_format,
        "service": service,
        "output_directory": output_directory,
        "chunk_size_limit": chunk_size_limit,
    }
    # To be able to distinguish between set and unset values
    if skip_existing:
        request_update_dict["skip_existing"] = skip_existing
    if overwrite:
        request_update_dict["overwrite"] = overwrite
    if netcdf_compression_level:
        request_update_dict[
            "netcdf_compression_level"
        ] = netcdf_compression_level
    if netcdf3_compatible:
        request_update_dict["netcdf3_compatible"] = netcdf3_compatible
    if coordinates_selection_method != DEFAULT_COORDINATES_SELECTION_METHOD:
        request_update_dict[
            "coordinates_selection_method"
        ] = coordinates_selection_method
    if raise_if_updating:
        request_update_dict["raise_if_updating"] = raise_if_updating
    if dry_run:
        request_update_dict["dry_run"] = dry_run
    if staging:
        request_update_dict["staging"] = staging
    if disable_progress_bar or dry_run:
        request_update_dict["disable_progress_bar"] = True
    if (
        output_filename
        and (suffix := pathlib.Path(output_filename).suffix)
        in DEFAULT_FILE_EXTENSIONS
    ):
        if suffix == ".nc":
            request_update_dict["file_format"] = "netcdf"
        elif suffix == ".csv":
            request_update_dict["file_format"] = "csv"
        elif suffix == ".zarr":
            request_update_dict["file_format"] = "zarr"

    return subset_request.update(request_update_dict)


def get_geographical_inputs(
    minimum_longitude: float | None,
    maximum_longitude: float | None,
    minimum_latitude: float | None,
    maximum_latitude: float | None,
    minimum_x: float | None,
    maximum_x: float | None,
    minimum_y: float | None,
    maximum_y: float | None,
    dataset_part: str | None,
) -> tuple[float | None, float | None, float | None, float | None]:
    """
    Returns the geographical selection of the user.

    Parameters
    ----------
    minimum_longitude : float
        Minimum longitude of the area of interest. For lat/lon datasets.
    maximum_longitude : float
        Maximum longitude of the area of interest. For lat/lon datasets.
    minimum_latitude : float
        Minimum latitude of the area of interest. For lat/lon datasets.
    maximum_latitude : float
        Maximum latitude of the area of interest. For lat/lon datasets.
    minimum_x : float
        Minimum x coordinate of the area of interest. For "originalGrid" datasets.
    maximum_x : float
        Maximum x coordinate of the area of interest. For "originalGrid" datasets.
    minimum_y : float
        Minimum y coordinate of the area of interest. For "originalGrid" datasets.
    maximum_y : float
        Maximum y coordinate of the area of interest. For "originalGrid" datasets.
    dataset_part : str
        The part of the dataset to be used. If "originalGrid", the x and y coordinates
        should be the inputs.

    Returns
    -------
    tuple[float | None, float | None, float | None, float | None]
        The geographical selection of the user. (minimum_x_axis, maximum_x_axis, minimum_y_axis, maximum_y_axis).

    Raises
    ------

    LonLatSubsetNotAvailableInOriginalGridDatasets
        If the dataset is "originalGrid" and the user tries to use lat/lon coordinates.
    XYNotAvailableInNonOriginalGridDatasets
        If the dataset is not "originalGrid" and the user tries to use x/y coordinates.
    """  # noqa: E501

    if dataset_part == "originalGrid":
        if (
            minimum_longitude is not None
            or maximum_longitude is not None
            or minimum_latitude is not None
            or maximum_latitude is not None
        ):
            raise LonLatSubsetNotAvailableInOriginalGridDatasets
        else:
            return (
                minimum_x,
                maximum_x,
                minimum_y,
                maximum_y,
            )
    else:
        if (
            minimum_x is not None
            or maximum_x is not None
            or minimum_y is not None
            or maximum_y is not None
        ):
            raise XYNotAvailableInNonOriginalGridDatasets
        else:
            return (
                minimum_longitude,
                maximum_longitude,
                minimum_latitude,
                maximum_latitude,
            )


class GetRequest(BaseModel):
    dataset_id: str
    username: str
    dataset_url: str | None = None
    dataset_version: str | None = None
    dataset_part: str | None = None
    no_directories: bool = False
    output_directory: pathlib.Path = pathlib.Path(".")
    overwrite: bool = False
    filter: str | None = None
    regex: str | None = None
    file_list: pathlib.Path | None = None
    sync: bool = False
    sync_delete: bool = False
    index_parts: bool = False
    direct_download: list[str] | None = None
    dry_run: bool = False
    skip_existing: bool = False
    disable_progress_bar: bool = False
    create_file_list: str | None = None
    max_concurrent_requests: int = 15

    def update(self, new_dict: dict) -> "GetRequest":
        filtered_dict = {
            key: value
            for key, value in new_dict.items()
            if value is not None
            and not (isinstance(value, (list, tuple, str)) and not value)
        }
        data = self.model_dump(
            exclude_defaults=True,
            exclude_unset=True,
            exclude_none=True,
        )
        data.update(filtered_dict)
        return self.model_validate(data)

    def enforce_types(self):
        type_enforced_dict = {}
        for key, value in self.__dict__.items():
            if key in [
                "no_directories",
                "sync",
                "sync_delete",
                "dry_run",
                "overwrite",
                "skip_existing",
                "index_parts",
            ]:
                new_value = bool(value) if value is not None else None
            else:
                new_value = str(value) if value else None
            type_enforced_dict[key] = new_value
        self.__dict__.update(type_enforced_dict)

    def from_file(self, filepath: pathlib.Path):
        json_file = json.load(open(filepath))
        json_with_mapped_options = {}
        for key, val in json_file.items():
            if key in MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS:
                new_key = MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS[key]
                json_with_mapped_options[new_key] = val
            else:
                json_with_mapped_options[key] = val
        self.__dict__.update(json_with_mapped_options)
        self.enforce_types()
        full_regex = self.regex
        if self.filter:
            filter_regex = filter_to_regex(self.filter)
            full_regex = overload_regex_with_additional_filter(
                filter_regex, full_regex
            )
        if self.file_list:
            file_list_regex = file_list_to_regex(self.file_list)
            full_regex = overload_regex_with_additional_filter(
                file_list_regex, full_regex
            )
        self.regex = full_regex


def create_get_request(
    dataset_id: str | None,
    dataset_version: str | None,
    dataset_part: str | None,
    username: str | None,
    password: str | None,
    credentials_file: pathlib.Path | None,
    no_directories: bool,
    output_directory: pathlib.Path | None,
    overwrite: bool,
    request_file: pathlib.Path | None,
    filter: str | None,
    regex: str | None,
    file_list: pathlib.Path | None,
    create_file_list: str | None,
    sync: bool,
    sync_delete: bool,
    skip_existing: bool,
    index_parts: bool,
    dry_run: bool,
    max_concurrent_requests: int,
    disable_progress_bar: bool,
) -> GetRequest:
    logger.debug("Checking username and password...")
    username, _ = get_and_check_username_password(
        username, password, credentials_file
    )
    get_request = GetRequest(dataset_id=dataset_id or "", username=username)

    if request_file:
        get_request.from_file(request_file)
    (
        get_request.dataset_id,
        get_request.dataset_version,
    ) = process_dataset_id_and_dataset_version(
        get_request.dataset_id,
        dataset_version or get_request.dataset_version,
    )

    request_update_dict = {
        "dataset_version": dataset_version,
        "output_directory": output_directory,
        "username": username,
        "max_concurrent_requests": max_concurrent_requests,
        "disable_progress_bar": disable_progress_bar,
    }
    # To be able to distinguish between set and unset values
    if dataset_part:
        request_update_dict["dataset_part"] = dataset_part
    if no_directories:
        request_update_dict["no_directories"] = no_directories
    if overwrite:
        request_update_dict["overwrite"] = overwrite
    if skip_existing:
        request_update_dict["skip_existing"] = skip_existing

    if filter:
        get_request.regex = filter_to_regex(filter)
    if regex:
        get_request.regex = overload_regex_with_additional_filter(
            regex, get_request.regex
        )
    if sync or sync_delete:
        request_update_dict["sync"] = True
        if not request_update_dict["dataset_version"]:
            raise ValueError(
                "Sync requires to set a dataset version. "
                "Please use --dataset-version option."
            )
    if sync_delete:
        request_update_dict["sync_delete"] = sync_delete
    if index_parts:
        request_update_dict["index_parts"] = index_parts
        get_request.regex = overload_regex_with_additional_filter(
            filter_to_regex("*index_*"), get_request.regex
        )
    if create_file_list is not None:
        if not (
            create_file_list.endswith(".txt")
            or create_file_list.endswith(".csv")
        ):
            raise ValueError(
                "Download file list must be a '.txt' or '.csv' file. "
                f"Got '{create_file_list}' instead."
            )
        request_update_dict["create_file_list"] = create_file_list
    if file_list:
        direct_download_files = get_direct_download_files(file_list)
        if direct_download_files:
            get_request.direct_download = direct_download_files
    if create_file_list or dry_run:
        request_update_dict["dry_run"] = True

    return get_request.update(request_update_dict)


def filter_to_regex(filter: str) -> str:
    return fnmatch.translate(filter)


def file_list_to_regex(file_list_path: pathlib.Path) -> str:
    pattern = ""
    with open(file_list_path) as file_list:
        pattern = "|".join(map(re.escape, file_list.read().splitlines()))
    return pattern


def overload_regex_with_additional_filter(
    regex: str, filter: str | None
) -> str:
    return "(" + regex + "|" + filter + ")" if filter else regex


def get_direct_download_files(
    file_list_path: pathlib.Path,
) -> list[str]:
    if not os.path.exists(file_list_path):
        raise FileNotFoundError(
            f"File {file_list_path} does not exist."
            " Please provide a valid path to a '.txt' file."
        )
    with open(file_list_path) as f:
        direct_download_files = [line.strip() for line in f.readlines()]
    return direct_download_files


def process_dataset_id_and_dataset_version(
    dataset_id: str,
    user_input_dataset_version: str | None,
) -> tuple[str, str | None]:
    if not dataset_id:
        raise ValueError("Dataset id must be provided.")
    dataset_id, dataset_version, _ = get_version_and_part_from_full_dataset_id(
        dataset_id
    )
    if user_input_dataset_version and dataset_version:
        raise ValueError(
            f"Conflicting dataset versions provided. "
            f"Dataset id '{dataset_id}' contains "
            f"version '{dataset_version}', "
            f"but user input dataset version "
            f"is '{user_input_dataset_version}'. "
            f"Please provide consistent dataset version information."
        )
    elif dataset_version and not user_input_dataset_version:
        logger.warning(
            "The dataset version has been included "
            "in the dataset_id argument. "
            "This is not recommended. "
            "Please either omit the version —allowing "
            "the Toolbox to select it "
            "automatically— or specify it "
            "using the dataset_version argument."
        )
    return dataset_id, dataset_version or user_input_dataset_version
