import fnmatch
import logging
import pathlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from json import load
from typing import Any, Dict, List, Optional

from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_FILE_FORMAT,
    DEFAULT_SUBSET_METHOD,
    FileFormat,
    SubsetMethod,
)
from copernicusmarine.core_functions.utils import datetime_parser
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    TemporalParameters,
)

logger = logging.getLogger("copernicus_marine_root_logger")


MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS: dict[str, str] = {
    "dataset_version": "force_dataset_version",
    "dataset_part": "force_dataset_part",
    "service": "force_service",
}


@dataclass
class DatasetTimeAndGeographicalSubset:
    minimum_longitude: Optional[float] = None
    maximum_longitude: Optional[float] = None
    minimum_latitude: Optional[float] = None
    maximum_latitude: Optional[float] = None
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None


@dataclass
class SubsetRequest:
    dataset_url: Optional[str] = None
    dataset_id: Optional[str] = None
    force_dataset_version: Optional[str] = None
    force_dataset_part: Optional[str] = None
    variables: Optional[List[str]] = None
    minimum_longitude: Optional[float] = None
    maximum_longitude: Optional[float] = None
    minimum_latitude: Optional[float] = None
    maximum_latitude: Optional[float] = None
    minimum_depth: Optional[float] = None
    maximum_depth: Optional[float] = None
    vertical_dimension_as_originally_produced: bool = True
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    subset_method: SubsetMethod = DEFAULT_SUBSET_METHOD
    output_filename: Optional[str] = None
    file_format: FileFormat = DEFAULT_FILE_FORMAT
    force_service: Optional[str] = None
    output_directory: pathlib.Path = pathlib.Path(".")
    force_download: bool = False
    overwrite_output_data: bool = False
    netcdf_compression_enabled: bool = False
    netcdf_compression_level: Optional[int] = None

    def update(self, new_dict: dict):
        """Method to update values in SubsetRequest object.
        Skips "None" values
        """
        for key, value in new_dict.items():
            if value is None or (
                isinstance(value, (list, tuple)) and len(value) < 1
            ):
                pass
            else:
                self.__dict__.update({key: value})

    def enforce_types(self):
        type_enforced_dict = {}
        for key, value in self.__dict__.items():
            if key in [
                "minimum_longitude",
                "maximum_longitude",
                "minimum_latitude",
                "maximum_latitude",
                "minimum_depth",
                "maximum_depth",
            ]:
                new_value = float(value) if value is not None else None
            elif key in [
                "start_datetime",
                "end_datetime",
            ]:
                new_value = datetime_parser(value) if value else None
            elif key in ["force_download"]:
                new_value = (
                    True if value in [True, "true", "True", 1] else False
                )
            elif key in ["variables"]:
                new_value = list(value) if value is not None else None
            elif key in ["output_directory"]:
                new_value = pathlib.Path(value) if value is not None else None
            else:
                new_value = str(value) if value else None
            type_enforced_dict[key] = new_value
        self.__dict__.update(type_enforced_dict)

    def from_file(self, filepath: pathlib.Path):
        self.update(subset_request_from_file(filepath).__dict__)
        return self

    def get_time_and_geographical_subset(
        self,
    ) -> DatasetTimeAndGeographicalSubset:
        return DatasetTimeAndGeographicalSubset(
            minimum_longitude=self.minimum_longitude,
            maximum_longitude=self.maximum_longitude,
            minimum_latitude=self.minimum_latitude,
            maximum_latitude=self.maximum_latitude,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
        )


def subset_request_from_file(filepath: pathlib.Path) -> SubsetRequest:
    json_file = open(filepath)
    json_content = load(json_file)

    json_with_deprecated_options_replace = {}

    for key, val in json_content.items():
        if key in DEPRECATED_OPTIONS:
            deprecated_option = DEPRECATED_OPTIONS[key]
            json_with_deprecated_options_replace[
                deprecated_option.new_name
            ] = val
        elif key in MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS:
            new_key = MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS[key]
            json_with_deprecated_options_replace[new_key] = val
        else:
            json_with_deprecated_options_replace[key] = val

    subset_request = SubsetRequest()
    subset_request.__dict__.update(json_with_deprecated_options_replace)
    subset_request.enforce_types()
    return subset_request


def convert_motu_api_request_to_structure(
    motu_api_request: str,
) -> SubsetRequest:
    prefix = "python -m motuclient "
    string = motu_api_request.replace(prefix, "").replace("'", "")
    arguments = [
        substr.strip() for substr in string.split("--")[1:]
    ]  # for subsubstr in substr.split(" ", maxsplit=1)]
    arg_value_tuples = [
        tuple(substr.split(" ", maxsplit=1)) for substr in arguments
    ]
    motu_api_request_dict: Dict[str, Any] = {}
    for arg, value in arg_value_tuples:
        if arg == "variable":
            # special case for variable, since it can have multiple values
            motu_api_request_dict.setdefault(arg, []).append(value)
        else:
            motu_api_request_dict[arg] = value
    subset_request = SubsetRequest(
        output_directory=pathlib.Path("."),
        force_download=False,
        output_filename=None,
        force_service=None,
    )
    conversion_dict = {
        "product-id": "dataset_id",
        "latitude-min": "minimum_latitude",
        "latitude-max": "maximum_latitude",
        "longitude-min": "minimum_longitude",
        "longitude-max": "maximum_longitude",
        "depth-min": "minimum_depth",
        "depth-max": "maximum_depth",
        "date-min": "start_datetime",
        "date-max": "end_datetime",
        "variable": "variables",
    }
    for key, value in motu_api_request_dict.items():
        if key in conversion_dict.keys():
            subset_request.__dict__.update({conversion_dict[key]: value})
    subset_request.enforce_types()
    return subset_request


@dataclass
class GetRequest:
    dataset_url: Optional[str] = None
    dataset_id: Optional[str] = None
    force_dataset_version: Optional[str] = None
    force_dataset_part: Optional[str] = None
    no_directories: bool = False
    show_outputnames: bool = False
    output_directory: str = "."
    force_download: bool = False
    overwrite_output_data: bool = False
    force_service: Optional[str] = None
    filter: Optional[str] = None
    regex: Optional[str] = None
    file_list: Optional[pathlib.Path] = None
    sync: bool = False
    sync_delete: bool = False
    index_parts: bool = False

    def update(self, new_dict: dict):
        """Method to update values in GetRequest object.
        Skips "None" values
        """
        for key, value in new_dict.items():
            if value is None:
                pass
            else:
                self.__dict__.update({key: value})

    def enforce_types(self):
        type_enforced_dict = {}
        for key, value in self.__dict__.items():
            if key in [
                "no_directories",
                "show_outputnames",
                "force_download",
            ]:
                new_value = bool(value) if value is not None else None
            else:
                new_value = str(value) if value else None
            type_enforced_dict[key] = new_value
        self.__dict__.update(type_enforced_dict)

    def from_file(self, filepath: pathlib.Path):
        self = get_request_from_file(filepath)
        logger.info(filepath)
        return self


def get_request_from_file(filepath: pathlib.Path) -> GetRequest:
    json_file = load(open(filepath))
    json_with_mapped_options = {}
    for key, val in json_file.items():
        if key in MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS:
            new_key = MAPPING_REQUEST_FILES_AND_REQUEST_OPTIONS[key]
            json_with_mapped_options[new_key] = val
        else:
            json_with_mapped_options[key] = val
    get_request = GetRequest()
    get_request.__dict__.update(json_with_mapped_options)
    get_request.enforce_types()
    full_regex = get_request.regex
    if get_request.filter:
        filter_regex = filter_to_regex(get_request.filter)
        full_regex = overload_regex_with_additionnal_filter(
            filter_regex, full_regex
        )
    if get_request.file_list:
        file_list_regex = file_list_to_regex(get_request.file_list)
        full_regex = overload_regex_with_additionnal_filter(
            file_list_regex, full_regex
        )
    get_request.regex = full_regex

    return get_request


@dataclass
class LoadRequest:
    dataset_url: Optional[str] = None
    dataset_id: Optional[str] = None
    force_dataset_version: Optional[str] = None
    force_dataset_part: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    variables: Optional[List[str]] = None
    geographical_parameters: GeographicalParameters = field(
        default_factory=GeographicalParameters
    )
    temporal_parameters: TemporalParameters = field(
        default_factory=TemporalParameters
    )
    depth_parameters: DepthParameters = field(default_factory=DepthParameters)
    subset_method: SubsetMethod = DEFAULT_SUBSET_METHOD
    force_service: Optional[str] = None
    credentials_file: Optional[pathlib.Path] = None
    overwrite_metadata_cache: bool = False
    no_metadata_cache: bool = False

    def get_time_and_geographical_subset(
        self,
    ) -> DatasetTimeAndGeographicalSubset:
        return DatasetTimeAndGeographicalSubset(
            minimum_longitude=self.geographical_parameters.longitude_parameters.minimum_longitude,  # noqa
            maximum_longitude=self.geographical_parameters.longitude_parameters.maximum_longitude,  # noqa
            minimum_latitude=self.geographical_parameters.latitude_parameters.minimum_latitude,  # noqa
            maximum_latitude=self.geographical_parameters.latitude_parameters.maximum_latitude,  # noqa
            start_datetime=self.temporal_parameters.start_datetime,
            end_datetime=self.temporal_parameters.end_datetime,
        )


def filter_to_regex(filter: str) -> str:
    return fnmatch.translate(filter)


def file_list_to_regex(file_list_path: pathlib.Path) -> str:
    pattern = ""
    with open(file_list_path) as file_list:
        pattern = "|".join(map(re.escape, file_list.read().splitlines()))
    return pattern


def overload_regex_with_additionnal_filter(
    regex: str, filter: Optional[str]
) -> str:
    return "(" + regex + "|" + filter + ")" if filter else regex
