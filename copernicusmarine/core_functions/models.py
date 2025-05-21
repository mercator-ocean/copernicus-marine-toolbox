import pathlib
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional, Union, get_args

from pydantic import BaseModel, ConfigDict

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceNames,
    short_name_from_service_name,
)

FileFormat = Literal["netcdf", "zarr", "csv", "parquet"]
DEFAULT_FILE_FORMAT: FileFormat = "netcdf"
DEFAULT_FILE_FORMATS = list(get_args(FileFormat))

FileExtension = Literal[".nc", ".zarr"]
DEFAULT_FILE_EXTENSION: FileExtension = ".nc"
DEFAULT_FILE_EXTENSIONS = list(get_args(FileExtension))

CoordinatesSelectionMethod = Literal[
    "inside", "strict-inside", "nearest", "outside"
]
DEFAULT_COORDINATES_SELECTION_METHOD: CoordinatesSelectionMethod = "inside"
DEFAULT_COORDINATES_SELECTION_METHODS = list(
    get_args(CoordinatesSelectionMethod)
)

VerticalAxis = Literal["depth", "elevation"]
DEFAULT_VERTICAL_AXIS: VerticalAxis = "depth"
DEFAULT_VERTICAL_AXES = list(get_args(VerticalAxis))

GeoSpatialProjection = Literal["lonlat", "originalGrid"]
DEFAULT_GEOSPATIAL_PROJECTION: GeoSpatialProjection = "lonlat"


class ChunkType(str, Enum):
    ARITHMETIC = "default"
    GEOMETRIC = "symmetricGeometric"


class StatusCode(str, Enum):
    """
    Enumeration of the possible of a request.
    Only concerns ``get`` and ``subset`` functions.
    """

    SUCCESS = "000"
    DRY_RUN = "001"
    FILE_LIST_CREATED = "002"
    NO_DATA_TO_DOWNLOAD = "003"
    ERROR = "100"


class StatusMessage(str, Enum):
    """
    Enumeration of the possible messages of a request.
    Only concerns ``get`` and ``subset`` functions.
    """

    SUCCESS = "The request was successful."
    DRY_RUN = "The request was run with the dry-run option. No data was downloaded."  # noqa: E501
    FILE_LIST_CREATED = "The request created a file list and then stopped."
    NO_DATA_TO_DOWNLOAD = "No data to download from the remote server corresponding to your request."  # noqa: E501
    ERROR = "An error occurred during the request."


class FileStatus(str, Enum):
    """
    Gives an indication of how the file was handled.

    This depends on the option passed to the query.
    """

    #: The file has been downloaded successfully.
    DOWNLOADED = "DOWNLOADED"
    #: The file has been ignored. Probably because it already exists.
    IGNORED = "IGNORED"
    #: The file has been overwritten and downloaded.
    OVERWRITTEN = "OVERWRITTEN"

    @classmethod
    def get_status(cls, ignore: bool, overwrite: bool) -> "FileStatus":
        if ignore:
            return FileStatus.IGNORED
        if overwrite:
            return FileStatus.OVERWRITTEN
        return FileStatus.DOWNLOADED


class FileGet(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
    #: Full url of the location of the file on remote server.
    s3_url: str
    #: Location of the file on the remote server, https url.
    https_url: str
    #: Size of the file in MB.
    file_size: float
    #: Last modified date.
    last_modified_datetime: str
    #: ETag of the file.
    etag: str
    #: File format.
    file_format: str
    #: Output directory where the file is stored.
    output_directory: pathlib.Path
    #: File name.
    filename: str
    #: Relative path to the file.
    file_path: pathlib.Path
    #: Status of the file.
    file_status: FileStatus


class ResponseGet(BaseModel):
    """Metadata returned when using :func:`~copernicusmarine.get`"""

    model_config = ConfigDict(use_enum_values=True)

    #: Description of the files concerned by the query.
    files: list[FileGet]
    #: List of deleted files. Only if option ``sync-delete`` is passed.
    files_deleted: Optional[list[str]]
    #: List of not found files from the file list input.
    files_not_found: Optional[list[str]]
    #: Number of files to be downloaded.
    number_of_files_to_download: int
    #: Total size of the files that would be downloaded.
    total_size: Optional[float]
    #: Status of the request.
    status: StatusCode
    #: Message explaning the status.
    message: StatusMessage


class S3FileInfo(BaseModel):
    """
    Basic information about a file in the bucket.
    """

    model_config = ConfigDict(use_enum_values=True)

    filename_in: str
    filename_out: pathlib.Path = pathlib.Path()
    size: float
    last_modified: str
    etag: str
    ignore: bool = False
    overwrite: bool = False


class S3FilesDescriptor(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    endpoint: str = ""
    bucket: str = ""
    s3_files: list[S3FileInfo] = []
    total_size: float = 0.0
    files_not_found: list[str] = []
    files_to_delete: list[pathlib.Path] = []
    create_file_list: bool = False

    def add_s3_file(self, s3_file: S3FileInfo):
        self.s3_files.append(s3_file)
        if not s3_file.ignore:
            self.total_size += s3_file.size


class GeographicalExtent(BaseModel):
    """Interval for geographical coordinates."""

    minimum: float
    maximum: float
    unit: Optional[str]
    coordinate_id: str


class TimeExtent(BaseModel):
    """Interval for time coordinates."""

    minimum: str
    maximum: str
    unit: str
    coordinate_id: str


class ResponseSubset(BaseModel):
    """Metadata returned when using :func:`~copernicusmarine.subset`"""

    model_config = ConfigDict(use_enum_values=True)

    #: Relative path to the file.
    file_path: pathlib.Path
    #: Output directory where the file stored.
    output_directory: pathlib.Path
    #: File name.
    filename: str
    #: Estimation of the size of the final result file in MB.
    #: This estimation may not be accurate if you save the result as
    #: a compressed NetCDF file.
    file_size: Optional[float]
    #: Estimation of the maximum amount of data needed to
    #: get the final result in MB.
    data_transfer_size: Optional[float]
    #: Variables of the subsetted dataset.
    variables: list[str]
    #: The bounds of the subsetted dataset.
    coordinates_extent: list[Union[GeographicalExtent, TimeExtent]]
    #: Status of the request.
    status: StatusCode
    #: Message explaning the status.
    message: StatusMessage
    #: Status of the files.
    file_status: FileStatus


# Internal use only
@dataclass
class VariableChunking:
    variable_short_name: str
    number_values: float
    number_chunks: int
    chunk_size: float


@dataclass
class CoordinateChunking:
    coordinate_id: str
    chunking_length: float
    number_of_chunks: int


@dataclass
class DatasetChunking:
    number_chunks: int
    chunking_per_variable: dict[str, VariableChunking]
    chunking_per_coordinate: dict[str, CoordinateChunking]

    def get_number_values_variable(self, variable_short_name: str) -> float:
        if variable_short_name in self.chunking_per_variable:
            return self.chunking_per_variable[
                variable_short_name
            ].number_values
        return 0

    def get_number_chunks_coordinate(
        self, coordinate_id: str
    ) -> Optional[float]:
        if coordinate_id in self.chunking_per_coordinate:
            return self.chunking_per_coordinate[coordinate_id].number_of_chunks
        return None


class _Command(Enum):
    GET = "get"
    SUBSET = "subset"
    OPEN_DATASET = "open_dataset"
    READ_DATAFRAME = "read_dataframe"


@dataclass(frozen=True)
class Command:
    command_name: _Command
    service_names_by_priority: list[CopernicusMarineServiceNames]

    def service_names(self) -> list[str]:
        return [
            service_name.value
            for service_name in self.service_names_by_priority
        ]

    def short_names_services(self) -> list[str]:
        return [
            short_name_from_service_name(service_name).value
            for service_name in self.service_names_by_priority
        ]

    def get_available_service_for_command(self) -> list[str]:
        available_services = []
        for service_name in self.service_names_by_priority:
            available_services.append(service_name.value)
            short_name = short_name_from_service_name(service_name)
            if short_name != service_name:
                available_services.append(
                    short_name_from_service_name(service_name).value
                )
        return available_services


class CommandType(Command, Enum):
    SUBSET = (
        _Command.SUBSET,
        [
            CopernicusMarineServiceNames.GEOSERIES,
            CopernicusMarineServiceNames.TIMESERIES,
            CopernicusMarineServiceNames.OMI_ARCO,
            CopernicusMarineServiceNames.STATIC_ARCO,
            CopernicusMarineServiceNames.PLATFORMSERIES,
        ],
    )
    GET = (
        _Command.GET,
        [
            CopernicusMarineServiceNames.FILES,
        ],
    )
    OPEN_DATASET = (
        _Command.OPEN_DATASET,
        [
            CopernicusMarineServiceNames.GEOSERIES,
            CopernicusMarineServiceNames.TIMESERIES,
            CopernicusMarineServiceNames.OMI_ARCO,
            CopernicusMarineServiceNames.STATIC_ARCO,
        ],
    )
    READ_DATAFRAME = (
        _Command.READ_DATAFRAME,
        [
            CopernicusMarineServiceNames.GEOSERIES,
            CopernicusMarineServiceNames.TIMESERIES,
            CopernicusMarineServiceNames.OMI_ARCO,
            CopernicusMarineServiceNames.STATIC_ARCO,
            CopernicusMarineServiceNames.PLATFORMSERIES,
        ],
    )
