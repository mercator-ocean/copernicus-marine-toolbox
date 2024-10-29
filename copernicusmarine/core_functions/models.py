import pathlib
from enum import Enum
from typing import Literal, Optional, get_args

from pydantic import BaseModel, ConfigDict

FileFormat = Literal["netcdf", "zarr"]
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

VerticalDimensionOutput = Literal["depth", "elevation"]
DEFAULT_VERTICAL_DIMENSION_OUTPUT: VerticalDimensionOutput = "depth"
DEFAULT_VERTICAL_DIMENSION_OUTPUTS = list(get_args(VerticalDimensionOutput))


# class Status(BaseModel):
#     """Indicate the status of a request."""

#     #: Status of the request.
#     status: Literal["SUCCESS", "DRY_RUN", "ERROR", "NO_DATA_TO_DOWNLOAD"]
#     #: Message explaning the status.
#     message: Literal[
#         "The request was successful.",
#         "The request was successful but no data was transferred.",
#         "An error occurred during the request.",
#         "No data to download from the remote server"
#         " corresponding to your request.",
#     ]


class StatusCode(str, Enum):
    """
    Enumeration of the possible of a request.
    Only concerns ``get`` and ``subset`` functions.
    """

    SUCCESS = "SUCCESS"
    DRY_RUN = "DRY_RUN"
    ERROR = "ERROR"
    NO_DATA_TO_DOWNLOAD = "NO_DATA_TO_DOWNLOAD"
    FILE_LIST_CREATED = "FILE_LIST_CREATED"


class StatusMessage(str, Enum):
    """
    Enumeration of the possible messages of a request.
    Only concerns ``get`` and ``subset`` functions.
    """

    SUCCESS = "The request was successful."
    DRY_RUN = "The request was successful but no data was transferred."
    ERROR = "An error occurred during the request."
    NO_DATA_TO_DOWNLOAD = "No data to download from the remote server corresponding to your request."  # noqa: E501
    FILE_LIST_CREATED = "The request created a file list and then stopped."


class FileGet(BaseModel):
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
    #: Output directory where the file stored.
    output_directory: pathlib.Path
    #: File name.
    filename: str
    #: Path to the file.
    file_path: pathlib.Path


class ResponseGet(BaseModel):
    """Metadata returned when using :func:`~copernicusmarine.get`"""

    model_config = ConfigDict(use_enum_values=True)

    #: Description of the files concerned by the query
    files: list[FileGet]
    #: Total size of the files in MB.
    total_size: Optional[float]
    #: status of the request.
    status: StatusCode
    #: Message explaning the status.
    message: StatusMessage


class GeographicalExtent(BaseModel):
    """Interval for geographical coordinates."""

    minimum: Optional[float]
    maximum: Optional[float]
    unit: Optional[str]


class TimeExtent(BaseModel):
    """Interval for time coordinates."""

    minimum: Optional[str]
    maximum: Optional[str]
    unit: Optional[str]


class DatasetCoordinatesExtent(BaseModel):
    #: Longitude interval of the subsetted data.
    longitude: Optional[GeographicalExtent]
    #: Latitude interval of the subsetted data.
    latitude: Optional[GeographicalExtent]
    #: Time interval of the subsetted data in iso8601 string
    time: Optional[TimeExtent]
    #: Depth interval of the subsetted data.
    depth: Optional[GeographicalExtent] = None
    #: Elevation interval of the subsetted data.
    #: Is relevant if data are requested for elevation
    #: instead of depth
    elevation: Optional[GeographicalExtent] = None


class ResponseSubset(BaseModel):
    """Metadata returned when using :func:`~copernicusmarine.subset`"""

    model_config = ConfigDict(use_enum_values=True)

    #: Path to the result file.
    file_path: pathlib.Path
    #: Output directory where the file stored.
    output_directory: pathlib.Path
    #: File name.
    filename: str
    #: Estimation of the size of the final result file in MB.
    file_size: Optional[float]
    #: Estimation of the maximum amount of data needed to
    #: get the final result in MB.
    data_transfer_size: Optional[float]
    #: The bounds of the subsetted dataset.
    coordinates_extent: DatasetCoordinatesExtent
    #: status of the request.
    status: StatusCode
    #: Message explaning the status.
    message: StatusMessage
