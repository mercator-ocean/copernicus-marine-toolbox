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

VerticalAxis = Literal["depth", "elevation"]
DEFAULT_VERTICAL_AXIS: VerticalAxis = "depth"
DEFAULT_VERTICAL_AXES = list(get_args(VerticalAxis))


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
    #: Time interval of the subsetted data in iso8601 string.
    time: Optional[TimeExtent]
    #: Depth interval of the subsetted data.
    depth: Optional[GeographicalExtent] = None
    #: Elevation interval of the subsetted data.
    #: Is relevant if data are requested for elevation
    #: instead of depth.
    elevation: Optional[GeographicalExtent] = None


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
    file_size: Optional[float]
    #: Estimation of the maximum amount of data needed to
    #: get the final result in MB.
    data_transfer_size: Optional[float]
    #: Variables of the subsetted dataset.
    variables: list[str]
    #: The bounds of the subsetted dataset.
    coordinates_extent: DatasetCoordinatesExtent
    #: Status of the request.
    status: StatusCode
    #: Message explaning the status.
    message: StatusMessage
    #: Status of the files.
    file_status: FileStatus
