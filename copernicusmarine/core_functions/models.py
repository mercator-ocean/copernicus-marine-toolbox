from typing import Literal, get_args

FileFormat = Literal["netcdf", "zarr"]
DEFAULT_FILE_FORMAT: FileFormat = "netcdf"
DEFAULT_FILE_FORMATS = list(get_args(FileFormat))

FileExtension = Literal[".nc", ".zarr"]
DEFAULT_FILE_EXTENSION: FileExtension = ".nc"
DEFAULT_FILE_EXTENSIONS = list(get_args(FileExtension))

SubsetMethod = Literal["nearest", "strict"]
DEFAULT_SUBSET_METHOD: SubsetMethod = "nearest"
DEFAULT_SUBSET_METHODS = list(get_args(SubsetMethod))
