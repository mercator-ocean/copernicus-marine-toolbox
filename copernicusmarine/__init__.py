import copernicusmarine._init_logging  # noqa
from copernicusmarine._version import __version__
from copernicusmarine.catalogue_parser.models import (
    DatasetNotFound,
    DatasetVersionNotFound,
    DatasetVersionPartNotFound,
    ServiceNotHandled,
)
from copernicusmarine.command_line_interface.utils import (
    OtherOptionsPassedWithCreateTemplate,
)
from copernicusmarine.core_functions.credentials_utils import (
    CredentialsCannotBeNone,
    InvalidUsernameOrPassword,
)
from copernicusmarine.core_functions.exceptions import (
    CoordinatesOutOfDatasetBounds,
    FormatNotSupported,
    MinimumLongitudeGreaterThanMaximumLongitude,
    NetCDFCompressionNotAvailable,
    ServiceNotSupported,
    VariableDoesNotExistInTheDataset,
    WrongDatetimeFormat,
)
from copernicusmarine.core_functions.models import (
    DatasetCoordinatesExtent,
    FileGet,
    GeographicalExtent,
    ResponseGet,
    ResponseSubset,
    TimeExtent,
)
from copernicusmarine.core_functions.services_utils import (
    NoServiceAvailable,
    ServiceDoesNotExistForCommand,
    ServiceNotAvailable,
)
from copernicusmarine.python_interface.describe import describe
from copernicusmarine.python_interface.get import get
from copernicusmarine.python_interface.login import login
from copernicusmarine.python_interface.open_dataset import (
    open_dataset,
)
from copernicusmarine.python_interface.read_dataframe import (
    read_dataframe,
)
from copernicusmarine.python_interface.subset import subset

__all__ = [
    "__version__",
    "CoordinatesOutOfDatasetBounds",
    "CredentialsCannotBeNone",
    "DatasetCoordinatesExtent",
    "DatasetNotFound",
    "DatasetVersionNotFound",
    "DatasetVersionPartNotFound",
    "FileGet",
    "FormatNotSupported",
    "GeographicalExtent",
    "InvalidUsernameOrPassword",
    "MinimumLongitudeGreaterThanMaximumLongitude",
    "NetCDFCompressionNotAvailable",
    "NoServiceAvailable",
    "OtherOptionsPassedWithCreateTemplate",
    "ResponseGet",
    "ResponseSubset",
    "ServiceDoesNotExistForCommand",
    "ServiceNotAvailable",
    "ServiceNotHandled",
    "ServiceNotSupported",
    "TimeExtent",
    "VariableDoesNotExistInTheDataset",
    "WrongDatetimeFormat",
    "describe",
    "get",
    "login",
    "open_dataset",
    "read_dataframe",
    "subset",
]
