import copernicusmarine.logger as logger
from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCatalogue,
    CopernicusMarineCoordinate,
    CopernicusMarineDataset,
    CopernicusMarinePart,
    CopernicusMarineProduct,
    CopernicusMarineService,
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
    CopernicusMarineVariable,
    CopernicusMarineVersion,
    CoperniusMarineServiceShortNames,
    DatasetNotFound,
    DatasetVersionNotFound,
    DatasetVersionPartNotFound,
    ServiceNotHandled,
)
from copernicusmarine.command_line_interface.utils import (
    OtherOptionsPassedWithCreateTemplate,
)
from copernicusmarine.core_functions.credentials_utils import (
    CouldNotConnectToAuthenticationSystem,
    CredentialsCannotBeNone,
    InvalidUsernameOrPassword,
)
from copernicusmarine.core_functions.exceptions import (
    CoordinatesOutOfDatasetBounds,
    FormatNotSupported,
    GeospatialSubsetNotAvailableForNonLatLon,
    MinimumLongitudeGreaterThanMaximumLongitude,
    NetCDFCompressionNotAvailable,
    ServiceNotSupported,
    VariableDoesNotExistInTheDataset,
    WrongDatetimeFormat,
)
from copernicusmarine.core_functions.fields_query_builder import (
    WrongFieldsError,
)
from copernicusmarine.core_functions.models import (
    FileGet,
    FileStatus,
    GeographicalExtent,
    ResponseGet,
    ResponseSubset,
    StatusCode,
    StatusMessage,
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
from copernicusmarine.versioner import __version__

__all__ = [
    "CoordinatesOutOfDatasetBounds",
    "CopernicusMarineCatalogue",
    "CopernicusMarineCoordinate",
    "CopernicusMarineDataset",
    "CopernicusMarinePart",
    "CopernicusMarineProduct",
    "CopernicusMarineService",
    "CopernicusMarineServiceFormat",
    "CopernicusMarineServiceNames",
    "CopernicusMarineVariable",
    "CopernicusMarineVersion",
    "CoperniusMarineServiceShortNames",
    "CouldNotConnectToAuthenticationSystem",
    "CredentialsCannotBeNone",
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
    "GeospatialSubsetNotAvailableForNonLatLon",
    "StatusCode",
    "FileStatus",
    "StatusMessage",
    "TimeExtent",
    "VariableDoesNotExistInTheDataset",
    "WrongDatetimeFormat",
    "WrongFieldsError",
    "__version__",
    "describe",
    "get",
    "logger",
    "login",
    "open_dataset",
    "read_dataframe",
    "subset",
]
