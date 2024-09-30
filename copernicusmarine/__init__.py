import logging.config
import time
from importlib.metadata import version

from copernicusmarine.logging_conf import logging_configuration_dict

__version__ = version("copernicusmarine")

logging.config.dictConfig(logging_configuration_dict)
logging.Formatter.converter = time.gmtime

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
