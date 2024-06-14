"""
.
"""

import logging.config
import time
from importlib.metadata import version

from copernicusmarine.logging_conf import logging_configuration_dict

__version__ = version("copernicusmarine")

logging.config.dictConfig(logging_configuration_dict)
logging.Formatter.converter = time.gmtime

from copernicusmarine.python_interface.describe import describe
from copernicusmarine.python_interface.get import get
from copernicusmarine.python_interface.login import login
from copernicusmarine.python_interface.open_dataset import (
    load_xarray_dataset,  # depracated
    open_dataset,
)
from copernicusmarine.python_interface.read_dataframe import (
    load_pandas_dataframe,  # depracated
    read_dataframe,
)
from copernicusmarine.python_interface.subset import subset
