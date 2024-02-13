"""
.
"""

from importlib.metadata import version
import pathlib

import json
import logging.config
import time

__version__ = version("copernicusmarine")

log_configuration_dict = json.load(
    open(
        pathlib.Path(
            pathlib.Path(__file__).parent, "logging_conf.json"
        )
    )
)
logging.config.dictConfig(log_configuration_dict)
logging.Formatter.converter = time.gmtime

from copernicusmarine.python_interface.login import login
from copernicusmarine.python_interface.describe import describe
from copernicusmarine.python_interface.get import get
from copernicusmarine.python_interface.subset import subset
from copernicusmarine.python_interface.open_dataset import open_dataset
from copernicusmarine.python_interface.open_dataset import load_xarray_dataset  # depracated
from copernicusmarine.python_interface.read_dataframe import read_dataframe
from copernicusmarine.python_interface.read_dataframe import load_pandas_dataframe  # depracated
