from copernicusmarine.core_functions.models import CommandType

SHARED: dict[str, str] = {
    "OVERWRITE_HELP": "If specified and if the file already exists on destination, then it will be overwritten. By default, the toolbox creates a new file with a new index (eg 'filename_(1).nc').",  # noqa: E501
    "USERNAME_HELP": (
        "If not set, search for environment variable COPERNICUSMARINE_SERVICE_USERNAME, then search for a credentials file, else ask for user input."  # noqa
    ),  # a little hardcoding in Python API
    "PASSWORD_HELP": (
        "If not set, search for environment variable COPERNICUSMARINE_SERVICE_PASSWORD, then search for a credentials file, else ask for user input."  # noqa
    ),  # a little hardcoding in Python API
    "LOG_LEVEL_HELP": (
        "Set the details printed to console by the command "
        "(based on standard logging library)."
    ),
    "CREATE_TEMPLATE_HELP": (
        "Option to create a file <argument>_template.json in your current directory "
        "containing the arguments. If specified, no other action will be performed."
    ),
    "CREDENTIALS_FILE_HELP": (
        "Path to a credentials file if not in its default directory"
        " (``$HOME/.copernicusmarine``). Accepts "
        ".copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini "
        "files."
    ),
    "DATASET_VERSION_HELP": "Force the selection of a specific dataset version.",
    "DATASET_PART_HELP": "Force the selection of a specific dataset part.",
    "DATASET_ID_HELP": (
        "The datasetID, required either as an argument or in the request_file option."
    ),
    "DISABLE_PROGRESS_BAR_HELP": "Flag to hide progress bar.",
    "DRY_RUN_HELP": "If True, runs query without downloading data.",
    "RESPONSE_FIELDS_HELP": (
        "List of fields to include in the query metadata. "
        "The fields are separated by a comma. "
        "To return all fields, use 'all'."
    ),
    "OUTPUT_DIRECTORY_HELP": (
        "The destination folder for the downloaded files. Default is the current "
        "directory."
    ),
    "REQUEST_FILE_HELP": (
        "Option to pass a file containing the arguments. For more information "
        "please refer to the documentation or use option ``--create-template`` "
        "from the command line interface for an example template."
    ),
    "SKIP_EXISTING_HELP": (
        "If the files already exists where it would be downloaded, then "
        "the download is skipped for this file. By default, the toolbox "
        "creates a new file with a new index (eg 'filename_(1).nc')."
    ),
}

LOGIN: dict[str, str] = {
    "LOGIN_DESCRIPTION_HELP": (
        "Create a configuration file with your Copernicus Marine credentials"
        " under the ``$HOME/.copernicusmarine`` directory."
    ),
    "LOGIN_RESPONSE_HELP": (
        "Exit code\n 0 if the login was successfully completed, 1 otherwise."
    ),
    "USERNAME_HELP": (
        "If not set, search for environment variable COPERNICUSMARINE_SERVICE_USERNAME, else ask for user input."  # noqa
    ),
    "PASSWORD_HELP": (
        "If not set, search for environment variable COPERNICUSMARINE_SERVICE_PASSWORD, else ask for user input."  # noqa
    ),
    "CONFIGURATION_FILE_DIRECTORY_HELP": (
        "Path to the directory where the configuration file will be stored."
    ),
    "FORCE_OVERWRITE_HELP": (
        "Flag to skip confirmation before overwriting configuration file."
    ),
    "CHECK_CREDENTIALS_VALID_HELP": (
        "Flag to check if the credentials are valid. "
        "No other action will be performed. "
        "The validity will be check in this order: "
        "1. Check if the credentials are valid"
        " with the provided username and password. "
        "2. Check if the credentials are valid in the environment variables. "
        "3. Check if the credentials are valid in the configuration file. "
        "When any is found (valid or not valid), will return immediately."
    ),
    "CREDENTIALS_FILE_HELP": (
        "Path to a credentials file if not in its default directory"
        " (``$HOME/.copernicusmarine``). Accepts "
        ".copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini "
        "files. Will only be taken into account when checking the credentials validity."
    ),
}

DESCRIBE: dict[str, str] = {
    "DESCRIBE_DESCRIPTION_HELP": (
        "Retrieve and parse the metadata information "
        "from the Copernicus Marine catalogue."
    ),
    "DESCRIBE_RESPONSE_HELP": (
        "JSON\n A dictionary containing the retrieved metadata information."
    ),
    "SHOW_ALL_VERSIONS_HELP": (
        "Include dataset versions in output. By default, shows only the default "
        "version."
    ),
    "RETURN_FIELDS_HELP": (
        "Option to specify the fields to return in the output. "
        "The fields are separated by a comma. You can use 'all' to return all fields."
    ),
    "EXCLUDE_FIELDS_HELP": (
        "Option to specify the fields to exclude from the output. "
        "The fields are separated by a comma."
    ),
    "CONTAINS_HELP": (
        "Filter catalogue output. Returns products with attributes matching a string "
        "token."
    ),
    "PRODUCT_ID_HELP": (
        "Force the productID to be used for the describe command. Will not parse the "
        "whole catalogue, but only the product with the given productID."
    ),
    "DATASET_ID_HELP": (
        "Force the datasetID to be used for the describe command. Will not "
        "parse the whole catalogue, but only the dataset with the given datasetID."
    ),
    "MAX_CONCURRENT_REQUESTS_HELP": (
        "Maximum number of concurrent requests (>=1). Default 15. The command uses "
        "a thread pool executor to manage concurrent requests."
    ),
}

SUBSET: dict[str, str] = {
    "SUBSET_DESCRIPTION_HELP": (
        "Extract a subset of data from a specified dataset using given parameters."
        "\n\nThe datasetID is required and can be found via the ``describe`` "
        "command. "  # has some hardcoding in CLI and python API
    ),
    "SUBSET_RESPONSE_HELP": (
        "JSON \n A description of the downloaded data and its destination."
    ),
    "SERVICE_HELP": (
        f"Force download through one of the available services using the service name "
        f"among {CommandType.SUBSET.service_names()} or "
        f"its short name among {CommandType.SUBSET.short_names_services()}."
    ),
    "VARIABLES_HELP": "Specify dataset variable. Can be used multiple times.",
    "MINIMUM_LONGITUDE_HELP": (
        "Minimum longitude for the subset. The value will be transposed "
        "to the interval [-180; 360[."
    ),
    "MINIMUM_X_HELP": (
        "Minimum x-axis value for the subset. "
        "The units are considered in length (m, 100km...)."
    ),
    "ALIAS_MIN_X_HELP": (
        "Alias for ``--minimum-longitude`` and ``--minimum-x``."
    ),
    "MAXIMUM_LONGITUDE_HELP": (
        "Maximum longitude for the subset. The value will be transposed"
        " to the interval [-180; 360[."
    ),
    "ALIAS_MAX_X_HELP": (
        "Alias for ``--maximum-longitude`` and ``--maximum-x``."
    ),
    "MAXIMUM_X_HELP": (
        "Maximum x-axis value for the subset. "
        "The units are considered in length (m, 100km...)."
    ),
    "MINIMUM_LATITUDE_HELP": (
        "Minimum latitude for the subset. Requires a float from -90 "
        "degrees to +90."
    ),
    "ALIAS_MIN_Y_HELP": (
        "Alias for ``--minimum-latitude`` and ``--minimum-y``."
    ),
    "MINIMUM_Y_HELP": (
        "Minimum y-axis value for the subset. "
        "The units are considered in length (m, 100km...)."
    ),
    "MAXIMUM_LATITUDE_HELP": (
        "Maximum latitude for the subset. Requires a float from -90 degrees "
        "to +90."
    ),
    "ALIAS_MAX_Y_HELP": (
        "Alias for ``--maximum-latitude`` and ``--maximum-y``."
    ),
    "MAXIMUM_Y_HELP": (
        "Maximum y-axis value for the subset. "
        "The units are considered in length (m, 100km...)."
    ),
    "MINIMUM_DEPTH_HELP": (
        "Minimum depth for the subset. Requires a positive float (or 0)."
    ),
    "MAXIMUM_DEPTH_HELP": (
        "Maximum depth for the subset. Requires a positive float (or 0)."
    ),
    "VERTICAL_AXIS_HELP": (
        "Consolidate the vertical dimension (the z-axis) as requested: depth with "
        "descending positive values, elevation with ascending positive values. "
        "Default is depth."
    ),
    "START_DATETIME_HELP": (
        "The start datetime of the temporal subset. Supports common "
        "format parsed by dateutil (https://dateutil.readthedocs.io/en/stable/parser.html)."  # noqa
    ),  # hardocded in cli: Caution: encapsulate date with “ “ to ensure valid
    # expression for format “%Y-%m-%d %H:%M:%S”.
    "END_DATETIME_HELP": (
        "The end datetime of the temporal subset. Supports common "
        "format parsed by dateutil (https://dateutil.readthedocs.io/en/stable/parser.html)."  # noqa
    ),  # hardocded in cli: Caution: encapsulate date with “ “
    # to ensure valid expression for format “%Y-%m-%d %H:%M:%S”.
    "PLATFORM_IDS_HELP": (
        "Specify platform ID. Can be used multiple times. "
        "Only available for platform chunked datasets."
    ),
    "COORDINATES_SELECTION_METHOD_HELP": (
        "If ``inside``, the "
        "selection retrieved will be inside the requested range. If ``strict-"
        "inside``, the selection retrieved will be inside the requested range, "
        "and an error will be raised if the values don't exist. "
        "If ``nearest``, the extremes closest to the requested values will "
        "be returned. If ``outside``,"
        " the extremes will be taken to contain all the requested interval."
        " The methods ``inside``, ``nearest`` and ``outside`` will display"
        " a warning if the request is out of bounds."
    ),
    "OUTPUT_FILENAME_HELP": (
        "Save the downloaded data with the given file name (under the output "
        "directory)."
    ),
    "FILE_FORMAT_HELP": "Format of the downloaded dataset. Default to NetCDF '.nc'.",
    "MOTU_API_REQUEST_HELP": (
        "Option to pass a complete MOTU API request as a string. Caution, user has to "
        """replace double quotes " with single quotes ' in the request."""
    ),
    "NETCDF_COMPRESSION_LEVEL_HELP": (
        "Specify a compression level to apply on the NetCDF output file. A value of 0 "
        "means no compression, and 9 is the highest level of compression available."
    ),  # some hardcoding in CLI to add the flag value
    "NETCDF3_COMPATIBLE_HELP": (
        "Enable downloading the dataset in a netCDF3 compatible format."
    ),
    "CHUNK_SIZE_LIMIT_HELP": (
        "Limit the size of the chunks in the dask array. Default is set to -1 which "
        "behaves similarly to 'chunks=auto' from ``xarray``. Positive integer"
        " values and '-1' are accepted. This is an experimental feature."
    ),
    "RAISE_IF_UPDATING_HELP": (
        "If set, raises a :class:`copernicusmarine.DatasetUpdating` "
        "error if the dataset is being updated "
        "and the subset interval requested overpasses "
        "the updating start date of the dataset."
        " Otherwise, a simple warning is displayed."
    ),
}

GET: dict[str, str] = {
    "GET_DESCRIPTION_HELP": (
        "Download originally produced data files.\n\n"
        "The datasetID is required (either as an "
        "argument or in a request file) and can be found via the ``describe``"
        " command."
    ),  # has some hardcoding in CLI
    "MAX_CONCURRENT_REQUESTS_HELP": (
        "Maximum number of concurrent requests. Default 15. The command uses a thread "
        "pool executor to manage concurrent requests. If set to 0, no parallel"
        " executions are used."
    ),
    "GET_RESPONSE_HELP": (
        "JSON \n A list of files that were downloaded and some metadata."
    ),
    "FILTER_HELP": (
        "A pattern that must match the absolute paths of the files to download."
    ),
    "REGEX_HELP": (
        "The regular expression that must match the absolute paths of the files to "
        "download."
    ),
    "FILE_LIST_HELP": (
        "Path to a '.txt' file containing a "
        "list of file paths, line by line, that will "
        "be downloaded directly. These files must be from the same dataset as the one s"
        "pecified dataset with the datasetID option. If no files can be found, the "
        "Toolbox will list all files on the remote server and attempt to find a match."
    ),
    "CREATE_FILE_LIST_HELP": (
        "Option to only create a file containing the names of the targeted files "
        "instead of downloading them. It writes the file to the specified output"
        " directory (default to current directory). The file "
        "name specified should end with '.txt' or '.csv'. If specified, no other "
        "action will be performed."
    ),
    "SYNC_HELP": (
        "Option to synchronize the local directory with the remote directory. See the "
        "documentation for more details."
    ),
    "SYNC_DELETE_HELP": (
        "Option to delete local files that are not present on the remote server while "
        "applying sync."
    ),
    "INDEX_PARTS_HELP": (
        "Option to get the index files of an INSITU dataset."
    ),
    "NO_DIRECTORIES_HELP": (
        "If True, downloaded files will not be organized into directories."
    ),
}


SUBSET.update(SHARED)
GET.update(SHARED)
LOGIN.update({k: v for k, v in SHARED.items() if k not in LOGIN})
DESCRIBE.update({k: v for k, v in SHARED.items() if k not in DESCRIBE})
