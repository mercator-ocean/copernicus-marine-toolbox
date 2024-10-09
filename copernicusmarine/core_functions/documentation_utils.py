SHARED = {
    "OVERWRITE_SHORT_OPTION": "--overwrite",
    "OVERWRITE_LONG_OPTION": "--overwrite-output-data",
    "OVERWRITE_OPTION_HELP_TEXT": (
        "If specified and if the file already exists on destination, then it will be "
        "overwritten instead of creating new one with unique index."
    ),
    "USERNAME_HELP": (
        "The username for authentication."
    ),  # a little hardcoding in Python API
    "PASSWORD_HELP": (
        "The password for authentication."
    ),  # a little hardcoding in Python API
    "LOG_LEVEL_HELP": (
        "Set the details printed to console by the command "
        "(based on standard logging library)."
    ),
    "OVERWRITE_OUTPUT_DATA_HELP": (
        "If specified and if the file already exists on destination, then it will"
        " be overwritten instead of creating a new one with unique index."
    ),
    "CREATE_TEMPLATE_HELP": (
        "Option to create a file <argument>_template.json in your current directory "
        "containing the arguments. If specified, no other action will be performed."
    ),
    "CREDENTIALS_FILE_HELP": (
        "Path to a credentials file if not in its default directory. Accepts "
        ".copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini "
        "files."
    ),
    "SERVICE_HELP": (
        "Force download through one of the available services using the service name "
        "among [‘arco-geo-series’, ‘arco-time-series’, ‘omi-arco’, ‘static-arco’] or "
        "its short name among [‘geoseries’, ‘timeseries’, ‘omi-arco’, ‘static-arco’]."
    ),
    "DATASET_VERSION_HELP": "Force the selection of a specific dataset version.",
    "DATASET_PART_HELP": "Force the selection of a specific dataset part.",
    "DATASET_ID_HELP": "The datasetID.",
    "DISABLE_PROGRESS_BAR_HELP": "Flag to hide progress bar.",
    "FORCE_DOWNLOAD_HELP": "Flag to skip confirmation before download.",
    "DRY_RUN_HELP": "Runs query without downloading data.",
    "MAX_CONCURRENT_REQUESTS_HELP": (
        "Maximum number of concurrent requests. Default 15. The command uses a thread "
        "pool executor to manage concurrent requests."
    ),
    "OUTPUT_DIRECTORY_HELP": (
        "The destination folder for the downloaded files. Default is the current "
        "directory."
    ),
}

LOGIN = {
    "LOGIN_DESCRIPTION_HELP": (
        "Create a configuration file with your Copernicus Marine credentials"
        " under the ``$HOME/.copernicusmarine`` directory"
        " (overwritable with the ``--credentials-file`` option)."
    ),
    "CONFIGURATION_FILE_DIRECTORY_HELP": (
        "Path to the directory where the configuration file is stored."
    ),
    "OVERWRITE_CONFIGURATION_FILE_HELP": (
        "Flag to skip confirmation before overwriting configuration file."
    ),
    "SKIP_IF_USER_LOGGED_IN_HELP": (
        "Flag to skip the logging process if the user is already logged in."
    ),
}

DESCRIBE = {
    "DESCRIBE_DESCRIPTION_HELP": (
        "Retrieve metadata information from the Copernicus Marine catalogue"
        " based on specified parameters and options."
    ),
    "DESCRIBE_RESPONSE_HELP": (
        "dict[str, Any]\n"
        "A dictionary containing the retrieved metadata information."
    ),
    "INCLUDE_DESCRIPTION_HELP": "Include product description in output.",
    "INCLUDE_DATASETS_HELP": "Include product dataset details in output.",
    "INCLUDE_KEYWORDS_HELP": "Include product keyword details in output.",
    "INCLUDE_VERSIONS_HELP": (
        "Include dataset versions in output. By default, shows only the default "
        "version."
    ),
    "INCLUDE_ALL_HELP": (
        "Include all the possible data in output: description, datasets, keywords, "
        "and versions."
    ),
    "CONTAINS_HELP": (
        "Filter catalogue output. Returns products with attributes matching a string "
        "token."
    ),
}

SUBSET = {
    "SUBSET_DESCRIPTION_HELP": (
        "Extracts a subset of data from a specified dataset using given parameters."
        " \n \n The ``--dataset-id`` is required and can be found via the ``describe`` "
        "command. "  # has some hardcoding in CLI and python API
    ),
    "SUBSET_RESPONSE_HELP": (
        "ResponseSubset \n"
        "A description of the downloaded data and its destination."
    ),
    "OPEN_DESCRIPTION_HELP": (
        "Load an xarray dataset using 'lazy-loading' mode from a Copernicus Marine "
        "data source using either the ARCO series protocol.\n \n"
        "This means that data is only loaded into memory when a computation is called,"
        " optimizing RAM usage by avoiding immediate loading.\n"
        "It supports various parameters for customization, such as specifying "
        "geographical bounds, temporal range, depth range, and more."
    ),
    "OPEN_RESPONSE_HELP": ("xarray.Dataset\n The loaded xarray dataset."),
    "READ_DESCRIPTION_HELP": (
        "Immediately loads a Pandas DataFrame into memory from a specified dataset.\n "
        "\n Unlike 'lazy-loading,' the data is loaded as soon as this function is "
        "executed, which may be preferable when rapid access to the entire dataset "
        "is required, but may require careful memory management."
    ),
    "READ_RESPONSE_HELP": (
        "pandas.DataFrame \n A DataFrame containing the loaded Copernicus Marine data."
    ),
    "VARIABLE_HELP": "Specify dataset variable. Can be used multiple times.",
    "MINIMUM_LONGITUDE_HELP": (
        "Minimum longitude for the subset. The value will be reduced to the interval "
        "[-180; 360[."
    ),
    "MAXIMUM_LONGITUDE_HELP": (
        "Maximum longitude for the subset. The value will be reduced to the interval "
        "[-180; 360[."
    ),
    "MINIMUM_LATITUDE_HELP": (
        "Minimum latitude for the subset. Requires a float from -90 "
        "degrees to +90."
    ),
    "MAXIMUM_LATITUDE_HELP": (
        "Maximum latitude for the subset. Requires a float from -90 degrees "
        "to +90."
    ),
    "MINIMUM_DEPTH_HELP": (
        "Minimum depth for the subset. Requires a positive float (or 0)."
    ),
    "MAXIMUM_DEPTH_HELP": (
        "Maximum depth for the subset. Requires a positive float (or 0)."
    ),
    "VERTICAL_DIMENSION_OUTPUT_HELP": (
        "Consolidate the vertical dimension (the z-axis) as requested: depth with "
        "descending positive values, elevation with ascending positive values. "
        "Default is depth."
    ),
    "START_DATETIME_HELP": (
        "The start datetime of the temporal subset. Caution: encapsulate date with "
        "“ “ to ensure valid expression for format “%Y-%m-%d %H:%M:%S”. Supports "
        "common format parsed by pendulum."
    ),
    "END_DATETIME_HELP": (
        "The end datetime of the temporal subset. Caution: encapsulate date with “ “ "
        "to ensure valid expression for format “%Y-%m-%d %H:%M:%S”. Supports common "
        "format parsed by pendulum."
    ),
    "COORDINATES_SELECTION_METHOD_HELP": (
        "The method in which the coordinates will be retrieved. If ‘inside´, the "
        "selection retrieved will be inside the requested range. If ‘strict-"
        "inside’, the selection retrieved will be inside the requested range, "
        "and an error will be raised if the values don't exist. "
        "If ‘nearest’, the extremes closest to the requested values will "
        "be returned. A warning will be displayed if outside of bounds. If ‘outside’,"
        " the extremes will be taken to contain all the requested interval."
    ),
    "OUTPUT_FILENAME_HELP": (
        "Concatenate the downloaded data in the given file name (under the output "
        "directory)."
    ),
    "FILE_FORMAT_HELP": "Format of the downloaded dataset. Default to NetCDF (.nc).",
    "REQUEST_FILE_HELP": (
        "Option to pass a file containing the arguments. The file MUST follow the "
        "structure of dataclass ‘SubsetRequest’. For more information please refer to"
        " the documentation or use option ``--create-template`` "
        "for an example template."
    ),
    "MOTU_API_REQUEST_HELP": (
        "Option to pass a complete MOTU API request as a string. Caution, user has to "
        "replace double quotes “ with single quotes ‘ in the request."
    ),
    "NETCDF_COMPRESSION_ENABLED_HELP": (
        "Enable compression level 1 to the NetCDF output file. Use --netcdf-"
        "compression-level option to customize the compression level."
    ),
    "NETCDF_COMPRESSION_LEVEL_HELP": (
        "Specify a compression level to apply on the NetCDF output file. A value of 0 "
        "means no compression, and 9 is the highest level of compression available."
        "The default level is 1."
    ),
    "NETCDF_COMPATIBLE_HELP": (
        "Enable downloading the dataset in a netCDF3 compatible format."
    ),
}

GET = {
    "GET_DESCRIPTION_HELP": (
        "Download originally produced data files.\n \n  "
        " The ``--dataset-id`` is required and can be found via the ``describe``"
        " command."
    ),  # has some hardcoding in CLI
    "GET_RESPONSE_HELP": (
        "ResponseGet \n"
        "A list of files that were downloaded and some metadata."
    ),
    "REQUEST_FILE_HELP": (
        "Option to pass a file containing the arguments. The file MUST follow the "
        "structure of dataclass ‘GetRequest’. For more information please refer to"
        " the documentation or use option ``--create-template``"
        " for an example template."
    ),
    "SHOW_OUTPUTNAMES_HELP": (
        "Option to display the names of the output files before download."
    ),
    "FILTER_WITH_GLOBBING_PATTERN_HELP": (
        "A pattern that must match the absolute paths of the files to download."
    ),
    "FILTER_WITH_REGULAR_EXPRESSION_HELP": (
        "The regular expression that must match the absolute paths of the files to "
        "download."
    ),
    "FILE_LIST_HELP": (
        "Path to a .txt file containing a list of file paths, line by line, that will "
        "be downloaded directly. These files must be from the specified dataset using "
        "the –dataset-id. If no files can be found, the Toolbox will list all files "
        "on the remote server and attempt to find a match."
    ),
    "CREATE_FILE_LIST_HELP": (
        "Option to only create a file containing the names of the targeted files "
        "instead of downloading them. It writes the file in the directory specified "
        "with the –output-directory option (default to current directory). The file "
        "name specified should end with ‘.txt’ or ‘.csv’. If specified, no other "
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
        "Option to get the index files of an INSITU dataset. Temporary option."
    ),
    "NO_DIRECTORIES_HELP": (
        "If True, downloaded files will not be organized into directories."
    ),
}


SUBSET.update(SHARED)
GET.update(SHARED)
LOGIN.update(SHARED)
DESCRIBE.update(SHARED)
