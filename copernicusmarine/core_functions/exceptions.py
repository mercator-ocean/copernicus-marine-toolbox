class MinimumLongitudeGreaterThanMaximumLongitude(Exception):
    """
    Exception raised when the minimum longitude is greater than the maximum longitude.

    Please make sure the minimum longitude is less or equal than the maximum longitude.
    """

    pass


class VariableDoesNotExistInTheDataset(Exception):
    """
    Exception raised when the variable does not exist in the dataset.

    Please sure the variable exists in the dataset
    and/or that you use the standard name.
    """

    def __init__(self, variable):
        super().__init__(
            f"The variable '{variable}' is neither a variable or a standard name in"
            f" the dataset."
        )
        self.__setattr__(
            "custom_exception_message",
            f"The variable '{variable}' is neither a variable or a standard name in "
            f"the dataset.",
        )


class CoordinatesOutOfDatasetBounds(Exception):
    """
    Exception raised when the coordinates are out of the dataset bounds.

    Please make sure the coordinates are within the dataset bounds. If you are using the
    strict-inside mode, the coordinates must be within the dataset bounds.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.__setattr__("custom_exception_message", message)


class NetCDFCompressionNotAvailable(Exception):
    """
    Exception raised when the NetCDF compression is not available.

    Please make sure the NetCDF compression is available
    with the current Python libraries.
    """

    pass


class WrongDatetimeFormat(Exception):
    """
    Exception raised when the datetime format is wrong.

    Supported formats are:

    * the string "now"
    * all formats supported by dateutil Python library

    see `dateutil documentation page <https://dateutil.readthedocs.io/en/stable/parser.html>`_.
    """  # noqa

    pass


class FormatNotSupported(Exception):
    """
    Exception raised when the format is not supported for the command.
    Usually, it means that you are trying to subset a sparse dataset and
    for sparse datasets lazy loading is not available.
    Use :func:`copernicusmarine.read_dataframe` or :func:`copernicusmarine.subset` instead.

    Please try other commands or use datasets with the supported format.
    """  # noqa

    def __init__(
        self, format_type: str, command_type: str, recommended_command: str
    ):
        super().__init__(
            f"Lazy loading of format type '{format_type}' not "
            f"supported with command '{command_type}'. "
            f"You may want to look into '{recommended_command}' instead "
            f"to subset your data."
        )


class ServiceNotSupported(Exception):
    """
    Exception raised when the service type is not supported.

    Some services are not supported by the current implementation of the toolbox.
    """

    def __init__(self, service_type):
        super().__init__(f"Service type {service_type} not supported.")


class MutuallyExclusiveArguments(Exception):
    """
    Exception raised when mutually exclusive arguments are used together.

    Please make sure the arguments are not used together.
    """

    def __init__(self, arg1, arg2):
        super().__init__(
            f"Arguments '{arg1}' and '{arg2}' are mutually exclusive."
        )


class LonLatSubsetNotAvailableInOriginalGridDatasets(Exception):
    """
    Exception raised when using longitude and latitude subset on
    a original grid dataset.

    The options ``--maximum-longitude``, ``--minimum-longitude``,
    ``--maximum-latitude`` and ``--minimum-latitude`` cannot be
      used with 'originalGrid' dataset part.

    """

    def __init__(self):
        super().__init__(
            "You cannot specify longitude and latitude when using the 'originalGrid' "
            "dataset part. "
            "Try using ``--minimum-x``, ``--maximum-x``, ``--minimum-y`` and "
            "``--maximum-y``."
        )


class XYNotAvailableInNonOriginalGridDatasets(Exception):
    """
    Exception raised when using x and y subset on a non-original grid
    dataset.

    Please make sure the dataset part is 'originalGrid' when the options
    ``--minimum-x``, ``--maximum-x``, ``--minimum-y`` and ``--maximum-y``.
    """

    def __init__(self):
        super().__init__(
            "You cannot specify x and y when not using the 'originalGrid' dataset part."
            " Try using ``--maximum-longitude``, ``--minimum-longitude``, "
            "``--maximum-latitude`` and ``--minimum-latitude`` instead"
            " or make sure to specify the dataset_part."
        )


class DatasetUpdating(Exception):
    """
    Exception raised when the dataset is currently updating
    and the flag raise-if-updating is set to True.
    To avoid this exception, you can remove the flag from the query,
    request a subset of data before the updating start date or
    wait for the ARCO service update to be completed, which can
    take from 5 minutes to some hours.
    """

    def __init__(self, message: str):
        super().__init__(message)


class NotEnoughPlatformMetadata(Exception):
    """
    Exception raised when there is not enough platform metadata
    and user wants to perform subset on platform ids.

    Please contact the Copernicus Marine support team if needed.
    """

    def __init__(self):
        super().__init__(
            "Not enough platform metadata. "
            "Please make sure the platform metadata is available."
        )


class PlatformsSubsettingNotAvailable(Exception):
    """
    Exception raised when the subsetting on platforms is not available.


    Please make sure to not request platform ids for this dataset.
    """

    def __init__(self):
        super().__init__(
            "Subsetting on platforms is not available for this dataset. "
            "Please make sure not to request platform IDs for this dataset."
        )


class WrongPlatformID(Exception):
    """
    Exception raised when the requested platform ids are not in the list of platforms.

    Please make sure the platform id is in the list of platforms.
    Check the describe output and the "platformseries" service for more information.
    """  # noqa

    def __init__(self, platform_ids, platforms_metadata_url):
        super().__init__(
            f"None of the platform ids '{platform_ids}' are in the list of platforms."
            f" Please check the describe output and the platforms metadata at"
            f" '{platforms_metadata_url}' for more information."
        )


class ServiceDoesNotExistForCommand(Exception):
    """
    Exception raised when the service does not exist for the command.

    Please make sure the service exists for the command.
    """  # TODO: list available services per command

    def __init__(
        self,
        requested_service_name: str,
        command_name: str,
        available_services: list[str],
    ):
        super().__init__()
        self.__setattr__(
            "custom_exception_message",
            f"Service {requested_service_name} "
            f"does not exist for command {command_name}. "
            f"Possible service{'s' if len(available_services) > 1 else ''}: "
            f"{available_services}",
        )


class NoServiceAvailable(Exception):
    """
    Exception raised when no service is available for the dataset.

    We could not find a service for this dataset.
    Please make sure there is a service available for the dataset.
    """

    pass


class ServiceNotAvailable(Exception):
    """
    Exception raised when the service is not available for the dataset.

    Please make sure the service is available for the specific dataset.
    """

    pass
