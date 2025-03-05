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
    Exception raised when the format is not supported for the subset.

    For now, we are not able to subset sparse datasets which are in sqlite format.
    This feature will be available in the future.
    """

    def __init__(self, format_type):
        super().__init__(
            f"Subsetting format type {format_type} not supported yet."
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

    For now, make sure the dataset part is not 'originalGrid' when using
    the options ``--maximum-longitude``, ``--minimum-longitude``,
    ``--maximum-latitude`` and ``--minimum-latitude``.

    """

    def __init__(self):
        super().__init__(
            "You cannot specify longitude and latitude when using the originalGrid "
            "dataset part yet. "
            "Try using ``--minimum-x``, ``--maximum-x``, ``--minimum-y``, "
            "``--maximum-y`` instead and then convert it."
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
            "You cannot specify x and y when not using the originalGrid dataset part."
            " Try using ``--maximum-longitude``, ``--minimum-longitude``, "
            "``--maximum-latitude`` and ``--minimum-latitude`` instead"
            " or make sure to specify the dataset_part."
        )
