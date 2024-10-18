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
    with the current python libraries.
    """

    pass


class WrongDatetimeFormat(Exception):
    """
    Exception raised when the datetime format is wrong.

    Supported formats are:

    * the string "now"
    * all formats supported by pendulum python library

    see `pendulum parsing page <https://pendulum.eustace.io/docs/#parsing>`_.
    """

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
