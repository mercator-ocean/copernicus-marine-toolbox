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


class GeospatialSubsetNotAvailableForNonLatLon(Exception):
    """
    The data you are requesting is using a projection that is not on the
    normalised latitude and longitude grid. The geospatial subset of such
    datasets is not yet available.

    Please check other parts of the dataset to subset it. The geospatial subset
    of the datasets with different gridding will be fully available soon.
    """

    def __init__(self):
        super().__init__(
            "The geospatial subset of datasets in a projection that is not in "
            "latitude and longitude is not yet available. "
            "We are developing such feature and will be supported in future versions."
        )


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


# TODO: delete next major release
class FormatNotSupported(Exception):
    """
    Deprecated exception.

    Will be deleted in the next major release. Not used right now.
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


class NotEnoughPlatformMetadata(Exception):
    """
    Exception raised when there is not enough platform metadata.
    And user wants to perform subset on platform ids.

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
    Exception raised when the platform id is not in the list of platforms.

    Please make sure the platform id is in the list of platforms.
    Check the describe output and the "platformseries" service for more information.
    """

    def __init__(self, platform_id, platforms_metadata_url):
        super().__init__(
            f"The platform id '{platform_id}' is not in the list of platforms."
            f" Please check the describe output and the platforms metadata at"
            f" '{platforms_metadata_url}' for more information."
        )
