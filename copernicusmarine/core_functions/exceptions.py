class MinimumLongitudeGreaterThanMaximumLongitude(Exception):
    pass


class VariableDoesNotExistInTheDataset(Exception):
    def __init__(self, variable):
        super().__init__()
        self.__setattr__(
            "custom_exception_message",
            f"The variable '{variable}' is neither a variable or a standard name in the dataset",  # noqa
        )


class CoordinatesOutOfDatasetBounds(Exception):
    def __init__(self, message: str):
        super().__init__()
        self.__setattr__("custom_exception_message", message)


class NetCDFCompressionNotAvailable(Exception):
    pass
