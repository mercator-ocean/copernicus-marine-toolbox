import pathlib
from typing import Optional

from copernicusmarine.core_functions.models import (
    ResponseSubset,
    SplitOnTimeOption,
)
from copernicusmarine.core_functions.request_structure import (
    create_subset_request,
)
from copernicusmarine.core_functions.subset_split_on import (
    subset_split_on_function,
)
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@log_exception_and_exit
def subset_split_on(
    on_variables: bool = False,
    on_time: Optional[SplitOnTimeOption] = None,
    concurrent_processes: Optional[int] = None,
    **kwargs,
) -> list[ResponseSubset]:
    """
    Extract a subset of data from a specified dataset using given parameters
    and split the output files based on variable names, time intervals or both.
    By default, sequentially download the data.

    The datasetID is required and can be found via the ``describe`` command.
    Accept all the arguments from the subset.

    Parameters
    ----------
    on_variables : bool, optional
        If True, split the output files based on variable names, by default False.
    on_time : Optional[SplitOnTimeOption], optional
        If provided, split the output files based on specified time intervals, by default None.
    concurrent_processes : Optional[int], optional
        Number of concurrent processes to use for downloading data, by default None. Should be greater or equal to 1.
    **kwargs
        Additional keyword arguments for subset request creation. See :class:`copernicusmarine.subset` for detailed accepted parameters.


    Returns
    -------
    list[ResponseSubset]
        A description of the downloaded data and its destination for each downloaded files.

    """  # noqa

    if kwargs.get("variables") is not None:
        _check_type(kwargs.get("variables"), list, "variables")
    if kwargs.get("platform_ids") is not None:
        _check_type(kwargs.get("platform_ids"), list, "platform_ids")

    if kwargs.get("output_directory") is not None:
        kwargs["output_directory"] = pathlib.Path(
            kwargs.get("output_directory")  # type: ignore
        )
    if kwargs.get("credentials_file") is not None:
        kwargs["credentials_file"] = pathlib.Path(
            kwargs.get("credentials_file")  # type: ignore
        )

    subset_request = create_subset_request(
        **kwargs,
    )

    return subset_split_on_function(
        subset_request=subset_request,
        on_variables=on_variables,
        on_time=on_time,
        concurrent_processes=concurrent_processes,
    )


def _check_type(value, expected_type: type, name: str):
    if not isinstance(value, expected_type):
        raise TypeError(f"{name} must be of type {expected_type.__name__}")
