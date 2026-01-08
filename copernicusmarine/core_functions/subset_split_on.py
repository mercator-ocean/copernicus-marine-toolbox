import logging
from copy import deepcopy
from datetime import datetime
from typing import Optional, Union

import pandas as pd
from dateutil.tz import UTC
from pandas.core.groupby import DataFrameGroupBy
from tqdm import tqdm

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarinePart,
    CopernicusMarineService,
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.exceptions import (
    FormatNotSupported,
    ServiceNotSupported,
)
from copernicusmarine.core_functions.models import (
    CoordinatesSelectionMethod,
    ResponseSubset,
    SplitOnTimeOption,
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.services_utils import RetrievalService
from copernicusmarine.core_functions.subset import (
    download_zarr_or_sparse,
    retrieve_metadata_and_check_request,
)
from copernicusmarine.core_functions.utils import (
    run_multiprocessors,
    timestamp_or_datestring_to_datetime,
)
from copernicusmarine.download_functions.utils import (
    build_filename_from_request,
)

logger = logging.getLogger("copernicusmarine")

# Allow for hourly split-on and precise time formatting in filenames
SPLIT_ON_PRECISE_TIME_FORMAT = "%Y-%m-%dT%H-%M-%S"


def subset_split_on_function(
    on_variables: bool,
    on_time: Optional[SplitOnTimeOption],
    subset_request: SubsetRequest,
    concurrent_processes: Optional[int],
) -> list[ResponseSubset]:
    if not on_variables and not on_time:
        raise ValueError(
            "Split on should be requested either on variables or on time."
        )

    retrieval_service = retrieve_metadata_and_check_request(subset_request)
    if retrieval_service.service_name not in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.PLATFORMSERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        raise ServiceNotSupported(retrieval_service.service_name)
    if (
        retrieval_service.service_format
        == CopernicusMarineServiceFormat.SQLITE
    ):
        raise FormatNotSupported(
            CopernicusMarineServiceFormat.SQLITE.value,
            "split-on",
            "subset",
        )

    time_keys: list[tuple[datetime, datetime]] = []
    variables: list[str] = []
    if on_time:
        time_keys = get_split_time_keys_from_metadata(
            part=retrieval_service.dataset_part,
            time_frequence=on_time,
            requested_minimum_time=subset_request.start_datetime,
            requested_maximum_time=subset_request.end_datetime,
            coordinate_selection_method=subset_request.coordinates_selection_method,
        )
    if on_variables:
        variables = get_split_variable_keys_from_metadata(
            service=retrieval_service.service,
            requested_variables=set(subset_request.variables or []),
        )
    new_parameters: list[dict[str, Union[list[str], datetime]]] = []
    if time_keys and variables:
        (
            _,
            variables_with_time_dimension,
            _,
        ) = retrieval_service.dataset_part.get_coordinates().get(
            "time", (None, [], None)  # type: ignore
        )
        for variable in variables:
            if variable in variables_with_time_dimension:
                for start, end in time_keys:
                    new_parameters.append(
                        {
                            "variables": [variable],
                            "start_datetime": start,
                            "end_datetime": end,
                        }
                    )
            else:
                new_parameters.append({"variables": [variable]})
    elif time_keys:
        new_parameters = [
            {"start_datetime": start, "end_datetime": end}
            for start, end in time_keys
        ]
    elif variables:
        new_parameters = [{"variables": [var]} for var in variables]
    dataset_variables = [
        variable.short_name for variable in retrieval_service.service.variables
    ]
    download_function_parameters: list[
        tuple[SubsetRequest, RetrievalService, dict]
    ] = [
        (
            _update_output_filename(
                SubsetRequest(
                    **{
                        "disable_progress_bar": True,
                        **subset_request.model_dump(
                            exclude_unset=True,
                            exclude_defaults=True,
                            exclude_none=True,
                            exclude=set(split_on_parameter.keys()),
                        ),
                    }
                ).update(split_on_parameter),
                dataset_variables=dataset_variables,
                on_time=on_time,
                on_variables=on_variables,
                axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
            ),
            deepcopy(retrieval_service),
            {
                "disable": subset_request.disable_progress_bar,
                "desc": name_progress_bar(split_on_parameter),
                "leave": False,
                "position": 1,
            },
        )
        for split_on_parameter in new_parameters
    ]

    responses = []
    if concurrent_processes:
        responses = run_multiprocessors(
            func=download_zarr_or_sparse,
            function_arguments=download_function_parameters,
            max_concurrent_requests=concurrent_processes,
            tdqm_bar_configuration={
                "disable": subset_request.disable_progress_bar,
                "desc": "Downloading Files",
            },
        )
    else:
        with tqdm(
            total=len(download_function_parameters),
            disable=subset_request.disable_progress_bar,
            desc="Downloading Files",
        ) as pbar:
            for download_function_parameter in download_function_parameters:
                responses.append(
                    download_zarr_or_sparse(
                        download_function_parameter[0],
                        download_function_parameter[1],
                        download_function_parameter[2],
                    )
                )
                pbar.update(1)

    return responses


def get_split_time_keys_from_metadata(
    part: CopernicusMarinePart,
    time_frequence: SplitOnTimeOption,
    requested_minimum_time: Optional[datetime],
    requested_maximum_time: Optional[datetime],
    coordinate_selection_method: CoordinatesSelectionMethod,
) -> list[tuple[datetime, datetime]]:
    time_coordinate, _, _ = part.get_coordinates().get(
        "time", (None, None, None)
    )
    if not time_coordinate:
        raise ValueError(
            "No time coordinate found in the part metadata. Cannot split on time."
        )

    values = time_coordinate.values
    if values:
        values_datetime = [
            timestamp_or_datestring_to_datetime(v) for v in values
        ]

    else:
        minimum_time = timestamp_or_datestring_to_datetime(
            time_coordinate.minimum_value  # type: ignore
        )
        maximum_time = timestamp_or_datestring_to_datetime(
            time_coordinate.maximum_value  # type: ignore
        )
        step = timestamp_or_datestring_to_datetime(
            time_coordinate.step  # type: ignore
        ) - datetime(1970, 1, 1, tzinfo=UTC)
        values_datetime = [
            minimum_time + i * step
            for i in range(int((maximum_time - minimum_time) / step) + 1)
        ]

    values_datetime.sort()

    if requested_minimum_time:
        for i, v in enumerate(values_datetime):
            if v >= requested_minimum_time:
                minimum_index = i
                break
        else:
            raise ValueError(
                f"Start datetime: {requested_minimum_time} is after "
                "the maximum time available in the dataset."
            )
        if (
            coordinate_selection_method == "outside"
            and values_datetime[minimum_index] > requested_minimum_time
        ):
            minimum_index = max(0, minimum_index - 1)
        elif coordinate_selection_method == "nearest":
            if minimum_index > 0 and abs(
                values_datetime[minimum_index - 1] - requested_minimum_time
            ) < abs(values_datetime[minimum_index] - requested_minimum_time):
                minimum_index = minimum_index - 1
    else:
        minimum_index = 0

    if requested_maximum_time:
        for i in range(len(values_datetime) - 1, -1, -1):
            if values_datetime[i] <= requested_maximum_time:
                maximum_index = i
                break
        else:
            raise ValueError(
                f"End datetime: {requested_maximum_time} is before "
                "the minimum time available in the dataset."
            )
        if (
            coordinate_selection_method == "outside"
            and values_datetime[maximum_index] < requested_maximum_time
        ):
            maximum_index = min(len(values_datetime) - 1, maximum_index + 1)
        elif coordinate_selection_method == "nearest":
            if maximum_index < len(values_datetime) - 1 and abs(
                values_datetime[maximum_index + 1] - requested_maximum_time
            ) < abs(values_datetime[maximum_index] - requested_maximum_time):
                maximum_index = maximum_index + 1
    else:
        maximum_index = len(values_datetime) - 1

    selected_values = values_datetime[minimum_index : maximum_index + 1]

    groups = group_per_frequency(
        datetimes=selected_values,
        time_frequence=time_frequence,
    )
    return [
        (group_df["time"].min(), group_df["time"].max())
        for _, group_df in groups
    ]


def group_per_frequency(
    datetimes: list[datetime],
    time_frequence: SplitOnTimeOption,
) -> DataFrameGroupBy:
    df = pd.DataFrame({"time": datetimes})
    if time_frequence == "hour":
        groups = df.groupby(df["time"].dt.floor("h"))
    elif time_frequence == "day":
        groups = df.groupby(df["time"].dt.date)
    elif time_frequence == "month":
        groups = df.groupby(df["time"].apply(lambda t: (t.year, t.month)))
    elif time_frequence == "year":
        groups = df.groupby(df["time"].dt.year)
    else:
        raise ValueError(f"Invalid time option: {time_frequence}")
    return groups


def get_split_variable_keys_from_metadata(
    service: CopernicusMarineService,
    requested_variables: set[str],
) -> list[str]:
    return [
        variable.short_name
        for variable in service.variables
        if not requested_variables
        or (
            variable.short_name in requested_variables
            or variable.standard_name in requested_variables
        )
    ]


def name_progress_bar(
    split_on_parameter: dict,
) -> str:
    message = ""
    if split_on_parameter.get("start_datetime"):
        message += (
            f"{split_on_parameter['start_datetime'].strftime('%Y-%m-%d')}"
        )
    if split_on_parameter.get("end_datetime"):
        if message:
            message += " - "
        message += f"{split_on_parameter['end_datetime'].strftime('%Y-%m-%d')}"
    if split_on_parameter.get("variables"):
        if message:
            message += " - "
        message += f"{split_on_parameter['variables'][0]}"
    return message


def _update_output_filename(
    subset_request: SubsetRequest,
    on_time: Optional[SplitOnTimeOption],
    on_variables: bool,
    dataset_variables: list[str],
    axis_coordinate_id_mapping: dict[str, str],
) -> SubsetRequest:
    if subset_request.output_filename:
        parsed_filename = subset_request.output_filename.split(".")
        suffix = _get_split_on_suffix(
            subset_request,
            on_time=on_time,
            on_variables=on_variables,
        )
        if len(parsed_filename) == 1:
            return subset_request.update(
                {"output_filename": subset_request.output_filename + suffix}
            )
        else:
            return subset_request.update(
                {
                    "output_filename": (
                        ".".join(parsed_filename[:-1])
                        + suffix
                        + "."
                        + parsed_filename[-1]
                    )
                }
            )
    filename = build_filename_from_request(
        subset_request,
        subset_request.variables or dataset_variables,
        platform_ids=subset_request.platform_ids or [],
        axis_coordinate_id_mapping=axis_coordinate_id_mapping,
        time_format=SPLIT_ON_PRECISE_TIME_FORMAT,
    )
    return subset_request.update({"output_filename": filename})


def _get_split_on_suffix(
    subset_request: SubsetRequest,
    on_time: Optional[SplitOnTimeOption],
    on_variables: bool,
) -> str:
    suffix = "_"
    if on_time:
        time_format = SPLIT_ON_PRECISE_TIME_FORMAT
        if subset_request.start_datetime:
            suffix += f"{subset_request.start_datetime.strftime(time_format)}"
        if subset_request.end_datetime and (
            not subset_request.start_datetime
            or subset_request.end_datetime.strftime(time_format)
            != subset_request.start_datetime.strftime(time_format)
        ):
            if suffix:
                suffix += "_"
            suffix += f"{subset_request.end_datetime.strftime(time_format)}"
    if on_variables and subset_request.variables:
        if suffix:
            suffix += "_"
        suffix += f"{subset_request.variables[0]}"
    return suffix
