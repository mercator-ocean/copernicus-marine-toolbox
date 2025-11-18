import logging
import os
from datetime import datetime
from typing import Optional, Union

import pandas as pd
from dateutil.tz import UTC
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
from copernicusmarine.core_functions.subset import (
    download_zarr_or_sparse,
    retrieve_metadata_and_check_request,
)
from copernicusmarine.core_functions.utils import (
    run_multiprocessors,
    timestamp_or_datestring_to_datetime,
)

logger = logging.getLogger("copernicusmarine")


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
            start_datetime=subset_request.start_datetime,
            end_datetime=subset_request.end_datetime,
            coordinate_selection_method=subset_request.coordinates_selection_method,
        )
    if on_variables:
        variables = get_split_variable_keys_from_metadata(
            service=retrieval_service.service,
            requested_variables=set(subset_request.variables or []),
        )
    new_parameters: list[dict[str, Union[list[str], datetime]]] = []
    parameters_to_exclude = set()
    if time_keys and variables:
        new_parameters = [
            {
                "variables": [var],
                "start_datetime": start,
                "end_datetime": end,
            }
            for var in variables
            for start, end in time_keys
        ]
        parameters_to_exclude = {"variables", "start_datetime", "end_datetime"}
    elif time_keys:
        new_parameters = [
            {"start_datetime": start, "end_datetime": end}
            for start, end in time_keys
        ]
        parameters_to_exclude = {"start_datetime", "end_datetime"}
    elif variables:
        new_parameters = [{"variables": [var]} for var in variables]
        parameters_to_exclude = {"variables"}
    download_function_parameters = [
        (
            SubsetRequest(
                **{
                    "disable_progress_bar": True,
                    **subset_request.model_dump(
                        exclude_unset=True,
                        exclude_defaults=True,
                        exclude_none=True,
                        exclude=parameters_to_exclude,
                    ),
                }
            ).update(split_on_parameter),
            retrieval_service,
            {
                "disable": subset_request.disable_progress_bar
                or concurrent_processes,
                "desc": name_progress_bar(split_on_parameter),
                "leave": False,
            },
        )
        for split_on_parameter in new_parameters
    ]

    responses = []
    if concurrent_processes:
        responses = run_multiprocessors(
            func=download_zarr_or_sparse,
            function_arguments=download_function_parameters,
            max_concurrent_requests=concurrent_processes
            or (os.cpu_count() or 2),
            tdqm_bar_configuration={
                "disable": subset_request.disable_progress_bar,
                "desc": "Downloading Files",
            },
        )
        return responses
    else:
        with tqdm(
            total=len(new_parameters),
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
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
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

    if start_datetime:
        for i, v in enumerate(values_datetime):
            if v >= start_datetime:
                minimum_index = i
                break
        else:
            minimum_index = len(values_datetime) - 1
        if (
            coordinate_selection_method == "outside"
            and values_datetime[minimum_index] > start_datetime
        ):
            minimum_index = max(0, minimum_index - 1)
        elif coordinate_selection_method == "nearest":
            if minimum_index > 0 and abs(
                values_datetime[minimum_index - 1] - start_datetime
            ) < abs(values_datetime[minimum_index] - start_datetime):
                minimum_index = minimum_index - 1

    if end_datetime:
        for i in range(len(values_datetime) - 1, -1, -1):
            if values_datetime[i] <= end_datetime:
                maximum_index = i
                break
        else:
            maximum_index = 0
        if (
            coordinate_selection_method == "outside"
            and values_datetime[maximum_index] < end_datetime
        ):
            maximum_index = min(len(values_datetime) - 1, maximum_index + 1)
        elif coordinate_selection_method == "nearest":
            if maximum_index < len(values_datetime) - 1 and abs(
                values_datetime[maximum_index + 1] - end_datetime
            ) < abs(values_datetime[maximum_index] - end_datetime):
                maximum_index = maximum_index + 1

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
):
    df = pd.DataFrame({"time": datetimes})
    if time_frequence == "hour":
        groups = df.groupby(df["time"].dt.floor("H"))
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
