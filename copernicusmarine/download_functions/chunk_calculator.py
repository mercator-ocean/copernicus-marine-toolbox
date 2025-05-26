import logging
import math
from datetime import datetime
from typing import Optional, Union

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCoordinate,
    CopernicusMarinePart,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.models import (
    ChunkType,
    CoordinateChunking,
    DatasetChunking,
    VariableChunking,
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.utils import (
    datetime_to_timestamp,
    timestamp_or_datestring_to_datetime,
)
from copernicusmarine.download_functions.subset_xarray import (
    t_axis_selection,
    x_axis_selection,
)

logger = logging.getLogger("copernicusmarine")


def _get_chunks_index_arithmetic(
    requested_value: float,
    reference_chunking_step: float,
    chunk_length: Union[int, float],
    chunk_step: Union[int, float],
) -> int:
    """
    Chunk index calculation for arithmetic chunking.
    """
    return math.floor(
        (requested_value - reference_chunking_step)
        / (chunk_length * chunk_step)
    )


def _get_chunks_index_geometric(
    requested_value: float,
    reference_chunking_step: float,
    chunk_length: Union[int, float],
    factor: Union[int, float, None],
) -> int:
    """
    Chunk index calculation for geometric chunking.
    """
    absolute_coordinate = abs(requested_value - reference_chunking_step)
    if absolute_coordinate < chunk_length:
        return 0
    if factor == 1 or factor is None:
        chunk_index = math.floor(absolute_coordinate / chunk_length)
    else:
        chunk_index = math.ceil(
            math.log(absolute_coordinate / chunk_length) / math.log(factor)
        )
    return (
        -chunk_index
        if requested_value < reference_chunking_step
        else chunk_index
    )


def _get_chunk_indexes_for_coordinate(
    coordinate: CopernicusMarineCoordinate,
    requested_minimum: Optional[float],
    requested_maximum: Optional[float],
    chunking_length: Union[int, float],
) -> tuple[int, int]:
    (
        coordinate_minimum_value,
        coordinate_maximum_value,
    ) = _get_coordinate_extreme(coordinate)
    if coordinate_minimum_value is None or coordinate_maximum_value is None:
        return 0, 0
    if (
        requested_minimum is None
        or requested_minimum < coordinate_minimum_value
    ):
        requested_minimum = coordinate_minimum_value
    if (
        requested_maximum is None
        or requested_maximum > coordinate_maximum_value
    ):
        requested_maximum = coordinate_maximum_value
    index_min = 0
    index_max = 0
    if chunking_length:
        if (
            coordinate.chunk_type is None
            and coordinate.step is None
            and coordinate.values
        ):
            sorted_values = sorted(coordinate.values)
            index_max = int(
                len(
                    [
                        i
                        for i in sorted_values
                        if requested_minimum <= i <= requested_maximum  # type: ignore
                    ]
                )
                / chunking_length
            )
            # index max is actually number of chunks -1
            index_min = 0
        elif (
            coordinate.chunk_type == ChunkType.ARITHMETIC
            or coordinate.chunk_type is None
        ):
            index_min = _get_chunks_index_arithmetic(
                requested_minimum,
                coordinate.chunk_reference_coordinate
                or coordinate_minimum_value,
                chunking_length,
                coordinate.step or 1,
            )
            index_max = _get_chunks_index_arithmetic(
                requested_maximum,
                coordinate.chunk_reference_coordinate
                or coordinate_minimum_value,
                chunking_length,
                coordinate.step or 1,
            )
        elif coordinate.chunk_type == ChunkType.GEOMETRIC:
            index_min = _get_chunks_index_geometric(
                requested_minimum,
                coordinate.chunk_reference_coordinate
                or coordinate_minimum_value,
                chunking_length,
                coordinate.chunk_geometric_factor,
            )
            index_max = _get_chunks_index_geometric(
                requested_maximum,
                coordinate.chunk_reference_coordinate
                or coordinate_minimum_value,
                chunking_length,
                coordinate.chunk_geometric_factor,
            )
    return (index_min, index_max)


def get_dataset_chunking(
    dataset_subset: SubsetRequest,
    service_name: CopernicusMarineServiceNames,
    dataset_version_part: CopernicusMarinePart,
) -> DatasetChunking:
    service = dataset_version_part.get_service_by_service_name(service_name)
    axis_coordinate_mapping = service.get_axis_coordinate_id_mapping()
    variables = dataset_subset.variables or []
    number_of_chunks = 0
    variables_chunking: dict[str, VariableChunking] = {}
    coordinate_chunking: dict[str, CoordinateChunking] = {}
    variables_to_iterate = (
        [
            variable
            for variable in service.variables
            if variable.short_name in variables
            or variable.standard_name in variables
        ]
        if variables
        else service.variables
    )
    for variable in variables_to_iterate:
        number_chunks_per_variable = 1
        number_values_per_variable: Union[float, int] = 1
        for coordinate in variable.coordinates:
            if coordinate.chunking_length:
                chunking_length = coordinate.chunking_length
            else:
                continue
            (
                requested_minimum,
                requested_maximum,
            ) = _extract_requested_min_max(
                coordinate.coordinate_id,
                dataset_subset,
                axis_coordinate_mapping,
            )
            chunk_range = _get_chunk_indexes_for_coordinate(
                coordinate=coordinate,
                requested_minimum=requested_minimum,
                requested_maximum=requested_maximum,
                chunking_length=chunking_length,
            )
            number_chunks_coordinate = chunk_range[1] - chunk_range[0] + 1
            if coordinate.coordinate_id not in coordinate_chunking:
                coordinate_chunking[
                    coordinate.coordinate_id
                ] = CoordinateChunking(
                    coordinate_id=coordinate.coordinate_id,
                    chunking_length=chunking_length,
                    number_of_chunks=number_chunks_coordinate,
                )
            number_chunks_per_variable *= number_chunks_coordinate
            number_values_per_variable *= (
                number_chunks_coordinate * chunking_length
            )
        variables_chunking[variable.short_name] = VariableChunking(
            variable_short_name=variable.short_name,
            number_values=number_values_per_variable,
            number_chunks=number_chunks_per_variable,
            # default to 2MB as it is what is intended by ARCO producer
            chunk_size=2_000_000,
        )
        number_of_chunks += number_chunks_per_variable
    return DatasetChunking(
        chunking_per_variable=variables_chunking,
        chunking_per_coordinate=coordinate_chunking,
        number_chunks=number_of_chunks,
    )


def _extract_requested_min_max(
    coordinate_id: str,
    subset_request: SubsetRequest,
    axis_coordinate_id_mapping: dict[str, str],
) -> tuple[Optional[float], Optional[float]]:
    if coordinate_id in axis_coordinate_id_mapping.get("t", ""):
        temporal_selection = t_axis_selection(
            subset_request.get_temporal_parameters(
                axis_coordinate_id_mapping=axis_coordinate_id_mapping
            )
        )
        min_time = None
        max_time = None
        if isinstance(temporal_selection, slice):
            min_time_datetime = temporal_selection.start
            max_time_datetime = temporal_selection.stop
        elif isinstance(temporal_selection, datetime):
            min_time_datetime = temporal_selection
            max_time_datetime = temporal_selection
        else:
            return None, None
        if min_time_datetime:
            min_time = datetime_to_timestamp(min_time_datetime)
        if max_time_datetime:
            max_time = datetime_to_timestamp(max_time_datetime)

        return min_time, max_time
    if coordinate_id in axis_coordinate_id_mapping.get("y", ""):
        return (
            subset_request.minimum_y,
            subset_request.maximum_y,
        )
    if coordinate_id in axis_coordinate_id_mapping.get("x", ""):
        x_selection, _ = x_axis_selection(
            subset_request.get_geographical_parameters(
                axis_coordinate_id_mapping=axis_coordinate_id_mapping
            ).x_axis_parameters
        )
        if isinstance(x_selection, slice):
            return x_selection.start, x_selection.stop
        else:
            return (None, None)
    if coordinate_id in axis_coordinate_id_mapping.get("z", ""):
        return subset_request.minimum_depth, subset_request.maximum_depth
    return None, None


def _get_coordinate_extreme(
    coordinate: CopernicusMarineCoordinate,
) -> tuple[Union[int, float, None], Union[int, float, None]]:
    """
    Get the extreme value of a coordinate.
    """
    coordinate_minimum_value: Union[int, float]
    coordinate_maximum_value: Union[int, float]
    if isinstance(coordinate.minimum_value, str):
        coordinate_minimum_value = float(
            timestamp_or_datestring_to_datetime(
                coordinate.minimum_value
            ).timestamp()
            * 1e3
        )
    elif coordinate.minimum_value is not None:
        coordinate_minimum_value = coordinate.minimum_value
    elif coordinate.values is not None:
        coordinate_minimum_value = min(coordinate.values)  # type: ignore
        if coordinate.coordinate_id == "time":
            coordinate_minimum_value = min(
                float(
                    timestamp_or_datestring_to_datetime(value).timestamp()
                    * 1e3
                )
                for value in coordinate.values
            )

    else:
        logger.debug(
            f"Not enough information to get chunking information"
            f"for {coordinate.coordinate_id}."
        )
        return None, None
    if isinstance(coordinate.maximum_value, str):
        coordinate_maximum_value = float(
            timestamp_or_datestring_to_datetime(
                coordinate.maximum_value
            ).timestamp()
            * 1e3
        )
    elif coordinate.maximum_value is not None:
        coordinate_maximum_value = coordinate.maximum_value
    elif coordinate.values is not None:
        coordinate_maximum_value = max(coordinate.values)  # type: ignore
        if coordinate.coordinate_id == "time":
            coordinate_maximum_value = max(
                float(
                    timestamp_or_datestring_to_datetime(value).timestamp()
                    * 1e3
                )
                for value in coordinate.values
            )

    else:
        logger.debug(
            f"Not enough information to get chunking information"
            f"for {coordinate.coordinate_id}."
        )
        return None, None
    return (
        coordinate_minimum_value,
        coordinate_maximum_value,
    )
