import logging
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import List, Literal, Optional, Union

from copernicusmarine.catalogue_parser.catalogue_parser import (
    get_dataset_metadata,
)
from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineDatasetServiceType,
    CopernicusMarineDatasetVersion,
    CopernicusMarineProductDataset,
    CopernicusMarineService,
    CopernicusMarineServiceFormat,
    CopernicusMarineVersionPart,
)
from copernicusmarine.catalogue_parser.request_structure import (
    DatasetTimeAndSpaceSubset,
)
from copernicusmarine.core_functions import custom_open_zarr
from copernicusmarine.core_functions.exceptions import FormatNotSupported
from copernicusmarine.core_functions.utils import (
    datetime_parser,
    next_or_raise_exception,
)
from copernicusmarine.download_functions.subset_xarray import (
    get_size_of_coordinate_subset,
)

logger = logging.getLogger("copernicusmarine")


class _Command(Enum):
    GET = "get"
    SUBSET = "subset"
    LOAD = "load"


@dataclass(frozen=True)
class Command:
    command_name: _Command
    service_types_by_priority: List[CopernicusMarineDatasetServiceType]

    def service_names(self) -> List[str]:
        return list(
            map(
                lambda service_type: service_type.service_name.value,
                self.service_types_by_priority,
            )
        )

    def service_short_names(self) -> List[str]:
        return list(
            map(
                lambda service_type: service_type.short_name.value,
                self.service_types_by_priority,
            )
        )

    def service_aliases(self) -> List[str]:
        return list(
            chain(
                *map(
                    lambda service_type: service_type.aliases(),
                    self.service_types_by_priority,
                )
            )
        )


class CommandType(Command, Enum):
    SUBSET = (
        _Command.SUBSET,
        [
            CopernicusMarineDatasetServiceType.GEOSERIES,
            CopernicusMarineDatasetServiceType.TIMESERIES,
            CopernicusMarineDatasetServiceType.OMI_ARCO,
            CopernicusMarineDatasetServiceType.STATIC_ARCO,
        ],
    )
    GET = (
        _Command.GET,
        [
            CopernicusMarineDatasetServiceType.FILES,
        ],
    )
    LOAD = (
        _Command.LOAD,
        [
            CopernicusMarineDatasetServiceType.GEOSERIES,
            CopernicusMarineDatasetServiceType.TIMESERIES,
            CopernicusMarineDatasetServiceType.OMI_ARCO,
            CopernicusMarineDatasetServiceType.STATIC_ARCO,
        ],
    )


def assert_service_type_for_command(
    service_type: CopernicusMarineDatasetServiceType, command_type: CommandType
) -> CopernicusMarineDatasetServiceType:
    return next_or_raise_exception(
        (
            service_type
            for service_type in command_type.service_types_by_priority
        ),
        _service_type_does_not_exist_for_command(service_type, command_type),
    )


class ServiceDoesNotExistForCommand(Exception):
    """
    Exception raised when the service does not exist for the command.

    Please make sure the service exists for the command.
    """  # TODO: list available services per command

    def __init__(self, service_name, command_name, available_services):
        super().__init__()
        self.__setattr__(
            "custom_exception_message",
            f"Service {service_name} "
            f"does not exist for command {command_name}. "
            f"Possible service{'s' if len(available_services) > 1 else ''}: "
            f"{available_services}",
        )


def _service_type_does_not_exist_for_command(
    service_type: CopernicusMarineDatasetServiceType, command_type: CommandType
) -> ServiceDoesNotExistForCommand:
    return _service_does_not_exist_for_command(
        service_type.service_name.value, command_type
    )


def _service_does_not_exist_for_command(
    service_name: str, command_type: CommandType
) -> ServiceDoesNotExistForCommand:
    return ServiceDoesNotExistForCommand(
        service_name,
        command_type.command_name.value,
        command_type.service_aliases(),
    )


def _select_forced_service(
    dataset_version_part: CopernicusMarineVersionPart,
    force_service_type: CopernicusMarineDatasetServiceType,
    command_type: CommandType,
) -> CopernicusMarineService:
    return next_or_raise_exception(
        (
            service
            for service in dataset_version_part.services
            if service.service_type == force_service_type
        ),
        _service_not_available_error(dataset_version_part, command_type),
    )


def _get_best_arco_service_type(
    dataset_subset: DatasetTimeAndSpaceSubset,
    dataset_url: str,
    username: Optional[str],
) -> Literal[
    CopernicusMarineDatasetServiceType.TIMESERIES,
    CopernicusMarineDatasetServiceType.GEOSERIES,
]:
    dataset = custom_open_zarr.open_zarr(
        dataset_url, copernicus_marine_username=username
    )

    latitude_size = get_size_of_coordinate_subset(
        dataset,
        "latitude",
        dataset_subset.minimum_latitude,
        dataset_subset.maximum_latitude,
    )
    longitude_size = get_size_of_coordinate_subset(
        dataset,
        "longitude",
        dataset_subset.minimum_longitude,
        dataset_subset.maximum_longitude,
    )
    time_size = get_size_of_coordinate_subset(
        dataset,
        "time",
        (
            dataset_subset.start_datetime.in_tz("UTC").naive()
            if dataset_subset.start_datetime
            else dataset_subset.start_datetime
        ),
        (
            dataset_subset.end_datetime.in_tz("UTC").naive()
            if dataset_subset.end_datetime
            else dataset_subset.end_datetime
        ),
    )
    dataset_coordinates = dataset.coords

    geographical_dimensions = (
        dataset_coordinates["latitude"].size
        * dataset_coordinates["longitude"].size
    )
    subset_geographical_dimensions = latitude_size * longitude_size
    temporal_dimensions = dataset_coordinates["time"].size
    subset_temporal_dimensions = time_size

    geographical_coverage = (
        subset_geographical_dimensions / geographical_dimensions
    )
    temporal_coverage = subset_temporal_dimensions / temporal_dimensions

    if geographical_coverage >= temporal_coverage:
        return CopernicusMarineDatasetServiceType.GEOSERIES
    return CopernicusMarineDatasetServiceType.TIMESERIES


def _get_first_available_service_type(
    command_type: CommandType,
    dataset_available_service_types: list[CopernicusMarineDatasetServiceType],
) -> CopernicusMarineDatasetServiceType:
    available_service_types = command_type.service_types_by_priority
    return next_or_raise_exception(
        (
            service_type
            for service_type in available_service_types
            if service_type in dataset_available_service_types
        ),
        _no_service_available_for_command(command_type),
    )


def _select_service_by_priority(
    dataset_version_part: CopernicusMarineVersionPart,
    command_type: CommandType,
    dataset_subset: Optional[DatasetTimeAndSpaceSubset],
    username: Optional[str],
) -> CopernicusMarineService:
    dataset_available_service_types = [
        service.service_type for service in dataset_version_part.services
    ]
    first_available_service_type = _get_first_available_service_type(
        command_type=command_type,
        dataset_available_service_types=dataset_available_service_types,
    )
    first_available_service = dataset_version_part.get_service_by_service_type(
        service_type=first_available_service_type
    )
    if (
        CopernicusMarineDatasetServiceType.GEOSERIES
        in dataset_available_service_types
        and CopernicusMarineDatasetServiceType.TIMESERIES
        in dataset_available_service_types
        and command_type in [CommandType.SUBSET, CommandType.LOAD]
        and dataset_subset is not None
    ):
        if (
            first_available_service.service_format
            == CopernicusMarineServiceFormat.SQLITE
        ):
            raise FormatNotSupported(
                first_available_service.service_format.value
            )
        best_arco_service_type: CopernicusMarineDatasetServiceType = (
            _get_best_arco_service_type(
                dataset_subset, first_available_service.uri, username
            )
        )
        return dataset_version_part.get_service_by_service_type(
            best_arco_service_type
        )
    return first_available_service


@dataclass
class RetrievalService:
    dataset_id: str
    service_type: CopernicusMarineDatasetServiceType
    service_format: Optional[CopernicusMarineServiceFormat]
    uri: str
    dataset_valid_start_date: Optional[Union[str, int, float]]
    service: CopernicusMarineService


def get_retrieval_service(
    dataset_id: str,
    force_dataset_version_label: Optional[str],
    force_dataset_part_label: Optional[str],
    force_service_type_string: Optional[str],
    command_type: CommandType,
    index_parts: bool = False,
    dataset_subset: Optional[DatasetTimeAndSpaceSubset] = None,
    dataset_sync: bool = False,
    username: Optional[str] = None,
    staging: bool = False,
) -> RetrievalService:
    dataset_metadata = get_dataset_metadata(dataset_id, staging=staging)
    # logger.debug(dataset_metadata)
    if not dataset_metadata:
        raise KeyError(
            f"The requested dataset '{dataset_id}' was not found in the catalogue,"
            " you can use 'copernicusmarine describe --include-datasets "
            "--contains <search_token>' to find datasets"
        )
    force_service_type: Optional[CopernicusMarineDatasetServiceType] = (
        _service_type_from_string(force_service_type_string, command_type)
        if force_service_type_string
        else None
    )

    return _get_retrieval_service_from_dataset(
        dataset=dataset_metadata,
        force_dataset_version_label=force_dataset_version_label,
        force_dataset_part_label=force_dataset_part_label,
        force_service_type=force_service_type,
        command_type=command_type,
        index_parts=index_parts,
        dataset_subset=dataset_subset,
        dataset_sync=dataset_sync,
        username=username,
    )


def _get_retrieval_service_from_dataset(
    dataset: CopernicusMarineProductDataset,
    force_dataset_version_label: Optional[str],
    force_dataset_part_label: Optional[str],
    force_service_type: Optional[CopernicusMarineDatasetServiceType],
    command_type: CommandType,
    index_parts: bool,
    dataset_subset: Optional[DatasetTimeAndSpaceSubset],
    dataset_sync: bool,
    username: Optional[str],
) -> RetrievalService:
    if force_dataset_version_label:
        logger.info(
            "You forced selection of dataset version "
            + f'"{force_dataset_version_label}"'
        )
    dataset_version = dataset.get_version(force_dataset_version_label)
    if not force_dataset_version_label:
        logger.info(
            "Dataset version was not specified, the latest "
            f'one was selected: "{dataset_version.label}"'
        )
    return _get_retrieval_service_from_dataset_version(
        dataset_id=dataset.dataset_id,
        dataset_version=dataset_version,
        force_dataset_part_label=force_dataset_part_label,
        force_service_type=force_service_type,
        command_type=command_type,
        index_parts=index_parts,
        dataset_subset=dataset_subset,
        dataset_sync=dataset_sync,
        username=username,
    )


def _get_retrieval_service_from_dataset_version(
    dataset_id: str,
    dataset_version: CopernicusMarineDatasetVersion,
    force_dataset_part_label: Optional[str],
    force_service_type: Optional[CopernicusMarineDatasetServiceType],
    command_type: CommandType,
    index_parts: bool,
    dataset_subset: Optional[DatasetTimeAndSpaceSubset],
    dataset_sync: bool,
    username: Optional[str],
) -> RetrievalService:
    if len(dataset_version.parts) > 1 and dataset_sync:
        raise Exception(
            "Sync is not supported for datasets with multiple parts."
        )
    if force_dataset_part_label:
        logger.info(
            f"You forced selection of dataset part "
            f'"{force_dataset_part_label}"'
        )
    dataset_part = dataset_version.get_part(force_dataset_part_label)
    if not force_dataset_part_label:
        logger.info(
            "Dataset part was not specified, the first "
            f'one was selected: "{dataset_part.name}"'
        )
    if dataset_part.retired_date:
        _warning_dataset_will_be_deprecated(
            dataset_id, dataset_version, dataset_part
        )
    if dataset_part.released_date and datetime_parser(
        dataset_part.released_date
    ) > datetime_parser("now"):
        _warning_dataset_not_yet_released(
            dataset_id, dataset_version, dataset_part
        )

    if force_service_type:
        logger.info(
            f"You forced selection of service: "
            f"{force_service_type.service_name.value}"
        )
        service = _select_forced_service(
            dataset_version_part=dataset_part,
            force_service_type=force_service_type,
            command_type=command_type,
        )
        if service.service_format == CopernicusMarineServiceFormat.SQLITE:
            raise FormatNotSupported(service.service_format.value)
    else:
        service = _select_service_by_priority(
            dataset_version_part=dataset_part,
            command_type=command_type,
            dataset_subset=dataset_subset,
            username=username,
        )
        logger.info(
            "Service was not specified, the default one was "
            f'selected: "{service.service_type.service_name.value}"'
        )
    dataset_start_date = _get_dataset_start_date_from_service(service)
    return RetrievalService(
        dataset_id=dataset_id,
        service_type=service.service_type,
        uri=service.uri,
        dataset_valid_start_date=dataset_start_date,
        service_format=service.service_format,
        service=service,
    )


def _get_dataset_start_date_from_service(
    service: CopernicusMarineService,
) -> Optional[Union[str, int, float]]:
    for variable in service.variables:
        for coordinate in variable.coordinates:
            if coordinate.coordinate_id == "time":
                if coordinate.minimum_value:
                    return coordinate.minimum_value
                if coordinate.values:
                    return min(coordinate.values)
    return None


class ServiceNotAvailable(Exception):
    """
    Exception raised when the service is not available for the dataset.

    Please make sure the service is available for the specific dataset.
    """

    pass


def _warning_dataset_will_be_deprecated(
    dataset_id: str,
    dataset_version: CopernicusMarineDatasetVersion,
    dataset_part: CopernicusMarineVersionPart,
):
    logger.warning(
        f"""The dataset {dataset_id}"""
        f"""{f", version '{dataset_version.label}'"
             if dataset_version.label != 'default' else ''}"""
        f"""{(f"and part '{dataset_part.name}'"
              if dataset_part.name != 'default' else '')}"""
        f"""{"," if dataset_version.label != 'default' else ""}"""
        f""" will be retired on the {dataset_part.retired_date}."""
        """ After this date, it will no longer be available on the toolbox."""
    )


def _warning_dataset_not_yet_released(
    dataset_id: str,
    dataset_version: CopernicusMarineDatasetVersion,
    dataset_part: CopernicusMarineVersionPart,
):
    logger.warning(
        f"""The dataset {dataset_id}"""
        f"""{f", version '{dataset_version.label}'"
             if dataset_version.label != 'default' else ''}"""
        f"""{(f"and part '{dataset_part.name}'"
              if dataset_part.name != 'default' else '')}"""
        f"""{"," if dataset_version.label != 'default' else ""}"""
        f""" is not yet released officially."""
        f""" It will be available by default  on the toolbox on the"""
        f""" {dataset_part.released_date}."""
    )


def _service_not_available_error(
    dataset_version_part: CopernicusMarineVersionPart,
    command_type: CommandType,
) -> ServiceNotAvailable:
    dataset_available_service_types = [
        service.service_type.short_name.value
        for service in dataset_version_part.services
        if service.service_type in command_type.service_types_by_priority
    ]
    return ServiceNotAvailable(
        f"Available services for dataset: "
        f"{dataset_available_service_types}"
    )


class NoServiceAvailable(Exception):
    """
    Exception raised when no service is available for the dataset.

    We could not find a service for this dataset.
    Please make sure there is a service available for the dataset.
    """

    pass


def _no_service_available_for_command(
    command_type: CommandType,
) -> NoServiceAvailable:
    return NoServiceAvailable(
        f"No service available for dataset "
        f"with command {command_type.command_name.value}"
    )


def _service_type_from_string(
    string: str, command_type: CommandType
) -> CopernicusMarineDatasetServiceType:
    return next_or_raise_exception(
        (
            service_type
            for service_type in command_type.service_types_by_priority
            if string in service_type.aliases()
        ),
        _service_does_not_exist_for_command(string, command_type),
    )
