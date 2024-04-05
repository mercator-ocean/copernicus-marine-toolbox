import logging
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import List, Literal, Optional, Tuple, Union

from copernicusmarine.catalogue_parser.catalogue_parser import (
    CopernicusMarineCatalogue,
    CopernicusMarineDatasetServiceType,
    CopernicusMarineDatasetVersion,
    CopernicusMarineProductDataset,
    CopernicusMarineService,
    CopernicusMarineServiceFormat,
    CopernicusMarineVersionPart,
    dataset_version_not_found_exception,
)
from copernicusmarine.catalogue_parser.request_structure import (
    DatasetTimeAndGeographicalSubset,
)
from copernicusmarine.core_functions import sessions
from copernicusmarine.core_functions.utils import (
    FormatNotSupported,
    next_or_raise_exception,
)
from copernicusmarine.download_functions.subset_xarray import (
    get_size_of_coordinate_subset,
)

logger = logging.getLogger("copernicus_marine_root_logger")


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
    dataset_subset: DatasetTimeAndGeographicalSubset,
    dataset_url: str,
) -> Literal[
    CopernicusMarineDatasetServiceType.TIMESERIES,
    CopernicusMarineDatasetServiceType.GEOSERIES,
]:
    dataset = sessions.open_zarr(dataset_url)

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
        dataset_subset.start_datetime,
        dataset_subset.end_datetime,
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
    dataset_subset: Optional[DatasetTimeAndGeographicalSubset],
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
                dataset_subset, first_available_service.uri
            )
        )
        return dataset_version_part.get_service_by_service_type(
            best_arco_service_type
        )
    return first_available_service


def parse_dataset_id_and_service_and_suffix_path_from_url(
    catalogue: CopernicusMarineCatalogue,
    dataset_url: Optional[str],
) -> Tuple[str, CopernicusMarineDatasetServiceType, str,]:
    if dataset_url is None:
        syntax_error = SyntaxError(
            "Must specify at least one of "
            "'dataset_url' or 'dataset_id' options"
        )
        raise syntax_error
    return next_or_raise_exception(
        (
            (
                dataset.dataset_id,
                service.service_type,
                dataset_url.split(service.uri)[1],
            )
            for product in catalogue.products
            for dataset in product.datasets
            for dataset_version in dataset.versions
            for dataset_part in dataset_version.parts
            for service in dataset_part.services
            if dataset_url.startswith(service.uri)
        ),
        KeyError(
            f"The requested dataset URL '{dataset_url}' "
            "was not found in the catalogue, "
            "you can use 'copernicusmarine describe --include-datasets "
            "--contains <search_token>' to find datasets"
        ),
    )


@dataclass
class RetrievalService:
    dataset_id: str
    service_type: CopernicusMarineDatasetServiceType
    service_format: Optional[CopernicusMarineServiceFormat]
    uri: str
    dataset_valid_start_date: Optional[Union[str, int]]


def get_retrieval_service(
    catalogue: CopernicusMarineCatalogue,
    dataset_id: Optional[str],
    dataset_url: Optional[str],
    force_dataset_version_label: Optional[str],
    force_dataset_part_label: Optional[str],
    force_service_type_string: Optional[str],
    command_type: CommandType,
    index_parts: bool = False,
    dataset_subset: Optional[DatasetTimeAndGeographicalSubset] = None,
    dataset_sync: bool = False,
) -> RetrievalService:
    force_service_type: Optional[CopernicusMarineDatasetServiceType] = (
        _service_type_from_string(force_service_type_string, command_type)
        if force_service_type_string
        else None
    )
    if dataset_id is None:
        (
            dataset_id,
            service_type,
            suffix_path,
        ) = parse_dataset_id_and_service_and_suffix_path_from_url(
            catalogue, dataset_url
        )
        force_service_type = (
            service_type if not force_service_type else force_service_type
        )
    else:
        if dataset_url is not None:
            syntax_error = SyntaxError(
                "Must specify only one of 'dataset_url' or 'dataset_id' options"
            )
            raise syntax_error
        suffix_path = ""

    return _get_retrieval_service_from_dataset_id(
        catalogue=catalogue,
        dataset_id=dataset_id,
        suffix_path=suffix_path,
        force_dataset_version_label=force_dataset_version_label,
        force_dataset_part_label=force_dataset_part_label,
        force_service_type=force_service_type,
        command_type=command_type,
        index_parts=index_parts,
        dataset_subset=dataset_subset,
        dataset_sync=dataset_sync,
    )


def _get_retrieval_service_from_dataset_id(
    catalogue: CopernicusMarineCatalogue,
    dataset_id: str,
    suffix_path: str,
    force_dataset_version_label: Optional[str],
    force_dataset_part_label: Optional[str],
    force_service_type: Optional[CopernicusMarineDatasetServiceType],
    command_type: CommandType,
    index_parts: bool,
    dataset_subset: Optional[DatasetTimeAndGeographicalSubset],
    dataset_sync: bool,
) -> RetrievalService:
    dataset: CopernicusMarineProductDataset = next_or_raise_exception(
        (
            dataset
            for product in catalogue.products
            for dataset in product.datasets
            if dataset_id == dataset.dataset_id
        ),
        KeyError(
            f"The requested dataset '{dataset_id}' was not found in the catalogue,"
            " you can use 'copernicusmarine describe --include-datasets "
            "--contains <search_token>' to find datasets"
        ),
    )
    return _get_retrieval_service_from_dataset(
        dataset=dataset,
        suffix_path=suffix_path,
        force_dataset_version_label=force_dataset_version_label,
        force_dataset_part_label=force_dataset_part_label,
        force_service_type=force_service_type,
        command_type=command_type,
        index_parts=index_parts,
        dataset_subset=dataset_subset,
        dataset_sync=dataset_sync,
    )


def _get_retrieval_service_from_dataset(
    dataset: CopernicusMarineProductDataset,
    suffix_path: str,
    force_dataset_version_label: Optional[str],
    force_dataset_part_label: Optional[str],
    force_service_type: Optional[CopernicusMarineDatasetServiceType],
    command_type: CommandType,
    index_parts: bool,
    dataset_subset: Optional[DatasetTimeAndGeographicalSubset],
    dataset_sync: bool,
) -> RetrievalService:
    if force_dataset_version_label:
        logger.info(
            "You forced selection of dataset version "
            + f'"{force_dataset_version_label}"'
        )
        dataset_version: CopernicusMarineDatasetVersion = (
            next_or_raise_exception(
                filter(
                    lambda version: version.label
                    == force_dataset_version_label,
                    dataset.versions,
                ),
                dataset_version_not_found_exception(dataset),
            )
        )
    else:
        dataset_version = dataset.get_latest_version_or_raise()
        logger.info(
            "Dataset version was not specified, the latest "
            f'one was selected: "{dataset_version.label}"'
        )
    return _get_retrieval_service_from_dataset_version(
        dataset_id=dataset.dataset_id,
        dataset_version=dataset_version,
        force_dataset_part_label=force_dataset_part_label,
        suffix_path=suffix_path,
        force_service_type=force_service_type,
        command_type=command_type,
        index_parts=index_parts,
        dataset_subset=dataset_subset,
        dataset_sync=dataset_sync,
    )


def _get_retrieval_service_from_dataset_version(
    dataset_id: str,
    dataset_version: CopernicusMarineDatasetVersion,
    force_dataset_part_label: Optional[str],
    suffix_path: str,
    force_service_type: Optional[CopernicusMarineDatasetServiceType],
    command_type: CommandType,
    index_parts: bool,
    dataset_subset: Optional[DatasetTimeAndGeographicalSubset],
    dataset_sync: bool,
) -> RetrievalService:
    if len(dataset_version.parts) > 1 and dataset_sync:
        raise Exception(
            "Sync is not supported for datasets with multiple parts."
        )
    if (
        force_service_type == CopernicusMarineDatasetServiceType.FILES
        and not force_dataset_part_label
        and not index_parts
        and len(dataset_version.parts) > 1
    ):
        raise Exception(
            "When dataset has multiple parts and using 'files' service"
            ", please indicate the part you want to download "
            "with the dataset-part option"
        )
    if force_dataset_part_label:
        logger.info(
            f"You forced selection of dataset part "
            f'"{force_dataset_part_label}"'
        )
    dataset_part = dataset_version.get_part(
        force_part=force_dataset_part_label
    )
    if not force_dataset_part_label:
        logger.info(
            "Dataset part was not specified, the first "
            f'one was selected: "{dataset_part.name}"'
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
        )
        logger.info(
            "Service was not specified, the default one was "
            f'selected: "{service.service_type.service_name.value}"'
        )
    dataset_start_date = _get_dataset_start_date_from_service(service)
    return RetrievalService(
        dataset_id=dataset_id,
        service_type=service.service_type,
        uri=service.uri + suffix_path,
        dataset_valid_start_date=dataset_start_date,
        service_format=service.service_format,
    )


def _get_dataset_start_date_from_service(
    service: CopernicusMarineService,
) -> Optional[Union[str, int]]:
    for variable in service.variables:
        for coordinate in variable.coordinates:
            if (
                coordinate.coordinates_id == "time"
                and coordinate.minimum_value
            ):
                if isinstance(coordinate.minimum_value, str):
                    return coordinate.minimum_value.replace("Z", "")
                return int(coordinate.minimum_value)
    return None


class ServiceNotAvailable(Exception):
    ...


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
    ...


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
