import logging
from dataclasses import dataclass
from typing import Literal, Optional, Union

from copernicusmarine.catalogue_parser.catalogue_parser import (
    get_dataset_metadata,
)
from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineDataset,
    CopernicusMarinePart,
    CopernicusMarineService,
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
    CopernicusMarineVersion,
    short_name_from_service_name,
)
from copernicusmarine.core_functions.exceptions import (
    DatasetUpdating,
    NoServiceAvailable,
    PlatformsSubsettingNotAvailable,
    ServiceDoesNotExistForCommand,
    ServiceNotAvailable,
)
from copernicusmarine.core_functions.marine_datastore_config import (
    MarineDataStoreConfig,
)
from copernicusmarine.core_functions.models import CommandType, DatasetChunking
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.utils import (
    datetime_parser,
    next_or_raise_exception,
)
from copernicusmarine.download_functions.chunk_calculator import (
    get_dataset_chunking,
)

logger = logging.getLogger("copernicusmarine")


def _service_does_not_exist_for_command(
    requested_service_name: str,
    command_type: CommandType,
) -> ServiceDoesNotExistForCommand:
    return ServiceDoesNotExistForCommand(
        requested_service_name,
        command_type.command_name.value,
        command_type.get_available_service_for_command(),
    )


def _select_forced_service(
    dataset_version_part: CopernicusMarinePart,
    force_service_name: CopernicusMarineServiceNames,
    command_type: CommandType,
) -> CopernicusMarineService:
    return next_or_raise_exception(
        (
            service
            for service in dataset_version_part.services
            if service.service_name == force_service_name
        ),
        _service_not_available_error(dataset_version_part, command_type),
    )


def _get_best_arco_service_type(
    dataset_subset: SubsetRequest,
    dataset_version_part: CopernicusMarinePart,
) -> tuple[
    Literal[
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.GEOSERIES,
    ],
    Optional[DatasetChunking],
]:
    dataset_chunking_geoseries = get_dataset_chunking(
        dataset_subset,
        CopernicusMarineServiceNames.GEOSERIES,
        dataset_version_part,
    )
    dataset_chunking_timeseries = get_dataset_chunking(
        dataset_subset,
        CopernicusMarineServiceNames.TIMESERIES,
        dataset_version_part,
    )
    if (
        dataset_chunking_geoseries.number_chunks < 0
        and dataset_chunking_timeseries.number_chunks < 0
    ):
        logger.debug("We were not able to compute the optimum service.")
        return (
            CopernicusMarineServiceNames.GEOSERIES,
            None,
        )
    if dataset_chunking_geoseries.number_chunks < 0:
        return (
            CopernicusMarineServiceNames.TIMESERIES,
            dataset_chunking_timeseries,
        )
    if dataset_chunking_timeseries.number_chunks < 0:
        return (
            CopernicusMarineServiceNames.GEOSERIES,
            dataset_chunking_geoseries,
        )
    logger.debug(
        f"{dataset_chunking_geoseries.number_chunks} chunks to "
        f"download for geoseries and "
        f"{dataset_chunking_timeseries.number_chunks} chunks for timeseries"
    )
    if (
        dataset_chunking_timeseries.number_chunks
        >= dataset_chunking_geoseries.number_chunks
    ):
        return (
            CopernicusMarineServiceNames.GEOSERIES,
            dataset_chunking_geoseries,
        )
    return CopernicusMarineServiceNames.TIMESERIES, dataset_chunking_timeseries


def _get_first_available_service_name(
    command_type: CommandType,
    dataset_available_service_names: list[CopernicusMarineServiceNames],
) -> CopernicusMarineServiceNames:
    available_service_names = command_type.service_names_by_priority
    return next_or_raise_exception(
        (
            service_name
            for service_name in available_service_names
            if service_name in dataset_available_service_names
        ),
        _no_service_available_for_command(command_type),
    )


def _select_service_by_priority(
    dataset_version_part: CopernicusMarinePart,
    command_type: CommandType,
    dataset_subset: Optional[SubsetRequest],
    platform_ids_subset: bool,
) -> tuple[CopernicusMarineService, Optional[DatasetChunking]]:
    dataset_available_service_names = [
        service.service_name for service in dataset_version_part.services
    ]
    first_available_service_name = _get_first_available_service_name(
        command_type=command_type,
        dataset_available_service_names=dataset_available_service_names,
    )
    first_available_service = dataset_version_part.get_service_by_service_name(
        service_name=first_available_service_name
    )
    if (
        CopernicusMarineServiceNames.GEOSERIES
        in dataset_available_service_names
        and CopernicusMarineServiceNames.TIMESERIES
        in dataset_available_service_names
        and command_type
        in [
            CommandType.SUBSET,
            CommandType.OPEN_DATASET,
            CommandType.READ_DATAFRAME,
        ]
        and dataset_subset is not None
    ):
        if (
            first_available_service.service_format
            == CopernicusMarineServiceFormat.SQLITE
        ):
            if platform_ids_subset:
                try:
                    return (
                        dataset_version_part.get_service_by_service_name(
                            CopernicusMarineServiceNames.PLATFORMSERIES
                        ),
                        None,
                    )
                except StopIteration:
                    raise PlatformsSubsettingNotAvailable()

            return first_available_service, None
        best_arco_service_type, dataset_chunking = _get_best_arco_service_type(
            dataset_subset,
            dataset_version_part,
        )
        return (
            dataset_version_part.get_service_by_service_name(
                best_arco_service_type
            ),
            dataset_chunking,
        )
    return first_available_service, None


# TODO: clear this as there is redundancy
@dataclass
class RetrievalService:
    dataset_id: str
    service_name: CopernicusMarineServiceNames
    service_format: Optional[CopernicusMarineServiceFormat]
    uri: str
    dataset_valid_start_date: Optional[Union[str, int, float]]
    metadata_url: str
    service: CopernicusMarineService
    dataset_part: CopernicusMarinePart
    axis_coordinate_id_mapping: dict[str, str]
    dataset_chunking: Optional[DatasetChunking]
    is_original_grid: bool
    product_doi: Optional[str]


def get_retrieval_service(
    dataset_id: str,
    force_dataset_version_label: Optional[str],
    force_dataset_part_label: Optional[str],
    force_service_name_or_short_name: Optional[str],
    command_type: CommandType,
    dataset_subset: Optional[SubsetRequest],
    marine_datastore_config: MarineDataStoreConfig,
    platform_ids_subset: bool = False,
) -> RetrievalService:
    dataset_metadata = get_dataset_metadata(
        dataset_id, marine_datastore_config
    )
    if not dataset_metadata:
        raise KeyError(
            f"The requested dataset '{dataset_id}' was not found in the catalogue,"
            " you can use 'copernicusmarine describe -r datasets "
            "--contains <search_token>' to find datasets"
        )
    force_service_name: Optional[CopernicusMarineServiceNames] = (
        _service_name_from_string(
            force_service_name_or_short_name, command_type
        )
        if force_service_name_or_short_name
        else None
    )
    product_doi = dataset_metadata.digital_object_identifier

    return _get_retrieval_service_from_dataset(
        dataset=dataset_metadata,
        force_dataset_version_label=force_dataset_version_label,
        force_dataset_part_label=force_dataset_part_label,
        force_service_name=force_service_name,
        command_type=command_type,
        dataset_subset=dataset_subset,
        platform_ids_subset=platform_ids_subset,
        product_doi=product_doi,
    )


def _get_retrieval_service_from_dataset(
    dataset: CopernicusMarineDataset,
    force_dataset_version_label: Optional[str],
    force_dataset_part_label: Optional[str],
    force_service_name: Optional[CopernicusMarineServiceNames],
    command_type: CommandType,
    dataset_subset: Optional[SubsetRequest],
    platform_ids_subset: bool,
    product_doi: Optional[str],
) -> RetrievalService:
    dataset_version = dataset.get_version(force_dataset_version_label)
    logger.info(f'Selected dataset version: "{dataset_version.label}"')
    return _get_retrieval_service_from_dataset_version(
        dataset_id=dataset.dataset_id,
        dataset_version=dataset_version,
        force_dataset_part_label=force_dataset_part_label,
        force_service_name=force_service_name,
        command_type=command_type,
        dataset_subset=dataset_subset,
        platform_ids_subset=platform_ids_subset,
        product_doi=product_doi,
    )


def _get_retrieval_service_from_dataset_version(
    dataset_id: str,
    dataset_version: CopernicusMarineVersion,
    force_dataset_part_label: Optional[str],
    force_service_name: Optional[CopernicusMarineServiceNames],
    command_type: CommandType,
    dataset_subset: Optional[SubsetRequest],
    platform_ids_subset: bool,
    product_doi: Optional[str],
) -> RetrievalService:
    dataset_part = dataset_version.get_part(force_dataset_part_label)
    logger.info(f'Selected dataset part: "{dataset_part.name}"')
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

    # check that the dataset is not being updated
    if dataset_part.arco_updating_start_date:
        updating_date = datetime_parser(dataset_part.arco_updating_start_date)
        if not dataset_subset or (
            dataset_subset
            and (
                not dataset_subset.end_datetime
                or (
                    dataset_subset.end_datetime
                    and dataset_subset.end_datetime > updating_date
                )
            )
        ):
            error_message = _warning_dataset_updating(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                dataset_part=dataset_part,
            )
            logger.warning(error_message)
            if dataset_subset and dataset_subset.raise_if_updating:
                raise DatasetUpdating(error_message)

    service = None
    dataset_chunking = None
    if force_service_name:
        service = _select_forced_service(
            dataset_version_part=dataset_part,
            force_service_name=force_service_name,
            command_type=command_type,
        )
        if service.service_format == CopernicusMarineServiceFormat.SQLITE:
            logger.warning(
                "Forcing a service will not be taken into account for "
                "SQLite format services i.e. for sparse datasets."
            )
            service = None
    if not service:
        service, dataset_chunking = _select_service_by_priority(
            dataset_version_part=dataset_part,
            command_type=command_type,
            dataset_subset=dataset_subset,
            platform_ids_subset=platform_ids_subset,
        )
    if (
        command_type
        in [
            CommandType.SUBSET,
            CommandType.OPEN_DATASET,
            CommandType.READ_DATAFRAME,
        ]
        and service.service_format != CopernicusMarineServiceFormat.SQLITE
    ):
        logger.debug(f'Selected service: "{service.service_name}"')
        if dataset_subset and not dataset_chunking:
            dataset_chunking = get_dataset_chunking(
                dataset_subset,
                service.service_name,
                dataset_part,
            )
    dataset_start_date = _get_dataset_start_date_from_service(service)
    return RetrievalService(
        dataset_id=dataset_id,
        service_name=service.service_name,
        uri=service.uri,
        dataset_valid_start_date=dataset_start_date,
        service_format=service.service_format,
        service=service,
        dataset_part=dataset_part,
        axis_coordinate_id_mapping=service.get_axis_coordinate_id_mapping(),
        metadata_url=dataset_part.url_metadata,
        dataset_chunking=dataset_chunking,
        is_original_grid=dataset_part.name == "originalGrid",
        product_doi=product_doi,
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


def _warning_dataset_will_be_deprecated(
    dataset_id: str,
    dataset_version: CopernicusMarineVersion,
    dataset_part: CopernicusMarinePart,
):
    # TODO: maybe we can refer directly to the page of the dataset
    message = (
        f"You are using the dataset {dataset_id}"
        f", version '{dataset_version.label}'"
        f", part '{dataset_part.name}'. "
        f"This exact version and part of the dataset "
        f"will be retired on the {dataset_part.retired_date}. "
        "For more information you can check: "
        "https://marine.copernicus.eu/user-corner/product-roadmap/transition-information"  # noqa: E501
    )
    logger.warning(message)


def _warning_dataset_not_yet_released(
    dataset_id: str,
    dataset_version: CopernicusMarineVersion,
    dataset_part: CopernicusMarinePart,
):
    message = (
        f"Please note that the dataset {dataset_id}"
        f", version '{dataset_version.label}'"
        f", part '{dataset_part.name}' "
        f"is not yet released officially. It will be available by default "
        f"on the toolbox on the {dataset_part.released_date}."
    )
    logger.warning(message)


def _warning_dataset_updating(
    dataset_id: str,
    dataset_version: CopernicusMarineVersion,
    dataset_part: CopernicusMarinePart,
):
    message = (
        f"The dataset {dataset_id}"
        f", version '{dataset_version.label}'"
        f", part '{dataset_part.name}' "
        f"is currently being updated. "
        f"Data after {dataset_part.arco_updating_start_date} may not be up to date."
    )
    return message


def _service_not_available_error(
    dataset_version_part: CopernicusMarinePart,
    command_type: CommandType,
) -> ServiceNotAvailable:
    dataset_available_service_names = [
        service.service_short_name
        for service in dataset_version_part.services
        if service.service_name in command_type.service_names_by_priority
    ]
    return ServiceNotAvailable(
        f"Available services for dataset: "
        f"{dataset_available_service_names}"
    )


def _no_service_available_for_command(
    command_type: CommandType,
) -> NoServiceAvailable:
    return NoServiceAvailable(
        f"No service available for dataset "
        f"with command {command_type.command_name.value}"
    )


def _service_name_from_string(
    string: str, command_type: CommandType
) -> CopernicusMarineServiceNames:
    return next_or_raise_exception(
        (
            service_name
            for service_name in command_type.service_names_by_priority
            if string
            in {service_name, short_name_from_service_name(service_name)}
        ),
        _service_does_not_exist_for_command(string, command_type),
    )
