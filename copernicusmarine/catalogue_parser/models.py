import re

# TODO: change to pydantic
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Type, TypeVar, Union

import pystac

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_debug,
)
from copernicusmarine.core_functions.utils import (
    datetime_parser,
    next_or_raise_exception,
)

# Output Types definitions

VERSION_DEFAULT = "default"
PART_DEFAULT = "default"


# Service types
class _ServiceName(str, Enum):
    GEOSERIES = "arco-geo-series"
    TIMESERIES = "arco-time-series"
    FILES = "original-files"
    WMTS = "wmts"
    OMI_ARCO = "omi-arco"
    STATIC_ARCO = "static-arco"


class _ServiceShortName(str, Enum):
    GEOSERIES = "geoseries"
    TIMESERIES = "timeseries"
    FILES = "files"
    WMTS = "wmts"
    OMI_ARCO = "omi-arco"
    STATIC_ARCO = "static-arco"


@dataclass(frozen=True)
class _Service:
    service_name: _ServiceName
    short_name: _ServiceShortName

    def aliases(self) -> list[str]:
        return (
            [self.service_name.value, self.short_name.value]
            if self.short_name.value != self.service_name.value
            else [self.service_name.value]
        )

    def to_json_dict(self):
        return {
            "service_name": self.service_name.value,
            "short_name": self.short_name.value,
        }


class CopernicusMarineDatasetServiceType(_Service, Enum):
    GEOSERIES = _ServiceName.GEOSERIES, _ServiceShortName.GEOSERIES
    TIMESERIES = (
        _ServiceName.TIMESERIES,
        _ServiceShortName.TIMESERIES,
    )
    FILES = _ServiceName.FILES, _ServiceShortName.FILES
    WMTS = _ServiceName.WMTS, _ServiceShortName.WMTS
    OMI_ARCO = _ServiceName.OMI_ARCO, _ServiceShortName.OMI_ARCO
    STATIC_ARCO = _ServiceName.STATIC_ARCO, _ServiceShortName.STATIC_ARCO


def _service_type_from_web_api_string(
    name: str,
) -> CopernicusMarineDatasetServiceType:
    class WebApi(Enum):
        GEOSERIES = "timeChunked"
        TIMESERIES = "geoChunked"
        FILES = "native"
        WMTS = "wmts"
        OMI_ARCO = "omi"
        STATIC_ARCO = "static"

    web_api_mapping = {
        WebApi.GEOSERIES: CopernicusMarineDatasetServiceType.GEOSERIES,
        WebApi.TIMESERIES: CopernicusMarineDatasetServiceType.TIMESERIES,
        WebApi.FILES: CopernicusMarineDatasetServiceType.FILES,
        WebApi.WMTS: CopernicusMarineDatasetServiceType.WMTS,
        WebApi.OMI_ARCO: CopernicusMarineDatasetServiceType.OMI_ARCO,
        WebApi.STATIC_ARCO: CopernicusMarineDatasetServiceType.STATIC_ARCO,
    }

    return next_or_raise_exception(
        (
            service_type
            for service_web_api, service_type in web_api_mapping.items()
            if service_web_api.value == name
        ),
        ServiceNotHandled(name),
    )


class ServiceNotHandled(Exception):
    """
    Exception raised when the dataset does not support the service type requested.

    Please verifiy that the requested service type can be found in
    the result of the :func:`~copernicusmarine.describe` command
    for this specific dataset, version and part.
    """

    pass


# service formats
class CopernicusMarineServiceFormat(str, Enum):
    ZARR = "zarr"
    SQLITE = "sqlite"


@dataclass
class CopernicusMarineCoordinate:
    coordinate_id: str
    units: str
    minimum_value: Optional[float]
    maximum_value: Optional[float]
    step: Optional[float]
    values: Optional[list[Union[float, int]]]
    chunking_length: Optional[int]
    chunk_type: Optional[str]
    chunk_reference_coordinate: Optional[int]
    chunk_geometric_factor: Optional[int]

    Coordinate = TypeVar("Coordinate", bound="CopernicusMarineCoordinate")

    @classmethod
    def from_metadata_item(
        cls: Type[Coordinate],
        variable_id: str,
        dimension: str,
        dimension_metadata: dict,
        arco_data_metadata_producer_valid_start_date: Optional[str],
        arco_data_metadata_producer_valid_start_index: Optional[int],
    ) -> Coordinate:
        coordinates_info = dimension_metadata.get("coords", {})
        minimum_value = None
        coordinate_values = None
        if dimension == "time":
            if (
                arco_data_metadata_producer_valid_start_date
            ) and coordinates_info.get("min"):
                minimum_value = (
                    CopernicusMarineCoordinate._format_admp_valid_start_date(
                        arco_data_metadata_producer_valid_start_date,
                        to_timestamp=isinstance(
                            coordinates_info.get("min"), int
                        ),
                    )
                )
            elif (
                arco_data_metadata_producer_valid_start_index
                and coordinates_info.get("values")
            ):
                coordinate_values = coordinates_info.get("values")[
                    arco_data_metadata_producer_valid_start_index:
                ]
        chunking_length = dimension_metadata.get("chunkLen")
        if isinstance(chunking_length, dict):
            chunking_length = chunking_length.get(variable_id)

        coordinate = cls(
            coordinate_id=dimension,
            units=dimension_metadata.get("units") or "",
            minimum_value=minimum_value or coordinates_info.get("min"),  # type: ignore
            maximum_value=coordinates_info.get("max"),
            step=coordinates_info.get("step"),
            values=coordinate_values or coordinates_info.get("values"),
            chunking_length=chunking_length,
            chunk_type=dimension_metadata.get("chunkType"),
            chunk_reference_coordinate=dimension_metadata.get("chunkRefCoord"),
            chunk_geometric_factor=dimension_metadata.get(
                "chunkGeometricFactor", {}
            ).get(variable_id),
        )
        if dimension == "elevation":
            coordinate._convert_elevation_to_depth()
        return coordinate

    @staticmethod
    def _format_admp_valid_start_date(
        arco_data_metadata_producer_valid_start_date: str,
        to_timestamp: bool = False,
    ) -> Union[str, int]:
        if to_timestamp:
            return int(
                datetime_parser(
                    arco_data_metadata_producer_valid_start_date
                ).timestamp()
                * 1000
            )
        return arco_data_metadata_producer_valid_start_date

    def _convert_elevation_to_depth(self):
        self.coordinate_id = "depth"
        minimum_elevation = self.minimum_value
        maximum_elevation = self.maximum_value
        if minimum_elevation is not None:
            self.maximum_value = -minimum_elevation
        else:
            self.maximum_value = None
        if maximum_elevation is not None:
            self.minimum_value = -maximum_elevation
        else:
            self.minimum_value = None
        if self.values is not None:
            self.values = [-value for value in self.values]


@dataclass
class CopernicusMarineVariable:
    short_name: str
    standard_name: str
    units: str
    bbox: Optional[list[float]]
    coordinates: list[CopernicusMarineCoordinate]

    Variable = TypeVar("Variable", bound="CopernicusMarineVariable")

    @classmethod
    def from_metadata_item(
        cls: Type[Variable],
        metadata_item: pystac.Item,
        asset: pystac.Asset,
        variable_id: str,
        bbox: Optional[list[float]],
    ) -> Variable:
        cube_variables = metadata_item.properties["cube:variables"]
        cube_variable = cube_variables[variable_id]

        extra_fields_asset = asset.extra_fields
        dimensions = extra_fields_asset.get("viewDims") or {}
        return cls(
            short_name=variable_id,
            standard_name=cube_variable["standardName"],
            units=cube_variable.get("unit") or "",
            bbox=bbox,
            coordinates=[
                CopernicusMarineCoordinate.from_metadata_item(
                    variable_id,
                    dimension,
                    dimension_metadata,
                    metadata_item.properties.get("admp_valid_start_date"),
                    metadata_item.properties.get("admp_valid_start_index"),
                )
                for dimension, dimension_metadata in dimensions.items()
                if dimension in cube_variable["dimensions"]
            ],
        )


@dataclass
class CopernicusMarineService:
    service_type: CopernicusMarineDatasetServiceType
    service_format: Optional[CopernicusMarineServiceFormat]
    uri: str
    variables: list[CopernicusMarineVariable]

    Service = TypeVar("Service", bound="CopernicusMarineService")

    @classmethod
    def from_metadata_item(
        cls: Type[Service],
        metadata_item: pystac.Item,
        service_name: str,
        asset: pystac.Asset,
    ) -> Optional[Service]:
        try:
            service_uri = asset.get_absolute_href()
            if not service_uri:
                raise ServiceNotHandled(service_name)
            service_type = _service_type_from_web_api_string(service_name)
            service_format = None
            admp_in_preparation = metadata_item.properties.get(
                "admp_in_preparation"
            )
            if asset.media_type and "zarr" in asset.media_type:
                service_format = CopernicusMarineServiceFormat.ZARR
            elif asset.media_type and "sqlite3" in asset.media_type:
                service_format = CopernicusMarineServiceFormat.SQLITE

            if not service_uri.endswith("/"):
                if admp_in_preparation and (
                    service_type
                    == CopernicusMarineDatasetServiceType.GEOSERIES
                    or service_type
                    == CopernicusMarineDatasetServiceType.TIMESERIES
                ):
                    return None
                else:
                    bbox = metadata_item.bbox
                    return cls(
                        service_type=service_type,
                        uri=service_uri,
                        variables=[
                            CopernicusMarineVariable.from_metadata_item(
                                metadata_item, asset, var_cube["id"], bbox
                            )
                            for var_cube in metadata_item.properties[
                                "cube:variables"
                            ].values()
                        ],
                        service_format=service_format,
                    )
            return None
        except ServiceNotHandled as service_not_handled:
            log_exception_debug(service_not_handled)
            return None


@dataclass
class CopernicusMarineVersionPart:
    name: str
    services: list[CopernicusMarineService]
    retired_date: Optional[str]
    released_date: Optional[str]

    VersionPart = TypeVar("VersionPart", bound="CopernicusMarineVersionPart")

    @classmethod
    def from_metadata_item(
        cls: Type[VersionPart], metadata_item: pystac.Item, part_name: str
    ) -> Optional[VersionPart]:
        retired_date = metadata_item.properties.get("admp_retired_date")
        released_date = metadata_item.properties.get("admp_released_date")
        if retired_date and datetime_parser(retired_date) < datetime_parser(
            "now"
        ):
            return None
        services = [
            service
            for metadata_service_name, asset in metadata_item.get_assets().items()
            if (
                service := CopernicusMarineService.from_metadata_item(
                    metadata_item,
                    metadata_service_name,
                    asset,
                )
            )
        ]
        if not services:
            return None
        services = services
        return cls(
            name=part_name,
            services=services,
            retired_date=retired_date,
            released_date=released_date,
        )

    def get_service_by_service_type(
        self, service_type: CopernicusMarineDatasetServiceType
    ):
        return next(
            service
            for service in self.services
            if service.service_type == service_type
        )


@dataclass
class CopernicusMarineDatasetVersion:
    label: str
    parts: list[CopernicusMarineVersionPart]

    def get_part(
        self, force_part: Optional[str]
    ) -> CopernicusMarineVersionPart:
        wanted_part = force_part or PART_DEFAULT
        for part in self.parts:
            if part.name == wanted_part:
                return part
            elif not force_part:
                return part
        raise DatasetVersionPartNotFound(self)

    def sort_parts(self) -> tuple[Optional[str], Optional[str]]:
        not_released_parts = {
            part.name
            for part in self.parts
            if part.released_date
            and datetime_parser(part.released_date) > datetime_parser("now")
        }
        will_be_retired_parts = {
            part.name: datetime_parser(part.retired_date).timestamp()
            for part in self.parts
            if part.retired_date
        }
        max_retired_timestamp = 0.0
        if will_be_retired_parts:
            max_retired_timestamp = max(will_be_retired_parts.values()) + 1
        self.parts = sorted(
            self.parts,
            key=lambda x: (
                x.name in not_released_parts,
                max_retired_timestamp
                - will_be_retired_parts.get(x.name, max_retired_timestamp),
                -(x.name == PART_DEFAULT),
                -(x.name == "latest"),  # for INSITU datasets
                -(x.name == "bathy"),  # for STATIC datasets
                x.name,
            ),
        )
        return self.parts[0].released_date, self.parts[0].retired_date


@dataclass
class CopernicusMarineProductDataset:
    dataset_id: str
    dataset_name: str
    versions: list[CopernicusMarineDatasetVersion]

    def get_version(
        self, force_version: Optional[str]
    ) -> CopernicusMarineDatasetVersion:
        wanted_version = force_version or VERSION_DEFAULT
        for version in self.versions:
            if version.label == wanted_version:
                return version
            elif not force_version:
                return version
        raise DatasetVersionNotFound(self)

    def sort_versions(self) -> None:
        not_released_versions: set[str] = set()
        retired_dates = {}
        for version in self.versions:
            released_date, retired_date = version.sort_parts()
            if released_date and datetime_parser(
                released_date
            ) > datetime_parser("now"):
                not_released_versions.add(version.label)
            if retired_date:
                retired_dates[version.label] = retired_date

        self.versions = sorted(
            self.versions,
            key=lambda x: (
                -(x.label in not_released_versions),
                retired_dates.get(x.label, "9999-12-31"),
                -(x.label == VERSION_DEFAULT),
                x.label,
            ),
            reverse=True,
        )

    def parse_dataset_metadata_items(
        self, metadata_items: list[pystac.Item]
    ) -> None:
        all_versions = set()
        for metadata_item in metadata_items:
            (
                _,
                dataset_version,
                dataset_part,
            ) = get_version_and_part_from_full_dataset_id(metadata_item.id)
            part = CopernicusMarineVersionPart.from_metadata_item(
                metadata_item, dataset_part
            )
            if not part:
                continue
            if dataset_version in all_versions:
                for version in self.versions:
                    if version.label == dataset_version:
                        version.parts.append(part)
                        break
            else:
                all_versions.add(dataset_version)
                version = CopernicusMarineDatasetVersion(
                    label=dataset_version, parts=[part]
                )
                self.versions.append(version)


@dataclass
class CopernicusMarineProduct:
    title: str
    product_id: str
    thumbnail_url: str
    description: str
    digital_object_identifier: Optional[str]
    sources: list[str]
    processing_level: Optional[str]
    production_center: str
    keywords: Optional[list[str]]
    datasets: list[CopernicusMarineProductDataset]


@dataclass
class CopernicusMarineCatalogue:
    products: list[CopernicusMarineProduct]

    def filter_only_official_versions_and_parts(self):
        products_to_remove = []
        for product in self.products:
            datasets_to_remove = []
            for dataset in product.datasets:
                latest_version = dataset.versions[0]
                parts_to_remove = []
                for part in latest_version.parts:
                    if part.released_date and datetime_parser(
                        part.released_date
                    ) > datetime_parser("now"):
                        parts_to_remove.append(part)
                for part_to_remove in parts_to_remove:
                    latest_version.parts.remove(part_to_remove)
                if not latest_version.parts:
                    datasets_to_remove.append(dataset)
                else:
                    dataset.versions = [latest_version]
            for dataset_to_remove in datasets_to_remove:
                product.datasets.remove(dataset_to_remove)
            if not product.datasets:
                products_to_remove.append(product)
        for product_to_remove in products_to_remove:
            self.products.remove(product_to_remove)


# Errors
class DatasetVersionPartNotFound(Exception):
    """
    Exception raised when the asked part of the version of the dataset cannot be found.

    Please verifiy that the requested part can be found in
    the result of the :func:`~copernicusmarine.describe` command
    for this specific dataset version and dataset id.
    If yes, please contact user support.
    """

    def __init__(self, version: CopernicusMarineDatasetVersion):
        message = f"No part found for version {version.label}"
        super().__init__(message)


class DatasetVersionNotFound(Exception):
    """
    Exception raised when the asked version of the dataset cannot be found.

    Please verifiy that the requested version can be found in
    the result of the :func:`~copernicusmarine.describe` command
    for this specific dataset.
    If yes, please contact user support.
    """

    def __init__(self, dataset: CopernicusMarineProductDataset):
        message = f"No version found for dataset {dataset.dataset_id}"
        super().__init__(message)


class DatasetNotFound(Exception):
    """
    Exception raised when the dataset is not found in the catalogue.

    Possible reasons:

    - The dataset id is incorrect and not present in the catalog.
    - The dataset has been retired.

    Please verifiy that the dataset id is can be found in
    the result of the :func:`~copernicusmarine.describe` command.
    If yes, please contact user support.
    """

    def __init__(self, dataset_id: str):
        message = (
            f"{dataset_id} "
            f"Please check that the dataset exists and "
            f"the input datasetID is correct."
        )
        super().__init__(message)


REGEX_PATTERN_DATE_YYYYMM = r"[12]\d{3}(0[1-9]|1[0-2])"
PART_SEPARATOR = "--ext--"


def get_version_and_part_from_full_dataset_id(
    full_dataset_id: str,
) -> tuple[str, str, str]:
    if PART_SEPARATOR in full_dataset_id:
        name_with_maybe_version, part = full_dataset_id.split(PART_SEPARATOR)
    else:
        name_with_maybe_version = full_dataset_id
        part = PART_DEFAULT
    pattern = rf"^(.*?)(?:_({REGEX_PATTERN_DATE_YYYYMM}))?$"
    match = re.match(pattern, name_with_maybe_version)
    if match:
        dataset_name = match.group(1)
        version = match.group(2) or VERSION_DEFAULT
    else:
        raise Exception(f"Could not parse dataset id: {full_dataset_id}")
    return dataset_name, version, part
