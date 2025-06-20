import re
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional, Type, TypeVar, Union

import pystac
from pydantic import BaseModel, ConfigDict

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_debug,
)
from copernicusmarine.core_functions.utils import (
    datetime_parser,
    next_or_raise_exception,
)

VERSION_DEFAULT = "default"
PART_DEFAULT = "default"


class CopernicusMarineServiceNames(str, Enum):
    """
    Services parsed by the Copernicus Marine Toolbox.
    """

    GEOSERIES = "arco-geo-series"
    TIMESERIES = "arco-time-series"
    PLATFORMSERIES = "arco-platform-series"
    FILES = "original-files"
    WMTS = "wmts"
    OMI_ARCO = "omi-arco"
    STATIC_ARCO = "static-arco"


class CoperniusMarineServiceShortNames(str, Enum):
    """
    Short names or the services parsed by the Copernicus Marine Toolbox.
    Also accepted when a service is requested.
    """

    GEOSERIES = "geoseries"
    TIMESERIES = "timeseries"
    PLATFORMSERIES = "platformseries"
    FILES = "files"
    WMTS = "wmts"
    OMI_ARCO = "omi-arco"
    STATIC_ARCO = "static-arco"


def short_name_from_service_name(
    service_name: CopernicusMarineServiceNames,
) -> CoperniusMarineServiceShortNames:
    mapping = {
        CopernicusMarineServiceNames.GEOSERIES: CoperniusMarineServiceShortNames.GEOSERIES,  # noqa
        CopernicusMarineServiceNames.TIMESERIES: CoperniusMarineServiceShortNames.TIMESERIES,  # noqa
        CopernicusMarineServiceNames.PLATFORMSERIES: CoperniusMarineServiceShortNames.PLATFORMSERIES,  # noqa
        CopernicusMarineServiceNames.FILES: CoperniusMarineServiceShortNames.FILES,  # noqa
        CopernicusMarineServiceNames.WMTS: CoperniusMarineServiceShortNames.WMTS,  # noqa
        CopernicusMarineServiceNames.OMI_ARCO: CoperniusMarineServiceShortNames.OMI_ARCO,  # noqa
        CopernicusMarineServiceNames.STATIC_ARCO: CoperniusMarineServiceShortNames.STATIC_ARCO,  # noqa
    }
    return mapping[service_name]


def _service_type_from_web_api_string(
    name: str,
) -> CopernicusMarineServiceNames:
    class WebApi(Enum):
        GEOSERIES = "timeChunked"
        TIMESERIES = "geoChunked"
        PLATFORMSERIES = "platformChunked"
        FILES = "native"
        WMTS = "wmts"
        OMI_ARCO = "omi"
        STATIC_ARCO = "static"

    web_api_mapping: dict[WebApi, CopernicusMarineServiceNames] = {
        WebApi.GEOSERIES: CopernicusMarineServiceNames.GEOSERIES,
        WebApi.TIMESERIES: CopernicusMarineServiceNames.TIMESERIES,
        WebApi.PLATFORMSERIES: CopernicusMarineServiceNames.PLATFORMSERIES,
        WebApi.FILES: CopernicusMarineServiceNames.FILES,
        WebApi.WMTS: CopernicusMarineServiceNames.WMTS,
        WebApi.OMI_ARCO: CopernicusMarineServiceNames.OMI_ARCO,
        WebApi.STATIC_ARCO: CopernicusMarineServiceNames.STATIC_ARCO,
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

    Please verify that the requested service type can be found in
    the result of the :func:`~copernicusmarine.describe` command
    for this specific dataset, version and part.
    """

    pass


# service formats
class CopernicusMarineServiceFormat(str, Enum):
    """
    Format of the data for a service.
    For example, "arco-geo-series" and "arco-time-series" can be "zarr" or "sqlite".
    """

    ZARR = "zarr"
    SQLITE = "sqlite"


Coordinate = TypeVar("Coordinate", bound="CopernicusMarineCoordinate")


class CopernicusMarineCoordinate(BaseModel):
    """
    Coordinate for a variable.
    """

    #: Coordinate id.
    coordinate_id: str
    #: Coordinate units.
    coordinate_unit: str
    #: Minimum value of the coordinate.
    minimum_value: Optional[Union[float, str]]
    #: Maximum value of the coordinate.
    maximum_value: Optional[Union[float, str]]
    #: Step of the coordinate.
    step: Optional[float]
    #: Values of the coordinate.
    values: Optional[list[Union[float, int, str]]]
    #: Chunking length of the coordinate.
    chunking_length: Optional[Union[float, int]]
    #: Chunk type of the coordinate.
    chunk_type: Optional[str]
    #: Chunk reference coordinate of the coordinate.
    chunk_reference_coordinate: Optional[Union[float, int]]
    #: Chunk geometric factor of the coordinate.
    chunk_geometric_factor: Optional[Union[float, int]]
    #: Axis of the coordinate
    axis: Literal["x", "y", "z", "t"]

    @classmethod
    def from_metadata_item(
        cls: Type[Coordinate],
        variable_id: str,
        dimension: str,
        dimension_metadata: dict,
        arco_data_metadata_producer_valid_start_date: Optional[str],
        arco_data_metadata_producer_valid_start_index: Optional[int],
        cube_dimensions: dict,
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
        axis = cube_dimensions[dimension].get("axis", "t")
        if isinstance(chunking_length, dict):
            chunking_length = chunking_length.get(variable_id)

        coordinate = cls(
            coordinate_id=dimension,
            coordinate_unit=dimension_metadata.get("units") or "",
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
            axis=axis,
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
        if minimum_elevation is not None and isinstance(
            minimum_elevation, (int, float)
        ):
            self.maximum_value = -minimum_elevation
        else:
            self.maximum_value = None
        if maximum_elevation is not None and isinstance(
            maximum_elevation, (int, float)
        ):
            self.minimum_value = -maximum_elevation
        else:
            self.minimum_value = None
        if self.values is not None:
            self.values = [-value for value in self.values]  # type: ignore


Variable = TypeVar("Variable", bound="CopernicusMarineVariable")


class CopernicusMarineVariable(BaseModel):
    """
    Variable of the dataset.
    Contains the variable metadata and a list of coordinates.
    """

    #: Short name of the variable.
    short_name: str
    #: Standard name of the variable.
    standard_name: Optional[str]
    #: Units of the variable.
    units: Optional[str]
    #: Bounding box of the variable.
    bbox: Optional[list[float]]
    #: List of coordinates of the variable.
    coordinates: list[CopernicusMarineCoordinate]

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
        cube_dimensions = metadata_item.properties["cube:dimensions"]
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
                    cube_dimensions,
                )
                for dimension, dimension_metadata in dimensions.items()
                if dimension in cube_variable["dimensions"]
            ],
        )


Service = TypeVar("Service", bound="CopernicusMarineService")


class CopernicusMarineService(BaseModel):
    """
    Service available for a dataset.
    Contains the service metadata and a list of variables.
    For original files service, there are no variables.
    """

    model_config = ConfigDict(use_enum_values=True)

    #: Service name.
    service_name: CopernicusMarineServiceNames

    #: Service short name.
    service_short_name: Optional[CoperniusMarineServiceShortNames]

    #: Service format: format of the service
    #: (eg:"arco-geo-series" can be "zarr", "sqlite").
    service_format: Optional[CopernicusMarineServiceFormat]
    #: Service uri: uri of the service.
    uri: str
    #: List of variables of the service.
    variables: list[CopernicusMarineVariable]
    #: A link to information about available platforms.
    #: Only for arco-platform-series service.
    platforms_metadata: Optional[str]

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
            service_name_parsed = _service_type_from_web_api_string(
                service_name
            )
            service_short_name = short_name_from_service_name(
                service_name_parsed
            )
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
                    service_name_parsed
                    == CopernicusMarineServiceNames.GEOSERIES
                    or service_name_parsed
                    == CopernicusMarineServiceNames.TIMESERIES
                ):
                    return None
                else:
                    platforms_metadata = None
                    if (
                        service_name_parsed
                        == CopernicusMarineServiceNames.PLATFORMSERIES
                    ):
                        platforms_asset = metadata_item.get_assets().get(
                            "platforms"
                        )
                        if platforms_asset is not None:
                            platforms_metadata = platforms_asset.href

                    bbox = metadata_item.bbox
                    return cls(
                        service_name=service_name_parsed,
                        service_short_name=service_short_name,
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
                        platforms_metadata=platforms_metadata,
                    )
            return None
        except ServiceNotHandled as service_not_handled:
            log_exception_debug(service_not_handled)
            return None

    def get_axis_coordinate_id_mapping(
        self,
    ) -> dict[str, str]:
        axis_coordinate_id_mapping: dict[str, str] = {}
        for variable in self.variables:
            for coordinate in variable.coordinates:
                if len(axis_coordinate_id_mapping) == 4:
                    return axis_coordinate_id_mapping
                axis_coordinate_id_mapping[
                    coordinate.axis
                ] = coordinate.coordinate_id

        return axis_coordinate_id_mapping


VersionPart = TypeVar("VersionPart", bound="CopernicusMarinePart")


class CopernicusMarinePart(BaseModel):
    """
    Part of a dataset. Datasets can have multiple parts.
    Each part contains a distinct list of services and distinct data.
    """

    #: Name of the part.
    name: str
    #: List of services available for the part.
    services: list[CopernicusMarineService]
    #: Date when the part will be retired.
    retired_date: Optional[str]
    #: Date when the part will be/was released.
    released_date: Optional[str]
    #: Date (of the data) starting from which the data is currently being updated.
    #: If set, the data after this date may not be up to date.
    #: Only applies to ARCO series
    #: and not to the original files.
    arco_updating_start_date: Optional[str]
    #: Date when the arco series of the part were last updated.
    #: Only applies to ARCO series
    #: and not to the original files.
    arco_updated_date: Optional[str]
    #: TODO: ask if this should be hidden
    # = Field(..., exclude=True)
    # if yes: needs to modify the query builder
    url_metadata: str

    @classmethod
    def from_metadata_item(
        cls: Type[VersionPart],
        metadata_item: pystac.Item,
        part_name: str,
        url_metadata: str,
    ) -> Optional[VersionPart]:
        retired_date = metadata_item.properties.get("admp_retired_date")
        released_date = metadata_item.properties.get("admp_released_date")
        arco_updated_date = metadata_item.properties.get("admp_updated_data")
        arco_updating_start_date = metadata_item.properties.get(
            "admp_updating_start_date"
        )
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
            arco_updated_date=arco_updated_date,
            arco_updating_start_date=arco_updating_start_date,
            url_metadata=url_metadata,
        )

    def get_service_by_service_name(
        self, service_name: CopernicusMarineServiceNames
    ) -> CopernicusMarineService:
        return next(
            service
            for service in self.services
            if service.service_name == service_name
        )

    def get_coordinates(
        self,
    ) -> dict[
        str,
        tuple[
            CopernicusMarineCoordinate,
            list[str],
            list[CopernicusMarineServiceNames],
        ],
    ]:
        """
        Get the coordinates of the part as a dict.
        The dict has the coordinate IDs as keys and the values are tuples of:

        - the coordinate
        - variable_ids: list of variables the coordinate is associated with
        - service_names: list of service names the coordinate is associated with

        Returns
        -------
        dict
            The coordinates of the part and the associated variables and services
        """
        coordinates = {}
        variables = []
        services = []
        for service in self.services:
            services.append(service.service_name)
            for variable in service.variables:
                variables.append(variable.short_name)
                for coordinate in variable.coordinates:
                    coordinate_id = coordinate.coordinate_id
                    if coordinate_id not in coordinates:
                        coordinates[coordinate_id] = (
                            coordinate,
                            [variable.short_name],
                            [service.service_name],
                        )
                    else:
                        coordinates[coordinate_id][1].append(
                            variable.short_name
                        )
                        coordinates[coordinate_id][2].append(
                            service.service_name
                        )
        return coordinates


class CopernicusMarineVersion(BaseModel):
    """
    Version of a dataset. Datasets can have multiple versions.
    Usually around data releases.
    """

    #: Label of the version (eg: "latest", "202101").
    label: str
    #: List of parts of the version.
    parts: list[CopernicusMarinePart]

    def get_part(self, force_part: Optional[str]) -> CopernicusMarinePart:
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


# For internal use only
@dataclass
class DatasetItem:
    """
    Intermediate class for the dataset item.
    Used to parse the dataset item from the catalogue.
    """

    item_id: str
    parsed_id: str
    parsed_part: str
    parsed_version: str
    url: str
    stac_item: pystac.Item
    stac_json: dict
    product_doi: Optional[str]


class CopernicusMarineDataset(BaseModel):
    """
    Dataset of a product.
    Contains the dataset metadata and a list of versions.
    """

    #: The datasetID.
    dataset_id: str
    #: The dataset name.
    dataset_name: str
    #: Digital object identifier or doi from
    #: the product the dataset belongs to.
    digital_object_identifier: Optional[str]
    #: List of versions of the dataset.
    versions: list[CopernicusMarineVersion]

    def get_version(
        self, force_version: Optional[str]
    ) -> CopernicusMarineVersion:
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
        self,
        dataset_items: list[DatasetItem],
    ) -> None:
        all_versions = set()
        for dataset_item in dataset_items:
            dataset_version = dataset_item.parsed_version
            part = CopernicusMarinePart.from_metadata_item(
                dataset_item.stac_item,
                dataset_item.parsed_part,
                dataset_item.url,
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
                version = CopernicusMarineVersion(
                    label=dataset_version, parts=[part]
                )
                self.versions.append(version)


class CopernicusMarineProduct(BaseModel):
    """
    Product of the catalogue.
    Contains the product metadata and a list of datasets.
    """

    #: Title of the product.
    title: str
    #: ProductID.
    product_id: str
    #: Thumbnail url of the product.
    thumbnail_url: str
    #: Description of the product.
    description: Optional[str]
    #: Digital object identifier or doi of the product.
    digital_object_identifier: Optional[str]
    #: Sources of the product.
    sources: list[str]
    #: Processing level of the product.
    processing_level: Optional[str]
    #: Production center of the product.
    production_center: str
    #: Keywords of the product.
    keywords: Optional[list[str]]
    #: List of datasets of the product.
    datasets: list[CopernicusMarineDataset]


class CopernicusMarineCatalogue(BaseModel):
    """
    Catalogue of the Copernicus Marine service.
    You can find here the products of the catalogue and their metadata as the response of the describe command/function.
    """  # noqa

    #: List of products in the catalogue.
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

    Please verify that the requested part can be found in
    the result of the :func:`~copernicusmarine.describe` command
    for this specific dataset version and datasetID.
    If yes, please contact the User Support, (widget chat on
    `Copernicus Marine website <https://help.marine.copernicus.eu/en/>`_).
    """

    def __init__(self, version: CopernicusMarineVersion):
        message = f"No part found for version {version.label}"
        super().__init__(message)


class DatasetVersionNotFound(Exception):
    """
    Exception raised when the asked version of the dataset cannot be found.

    Please verify that the requested version can be found in
    the result of the :func:`~copernicusmarine.describe` command
    for this specific dataset.
    If yes, please contact the User Support, (widget chat on
    `Copernicus Marine website <https://help.marine.copernicus.eu/en/>`_).
    """

    def __init__(self, dataset: CopernicusMarineDataset):
        message = f"No version found for dataset {dataset.dataset_id}"
        super().__init__(message)


class DatasetNotFound(Exception):
    """
    Exception raised when the dataset is not found in the catalogue.

    Possible reasons:

    - The datasetID is incorrect and not present in the catalogue.
    - The dataset has been retired.

    Please verify that the datasetID can be found in
    the result of the :func:`~copernicusmarine.describe` command.
    If yes, please contact the User Support, (widget chat on
    `Copernicus Marine website <https://help.marine.copernicus.eu/en/>`_).
    """

    def __init__(self, dataset_id: str):
        message = (
            f"{dataset_id} "
            f"Please check that the dataset exists and "
            f"the input datasetID is correct."
        )
        super().__init__(message)


class DatasetIsNotPartOfTheProduct(Exception):
    """
    Exception raised when the dataset is not part of the product.

    If you request a datasetID and a productID
    at the same time with the describe command,
    please verify that the dataset is part of the product.
    """

    def __init__(self, dataset_id: str, product_id: str):
        message = (
            f"{dataset_id} not part of {product_id} "
            f"Please check that the dataset is part of the product and "
            f"the input datasetID is correct."
        )
        super().__init__(message)


class ProductNotFound(Exception):
    """
    Exception raised when the product is not found in the catalogue.

    Possible reasons:

    - The productID is incorrect and not present in the catalogue.
    - The product has been retired.

    Please verify that the productID can be found in
    the result of the :func:`~copernicusmarine.describe` command.
    If yes, please contact the User Support, (widget chat on
    `Copernicus Marine website <https://help.marine.copernicus.eu/en/>`_).
    """

    def __init__(self, product_id: str):
        message = (
            f"{product_id} "
            f"Please check that the product exists and "
            f"the input productID is correct."
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
        raise Exception(f"Could not parse datasetID: {full_dataset_id}")
    return dataset_name, version, part
