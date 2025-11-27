import json
import logging
import pathlib
from typing import Optional

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.exceptions import (
    ServiceNotSupported,
    WrongFormatRequested,
)
from copernicusmarine.core_functions.marine_datastore_config import (
    get_config_and_check_version_subset,
)
from copernicusmarine.core_functions.models import CommandType, ResponseSubset
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.services_utils import (
    RetrievalService,
    get_retrieval_service,
)
from copernicusmarine.core_functions.utils import get_unique_filepath
from copernicusmarine.download_functions.download_sparse import download_sparse
from copernicusmarine.download_functions.download_zarr import download_zarr
from copernicusmarine.download_functions.subset_xarray import (
    check_dataset_subset_bounds,
)

logger = logging.getLogger("copernicusmarine")


def subset_function(
    subset_request: SubsetRequest,
) -> ResponseSubset:
    retrieval_service = retrieve_metadata_and_check_request(subset_request)
    if retrieval_service.service_name not in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.PLATFORMSERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        raise ServiceNotSupported(retrieval_service.service_name)
    return download_zarr_or_sparse(
        subset_request=subset_request,
        retrieval_service=retrieval_service,
        tdqm_configuration={"disable": subset_request.disable_progress_bar},
    )


def retrieve_metadata_and_check_request(
    subset_request: SubsetRequest,
) -> RetrievalService:
    marine_datastore_config = get_config_and_check_version_subset(
        subset_request.staging
    )

    retrieval_service: RetrievalService = get_retrieval_service(
        request=subset_request,
        command_type=CommandType.SUBSET,
        marine_datastore_config=marine_datastore_config,
    )

    check_requested_area_time_valid(
        subset_request=subset_request,
        service_format=retrieval_service.service_format,
        dataset_part=retrieval_service.dataset_part.name,
    )
    check_dataset_subset_bounds(
        service=retrieval_service.service,
        part=retrieval_service.dataset_part,
        dataset_subset=subset_request,
        coordinates_selection_method=subset_request.coordinates_selection_method,
        axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
    )
    return retrieval_service


def download_zarr_or_sparse(
    subset_request: SubsetRequest,
    retrieval_service: RetrievalService,
    tdqm_configuration: dict,
) -> ResponseSubset:
    if retrieval_service.service_format == CopernicusMarineServiceFormat.ZARR:
        raise_when_all_dataset_requested(subset_request, False)
        if "file_format" not in subset_request.model_fields_set:
            subset_request.file_format = "netcdf"
        elif subset_request.file_format not in ["netcdf", "zarr"]:
            raise WrongFormatRequested(
                requested_format=subset_request.file_format,
                supported_formats=["netcdf", "zarr"],
            )
        logger.debug(
            f"Downloading data in {subset_request.file_format} format."
        )
        return download_zarr(
            subset_request=subset_request,
            dataset_url=retrieval_service.uri,
            dataset_valid_start_date=retrieval_service.dataset_valid_start_date,
            service=retrieval_service.service,
            is_original_grid=retrieval_service.is_original_grid,
            axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
            dataset_chunking=retrieval_service.dataset_chunking,
            tdqm_configuration=tdqm_configuration,
        )
    if (
        retrieval_service.service_format
        == CopernicusMarineServiceFormat.SQLITE
    ):
        raise_when_all_dataset_requested(subset_request, True)
        if "file_format" not in subset_request.model_fields_set:
            subset_request.file_format = "csv"
        elif subset_request.file_format not in ["parquet", "csv"]:
            raise WrongFormatRequested(
                requested_format=subset_request.file_format,
                supported_formats=["parquet", "csv"],
            )
        logger.debug(
            f"Downloading data in {subset_request.file_format} format."
        )
        if subset_request.coordinates_selection_method not in [
            "inside",
            "strict-inside",
        ]:
            logger.warning(
                f"coordinates-selection-method "
                f"{subset_request.coordinates_selection_method} "
                "is not supported for sparse data. "
                "Using 'inside' by default."
            )
        return download_sparse(
            subset_request.username,
            subset_request,
            retrieval_service.metadata_url,
            retrieval_service.service,
            retrieval_service.axis_coordinate_id_mapping,
            retrieval_service.product_doi,
            subset_request.disable_progress_bar,
        )
    else:
        raise ValueError("Dataset is missing some metadata. Cannot subset")


def create_subset_template() -> None:
    filename = pathlib.Path("subset_template.json")
    if filename.exists():
        get_unique_filepath(
            filepath=filename,
        )
    with open(filename, "w") as output_file:
        json.dump(
            {
                "dataset_id": "cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m",
                "start_datetime": "2023-10-01",
                "end_datetime": "2023-11-01",
                "minimum_longitude": -85,
                "maximum_longitude": -10,
                "minimum_latitude": 35,
                "maximum_latitude": 43,
                "minimum_depth": 1,
                "maximum_depth": 10,
                "variables": ["bottomT", "thetao"],
                "output_directory": "copernicusmarine_data",
                "service": None,
                "overwrite": False,
                "dry_run": False,
            },
            output_file,
            indent=4,
        )
    logger.info(f"Template created at: {filename}")


def raise_when_all_dataset_requested(
    subset_request: SubsetRequest, sparse_data: bool
) -> None:
    """
    Raise an error if the subset request is not a subsetting request
    and all the dataset would be downloaded.

    Parameters
    ----------
    subset_request: SubsetRequest
        The subset request.
    sparse_data: bool
        If the requested dataset is sparse data.
        If True, only subsetting on platform_ids is allowed.
        Otherwise it should raise.
    """
    if all(
        e is None
        for e in [
            subset_request.variables,
            subset_request.minimum_x,
            subset_request.maximum_x,
            subset_request.minimum_y,
            subset_request.maximum_y,
            subset_request.minimum_depth,
            subset_request.maximum_depth,
            subset_request.start_datetime,
            subset_request.end_datetime,
            (sparse_data and subset_request.platform_ids) or None,
        ]
    ):
        logger.info(
            "To retrieve a complete dataset, please use instead: "
            f"copernicusmarine get --dataset-id {subset_request.dataset_id}"
        )
        raise ValueError(
            "Missing subset option. Try 'copernicusmarine subset --help'."
        )


def check_requested_area_time_valid(
    subset_request: SubsetRequest,
    service_format: Optional[CopernicusMarineServiceFormat],
    dataset_part: str,
) -> None:
    is_original_grid = dataset_part == "originalGrid"
    x_axis_name = "x" if is_original_grid else "longitude"
    y_axis_name = "y" if is_original_grid else "latitude"
    if (
        subset_request.minimum_x is not None
        and subset_request.maximum_x is not None
        and subset_request.minimum_x > subset_request.maximum_x
    ):
        if (
            service_format == CopernicusMarineServiceFormat.ZARR
            and not is_original_grid
        ):
            logger.warning(
                "Minimum longitude greater than maximum longitude. "
                "Your selection will wrap around the 180° meridian: "
                f"Minimum longitude: {subset_request.minimum_x}, "
                f"Maximum longitude: {subset_request.maximum_x}."
            )
        else:
            raise ValueError(
                f"Minimum {x_axis_name} greater than maximum {x_axis_name}: "
                f"minimum-{x_axis_name} option must be smaller or equal to "
                f"maximum-{x_axis_name} for this dataset. "
                f"Minimum {x_axis_name}: {subset_request.minimum_x}, "
                f"Maximum {x_axis_name}: {subset_request.maximum_x}."
            )
    if (
        subset_request.minimum_y is not None
        and subset_request.maximum_y is not None
        and subset_request.minimum_y > subset_request.maximum_y
    ):
        raise ValueError(
            f"Minimum {y_axis_name} greater than maximum {y_axis_name}: "
            f"minimum-{y_axis_name} option must be smaller or equal to "
            f"maximum-{y_axis_name} for this dataset. "
            f"Minimum {y_axis_name}: {subset_request.minimum_y}, "
            f"Maximum {y_axis_name}: {subset_request.maximum_y}."
        )
    if (
        subset_request.minimum_depth is not None
        and subset_request.maximum_depth is not None
        and subset_request.minimum_depth > subset_request.maximum_depth
    ):
        raise ValueError(
            "Minimum depth greater than maximum depth: minimum-depth "
            "option must be smaller or equal to maximum-depth. "
            f"Minimum depth: {subset_request.minimum_depth}, "
            f"Maximum depth: {subset_request.maximum_depth}."
        )
    if (
        subset_request.start_datetime is not None
        and subset_request.end_datetime is not None
        and subset_request.start_datetime > subset_request.end_datetime
    ):
        raise ValueError(
            "Start datetime greater than end datetime: start-datetime "
            "option must be smaller or equal to end-datetime. "
            f"Start date: {subset_request.start_datetime}, "
            f"End date: {subset_request.end_datetime}."
        )
