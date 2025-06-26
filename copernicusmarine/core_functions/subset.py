import importlib.util
import json
import logging
import pathlib
from datetime import datetime
from typing import List, Optional

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.credentials_utils import (
    get_and_check_username_password,
)
from copernicusmarine.core_functions.exceptions import ServiceNotSupported
from copernicusmarine.core_functions.marine_datastore_config import (
    get_config_and_check_version_subset,
)
from copernicusmarine.core_functions.models import (
    CommandType,
    CoordinatesSelectionMethod,
    ResponseSubset,
    VerticalAxis,
)
from copernicusmarine.core_functions.request_structure import (
    SubsetRequest,
    convert_motu_api_request_to_structure,
)
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
from copernicusmarine.download_functions.utils import FileFormat
from copernicusmarine.versioner import __version__ as copernicusmarine_version

logger = logging.getLogger("copernicusmarine")


def subset_function(
    dataset_id: Optional[str],
    force_dataset_version: Optional[str],
    force_dataset_part: Optional[str],
    username: Optional[str],
    password: Optional[str],
    variables: Optional[List[str]],
    minimum_x: Optional[float],
    maximum_x: Optional[float],
    minimum_y: Optional[float],
    maximum_y: Optional[float],
    minimum_depth: Optional[float],
    maximum_depth: Optional[float],
    vertical_axis: VerticalAxis,
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
    platform_ids: Optional[List[str]],
    coordinates_selection_method: CoordinatesSelectionMethod,
    output_filename: Optional[str],
    file_format: FileFormat,
    force_service: Optional[str],
    request_file: Optional[pathlib.Path],
    output_directory: Optional[pathlib.Path],
    credentials_file: Optional[pathlib.Path],
    motu_api_request: Optional[str],
    overwrite: bool,
    skip_existing: bool,
    dry_run: bool,
    disable_progress_bar: bool,
    staging: bool,
    netcdf_compression_level: int,
    netcdf3_compatible: bool,
    chunk_size_limit: int,
    raise_if_updating: bool,
) -> ResponseSubset:
    marine_datastore_config = get_config_and_check_version_subset(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for subset command. "
            "Data will come from the staging environment."
        )

    subset_request = SubsetRequest(dataset_id=dataset_id or "")
    if netcdf3_compatible:
        documentation_url = (
            f"https://toolbox-docs.marine.copernicus.eu"
            f"/en/v{copernicusmarine_version}/installation.html#dependencies"
        )
        assert importlib.util.find_spec("netCDF4"), (
            "To enable the NETCDF3_COMPATIBLE option, the 'netCDF4' "
            f"package is required. "
            f"Please see {documentation_url}."
        )
    if request_file:
        subset_request.from_file(request_file)
    if motu_api_request:
        motu_api_subset_request = convert_motu_api_request_to_structure(
            motu_api_request
        )
        subset_request.update(motu_api_subset_request.__dict__)
    request_update_dict = {
        "force_dataset_version": force_dataset_version,
        "force_dataset_part": force_dataset_part,
        "variables": variables,
        "minimum_x": minimum_x,
        "maximum_x": maximum_x,
        "minimum_y": minimum_y,
        "maximum_y": maximum_y,
        "minimum_depth": minimum_depth,
        "maximum_depth": maximum_depth,
        "vertical_axis": vertical_axis,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "platform_ids": platform_ids,
        "coordinates_selection_method": coordinates_selection_method,
        "output_filename": output_filename,
        "file_format": file_format,
        "force_service": force_service,
        "output_directory": output_directory,
        "netcdf_compression_level": netcdf_compression_level,
        "netcdf3_compatible": netcdf3_compatible,
        "dry_run": dry_run,
        "raise_if_updating": raise_if_updating,
    }
    subset_request.update(request_update_dict)
    if not subset_request.dataset_id:
        raise ValueError("Please provide a dataset id for a subset request.")
    username, password = get_and_check_username_password(
        username,
        password,
        credentials_file,
    )
    # Specific treatment for default values:
    # In order to not overload arguments with default values
    if overwrite:
        subset_request.overwrite = overwrite
    if skip_existing:
        subset_request.skip_existing = skip_existing

    retrieval_service: RetrievalService = get_retrieval_service(
        subset_request.dataset_id,
        subset_request.force_dataset_version,
        subset_request.force_dataset_part,
        subset_request.force_service,
        CommandType.SUBSET,
        dataset_subset=subset_request,
        platform_ids_subset=bool(subset_request.platform_ids),
        marine_datastore_config=marine_datastore_config,
    )
    subset_request.dataset_url = retrieval_service.uri
    # TODO: Add check for insitu datasets
    check_dataset_subset_bounds(
        service=retrieval_service.service,
        part=retrieval_service.dataset_part,
        dataset_subset=subset_request,
        coordinates_selection_method=subset_request.coordinates_selection_method,
        axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
    )
    if retrieval_service.service_name in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.PLATFORMSERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        if (
            retrieval_service.service_format
            == CopernicusMarineServiceFormat.ZARR
        ):
            raise_when_all_dataset_requested(subset_request, False)
            if subset_request.file_format not in ["netcdf", "zarr"]:
                raise ValueError(
                    f"{subset_request.file_format} is not a valid format "
                    "for this dataset. "
                    "Available format for this dataset is 'netcdf' or 'zarr'."
                )
            response = download_zarr(
                username,
                password,
                subset_request,
                retrieval_service.dataset_id,
                disable_progress_bar,
                retrieval_service.dataset_valid_start_date,
                retrieval_service.service,
                retrieval_service.is_original_grid,
                retrieval_service.axis_coordinate_id_mapping,
                chunk_size_limit,
                retrieval_service.dataset_chunking,
            )
        if (
            retrieval_service.service_format
            == CopernicusMarineServiceFormat.SQLITE
        ):
            raise_when_all_dataset_requested(subset_request, True)
            if subset_request.file_format not in ["parquet", "csv"]:
                logger.debug(
                    "Using 'csv' format by default. "
                    "'parquet' format can also be set with 'file-format' option."
                )
                subset_request.file_format = "csv"
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
            response = download_sparse(
                username,
                subset_request,
                retrieval_service.metadata_url,
                retrieval_service.service,
                retrieval_service.axis_coordinate_id_mapping,
                retrieval_service.product_doi,
                disable_progress_bar,
            )
    else:
        raise ServiceNotSupported(retrieval_service.service_name)
    return response


def create_subset_template() -> None:
    filename = pathlib.Path("subset_template.json")
    if filename.exists():
        get_unique_filepath(
            filepath=filename,
        )
    with open(filename, "w") as output_file:
        json.dump(
            {
                "dataset_id": "cmems_mod_glo_phy_myint_0.083deg_P1M-m",
                "start_datetime": "2023-10-01",
                "end_datetime": "2023-11-01",
                "minimum_longitude": -85,
                "maximum_longitude": -10,
                "minimum_latitude": 35,
                "maximum_latitude": 43,
                "minimum_depth": 1,
                "maximum_depth": 10,
                "variables": ["so", "thetao"],
                "output_directory": "copernicusmarine_data",
                "service": False,
                "request_file": False,
                "motu_api_request": False,
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
        If yes, only subsetting on platform_ids is allowed.
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
