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
from copernicusmarine.catalogue_parser.request_structure import (
    SubsetRequest,
    convert_motu_api_request_to_structure,
)
from copernicusmarine.core_functions.credentials_utils import (
    get_and_check_username_password,
)
from copernicusmarine.core_functions.exceptions import ServiceNotSupported
from copernicusmarine.core_functions.models import (
    CoordinatesSelectionMethod,
    ResponseSubset,
    VerticalAxis,
)
from copernicusmarine.core_functions.services_utils import (
    CommandType,
    RetrievalService,
    get_retrieval_service,
)
from copernicusmarine.core_functions.utils import get_unique_filename
from copernicusmarine.core_functions.versions_verifier import VersionVerifier
from copernicusmarine.download_functions.download_arco_series import (
    download_zarr,
)
from copernicusmarine.download_functions.subset_xarray import (
    check_dataset_subset_bounds,
)
from copernicusmarine.download_functions.utils import FileFormat

logger = logging.getLogger("copernicusmarine")


def subset_function(
    dataset_id: Optional[str],
    force_dataset_version: Optional[str],
    force_dataset_part: Optional[str],
    username: Optional[str],
    password: Optional[str],
    variables: Optional[List[str]],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_depth: Optional[float],
    maximum_depth: Optional[float],
    vertical_axis: VerticalAxis,
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
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
) -> ResponseSubset:
    VersionVerifier.check_version_subset(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for subset command. "
            "Data will come from the staging environment."
        )

    subset_request = SubsetRequest(dataset_id=dataset_id or "")
    if netcdf3_compatible:
        assert importlib.util.find_spec("netCDF4"), (
            "To use the NETCDF3_COMPATIBLE option you need to have 'netCDF4' "
            "installed. You can install it with 'pip install netcdf4'."
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
        "minimum_longitude": minimum_longitude,
        "maximum_longitude": maximum_longitude,
        "minimum_latitude": minimum_latitude,
        "maximum_latitude": maximum_latitude,
        "minimum_depth": minimum_depth,
        "maximum_depth": maximum_depth,
        "vertical_axis": vertical_axis,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "coordinates_selection_method": coordinates_selection_method,
        "output_filename": output_filename,
        "file_format": file_format,
        "force_service": force_service,
        "output_directory": output_directory,
        "netcdf_compression_level": netcdf_compression_level,
        "netcdf3_compatible": netcdf3_compatible,
        "dry_run": dry_run,
    }
    subset_request.update(request_update_dict)
    if not subset_request.dataset_id:
        raise ValueError("Please provide a dataset id for a subset request.")
    username, password = get_and_check_username_password(
        username,
        password,
        credentials_file,
    )
    if all(
        e is None
        for e in [
            subset_request.variables,
            subset_request.minimum_longitude,
            subset_request.maximum_longitude,
            subset_request.minimum_latitude,
            subset_request.maximum_latitude,
            subset_request.minimum_depth,
            subset_request.maximum_depth,
            subset_request.start_datetime,
            subset_request.end_datetime,
        ]
    ):
        logger.info(
            "To retrieve a complete dataset, please use instead: "
            f"copernicusmarine get --dataset-id {subset_request.dataset_id}"
        )
        raise ValueError(
            "Missing subset option. Try 'copernicusmarine subset --help'."
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
        dataset_subset=subset_request.get_time_and_space_subset(),
        staging=staging,
    )
    subset_request.dataset_url = retrieval_service.uri
    check_dataset_subset_bounds(
        username=username,
        password=password,
        dataset_url=subset_request.dataset_url,
        service_name=retrieval_service.service_name,
        dataset_subset=subset_request.get_time_and_space_subset(),
        coordinates_selection_method=subset_request.coordinates_selection_method,
        dataset_valid_date=retrieval_service.dataset_valid_start_date,
        is_original_grid=retrieval_service.is_original_grid,
    )
    if retrieval_service.service_name in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        if (
            retrieval_service.service_format
            == CopernicusMarineServiceFormat.ZARR
        ):
            response = download_zarr(
                username,
                password,
                subset_request,
                retrieval_service.dataset_id,
                disable_progress_bar,
                retrieval_service.dataset_valid_start_date,
                retrieval_service.service,
                None if chunk_size_limit == 0 else chunk_size_limit,
            )
    else:
        raise ServiceNotSupported(retrieval_service.service_name)
    return response


def create_subset_template() -> None:
    filename = pathlib.Path("subset_template.json")
    if filename.exists():
        get_unique_filename(
            filepath=filename,
        )
    with open(filename, "w") as output_file:
        json.dump(
            {
                "dataset_id": "cmems_mod_glo_phy_myint_0.083deg_P1M-m",
                "start_datetime": "2023-10-07",
                "end_datetime": "2023-10-12",
                "minimum_longitude": -85,
                "maximum_longitude": -10,
                "minimum_latitude": 35,
                "maximum_latitude": 43,
                "minimum_depth": 1,
                "maximum_depth": 10,
                "variables": ["so", "thetao"],
                "output_directory": "copernicusmarine_data",
                "force_service": False,
                "request_file": False,
                "motu_api_request": False,
                "overwrite": False,
                "dry_run": False,
            },
            output_file,
            indent=4,
        )
    logger.info(f"Template created at: {filename}")
