import json
import logging
import pathlib
from datetime import datetime
from typing import List, Optional

from copernicusmarine.catalogue_parser.catalogue_parser import (
    CopernicusMarineDatasetServiceType,
    CopernicusMarineServiceFormat,
    parse_catalogue,
)
from copernicusmarine.catalogue_parser.request_structure import (
    SubsetRequest,
    convert_motu_api_request_to_structure,
    subset_request_from_file,
)
from copernicusmarine.core_functions.credentials_utils import (
    get_and_check_username_password,
)
from copernicusmarine.core_functions.models import SubsetMethod
from copernicusmarine.core_functions.services_utils import (
    CommandType,
    RetrievalService,
    get_retrieval_service,
    parse_dataset_id_and_service_and_suffix_path_from_url,
)
from copernicusmarine.core_functions.utils import (
    ServiceNotSupported,
    create_cache_directory,
    delete_cache_folder,
    get_unique_filename,
)
from copernicusmarine.core_functions.versions_verifier import VersionVerifier
from copernicusmarine.download_functions.download_arco_series import (
    download_zarr,
)
from copernicusmarine.download_functions.subset_xarray import (
    check_dataset_subset_bounds,
)
from copernicusmarine.download_functions.utils import FileFormat

logger = logging.getLogger("copernicus_marine_root_logger")


def subset_function(
    dataset_url: Optional[str],
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
    vertical_dimension_as_originally_produced: bool,
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
    subset_method: SubsetMethod,
    output_filename: Optional[str],
    file_format: FileFormat,
    force_service: Optional[str],
    request_file: Optional[pathlib.Path],
    output_directory: Optional[pathlib.Path],
    credentials_file: Optional[pathlib.Path],
    motu_api_request: Optional[str],
    force_download: bool,
    overwrite_output_data: bool,
    overwrite_metadata_cache: bool,
    no_metadata_cache: bool,
    disable_progress_bar: bool,
    staging: bool,
    netcdf_compression_enabled: bool,
    netcdf_compression_level: Optional[int],
    netcdf3_compatible: bool,
) -> pathlib.Path:
    VersionVerifier.check_version_subset(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for subset command. "
            "Data will come from the staging environment."
        )

    if overwrite_metadata_cache:
        delete_cache_folder()

    if not no_metadata_cache:
        create_cache_directory()

    if (
        netcdf_compression_level is not None
        and netcdf_compression_enabled is False
    ):
        raise ValueError(
            "You must provide --netcdf-compression-enabled if you want to use "
            "--netcdf-compression-level option"
        )

    subset_request = SubsetRequest()
    if request_file:
        subset_request = subset_request_from_file(request_file)
    if motu_api_request:
        motu_api_subset_request = convert_motu_api_request_to_structure(
            motu_api_request
        )
        subset_request.update(motu_api_subset_request.__dict__)
    request_update_dict = {
        "dataset_url": dataset_url,
        "dataset_id": dataset_id,
        "force_dataset_version": force_dataset_version,
        "force_dataset_part": force_dataset_part,
        "variables": variables,
        "minimum_longitude": minimum_longitude,
        "maximum_longitude": maximum_longitude,
        "minimum_latitude": minimum_latitude,
        "maximum_latitude": maximum_latitude,
        "minimum_depth": minimum_depth,
        "maximum_depth": maximum_depth,
        "vertical_dimension_as_originally_produced": vertical_dimension_as_originally_produced,  # noqa
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "subset_method": subset_method,
        "output_filename": output_filename,
        "file_format": file_format,
        "force_service": force_service,
        "output_directory": output_directory,
        "netcdf_compression_enabled": netcdf_compression_enabled,
        "netcdf_compression_level": netcdf_compression_level,
        "netcdf3_compatible": netcdf3_compatible,
    }
    subset_request.update(request_update_dict)
    username, password = get_and_check_username_password(
        username,
        password,
        credentials_file,
        no_metadata_cache=no_metadata_cache,
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
        if not subset_request.dataset_id:
            if subset_request.dataset_url:
                catalogue = parse_catalogue(
                    no_metadata_cache=no_metadata_cache,
                    disable_progress_bar=disable_progress_bar,
                    staging=staging,
                )
                (
                    dataset_id,
                    _,
                    _,
                ) = parse_dataset_id_and_service_and_suffix_path_from_url(
                    catalogue, subset_request.dataset_url
                )
            else:
                syntax_error = SyntaxError(
                    "Must specify at least one of "
                    "'dataset_url' or 'dataset_id' options"
                )
                raise syntax_error
        else:
            dataset_id = subset_request.dataset_id
        logger.info(
            "To retrieve a complete dataset, please use instead: "
            f"copernicusmarine get --dataset-id {dataset_id}"
        )
        raise ValueError(
            "Missing subset option. Try 'copernicusmarine subset --help'."
        )
    # Specific treatment for default values:
    # In order to not overload arguments with default values
    if force_download:
        subset_request.force_download = force_download
    if overwrite_output_data:
        subset_request.overwrite_output_data = overwrite_output_data

    catalogue = parse_catalogue(
        no_metadata_cache=no_metadata_cache,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    retrieval_service: RetrievalService = get_retrieval_service(
        catalogue,
        subset_request.dataset_id,
        subset_request.dataset_url,
        subset_request.force_dataset_version,
        subset_request.force_dataset_part,
        subset_request.force_service,
        CommandType.SUBSET,
        dataset_subset=subset_request.get_time_and_geographical_subset(),
    )
    subset_request.dataset_url = retrieval_service.uri
    check_dataset_subset_bounds(
        username=username,
        password=password,
        dataset_url=subset_request.dataset_url,
        service_type=retrieval_service.service_type,
        dataset_subset=subset_request.get_time_and_geographical_subset(),
        subset_method=subset_request.subset_method,
        dataset_valid_date=retrieval_service.dataset_valid_start_date,
    )
    logger.info(
        "Downloading using service "
        f"{retrieval_service.service_type.service_name.value}..."
    )
    if retrieval_service.service_type in [
        CopernicusMarineDatasetServiceType.GEOSERIES,
        CopernicusMarineDatasetServiceType.TIMESERIES,
        CopernicusMarineDatasetServiceType.OMI_ARCO,
        CopernicusMarineDatasetServiceType.STATIC_ARCO,
    ]:
        if (
            retrieval_service.service_format
            == CopernicusMarineServiceFormat.ZARR
        ):
            output_path = download_zarr(
                username,
                password,
                subset_request,
                retrieval_service.dataset_id,
                disable_progress_bar,
                retrieval_service.dataset_valid_start_date,
            )
    else:
        raise ServiceNotSupported(retrieval_service.service_type)
    return output_path


def create_subset_template() -> None:
    filename = get_unique_filename(
        filepath=pathlib.Path("subset_template.json"), overwrite_option=False
    )
    with open(filename, "w") as output_file:
        json.dump(
            {
                "dataset_id": "cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
                "start_datetime": "2023-10-07",
                "end_datetime": "2023-10-12",
                "minimum_longitude": -85,
                "maximum_longitude": -10,
                "minimum_latitude": 35,
                "maximum_latitude": 43,
                "minimum_depth": False,
                "maximum_depth": False,
                "variables": ["zos", "tob"],
                "output_directory": "copernicusmarine_data",
                "force_service": False,
                "force_download": False,
                "request_file": False,
                "motu_api_request": False,
                "overwrite_output_data": False,
                "overwrite_metadata_cache": False,
                "no_metadata_cache": False,
            },
            output_file,
            indent=4,
        )
    logger.info(f"Template created at: {filename}")
