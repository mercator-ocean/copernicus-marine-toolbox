from typing import Callable, Union

import pandas
import xarray

from copernicusmarine.catalogue_parser.catalogue_parser import (
    CopernicusMarineDatasetServiceType,
    parse_catalogue,
)
from copernicusmarine.catalogue_parser.request_structure import LoadRequest
from copernicusmarine.core_functions.credentials_utils import (
    get_username_password,
)
from copernicusmarine.core_functions.services_utils import (
    CommandType,
    RetrievalService,
    get_retrieval_service,
)
from copernicusmarine.core_functions.utils import (
    ServiceNotSupported,
    delete_cache_folder,
)
from copernicusmarine.download_functions.subset_xarray import (
    check_dataset_subset_bounds,
    date_to_datetime,
)


def load_data_object_from_load_request(
    load_request: LoadRequest,
    disable_progress_bar: bool,
    arco_series_load_function: Callable,
) -> Union[xarray.Dataset, pandas.DataFrame]:
    if load_request.overwrite_metadata_cache:
        delete_cache_folder()

    catalogue = parse_catalogue(
        no_metadata_cache=load_request.no_metadata_cache,
        disable_progress_bar=disable_progress_bar,
    )
    retrieval_service: RetrievalService = get_retrieval_service(
        catalogue=catalogue,
        dataset_id=load_request.dataset_id,
        dataset_url=load_request.dataset_url,
        force_dataset_version_label=load_request.force_dataset_version,
        force_dataset_part_label=load_request.force_dataset_part,
        force_service_type_string=load_request.force_service,
        command_type=CommandType.LOAD,
        dataset_subset=load_request.get_time_and_geographical_subset(),
    )
    username, password = get_username_password(
        load_request.username,
        load_request.password,
        load_request.credentials_file,
    )
    load_request.dataset_url = retrieval_service.uri
    check_dataset_subset_bounds(
        username=username,
        password=password,
        dataset_url=load_request.dataset_url,
        service_type=retrieval_service.service_type,
        dataset_subset=load_request.get_time_and_geographical_subset(),
        subset_method=load_request.subset_method,
        dataset_valid_date=retrieval_service.dataset_valid_start_date,
    )
    if retrieval_service.service_type in [
        CopernicusMarineDatasetServiceType.GEOSERIES,
        CopernicusMarineDatasetServiceType.TIMESERIES,
        CopernicusMarineDatasetServiceType.OMI_ARCO,
        CopernicusMarineDatasetServiceType.STATIC_ARCO,
    ]:
        if retrieval_service.dataset_valid_start_date:
            parsed_start_datetime = date_to_datetime(
                retrieval_service.dataset_valid_start_date
            )
            if (
                not load_request.temporal_parameters.start_datetime
                or load_request.temporal_parameters.start_datetime
                < parsed_start_datetime
            ):
                load_request.temporal_parameters.start_datetime = (
                    parsed_start_datetime
                )
        dataset = arco_series_load_function(
            username=username,
            password=password,
            dataset_url=load_request.dataset_url,
            variables=load_request.variables,
            geographical_parameters=load_request.geographical_parameters,
            temporal_parameters=load_request.temporal_parameters,
            depth_parameters=load_request.depth_parameters,
            chunks=None,
        )
    else:
        raise ServiceNotSupported(retrieval_service.service_type)
    return dataset
