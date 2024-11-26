from typing import Callable, Union

import pandas
import xarray

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceNames,
)
from copernicusmarine.catalogue_parser.request_structure import LoadRequest
from copernicusmarine.core_functions.credentials_utils import (
    get_username_password,
)
from copernicusmarine.core_functions.exceptions import ServiceNotSupported
from copernicusmarine.core_functions.services_utils import (
    CommandType,
    RetrievalService,
    get_retrieval_service,
)
from copernicusmarine.download_functions.download_arco_series import (
    get_optimum_dask_chunking,
)
from copernicusmarine.download_functions.subset_xarray import (
    check_dataset_subset_bounds,
    timestamp_or_datestring_to_datetime,
)


def load_data_object_from_load_request(
    load_request: LoadRequest,
    arco_series_load_function: Callable,
    chunks_factor_size_limit: int,
) -> Union[xarray.Dataset, pandas.DataFrame]:
    retrieval_service: RetrievalService = get_retrieval_service(
        dataset_id=load_request.dataset_id,
        force_dataset_version_label=load_request.force_dataset_version,
        force_dataset_part_label=load_request.force_dataset_part,
        force_service_name_or_short_name=load_request.force_service,
        command_type=CommandType.LOAD,
        dataset_subset=load_request.get_time_and_space_subset(),
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
        service_name=retrieval_service.service_name,
        dataset_subset=load_request.get_time_and_space_subset(),
        coordinates_selection_method=load_request.coordinates_selection_method,
        dataset_valid_date=retrieval_service.dataset_valid_start_date,
        is_original_grid=retrieval_service.is_original_grid,
    )
    if retrieval_service.service_name in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        if retrieval_service.dataset_valid_start_date:
            parsed_start_datetime = timestamp_or_datestring_to_datetime(
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
        optimum_dask_chunking = (
            get_optimum_dask_chunking(
                retrieval_service.service,
                load_request.geographical_parameters,
                load_request.temporal_parameters,
                load_request.depth_parameters,
                load_request.variables,
                chunks_factor_size_limit,
            )
            if chunks_factor_size_limit
            else None
        )
        dataset = arco_series_load_function(
            username=username,
            password=password,
            dataset_url=load_request.dataset_url,
            variables=load_request.variables,
            geographical_parameters=load_request.geographical_parameters,
            temporal_parameters=load_request.temporal_parameters,
            depth_parameters=load_request.depth_parameters,
            coordinates_selection_method=load_request.coordinates_selection_method,
            chunks=optimum_dask_chunking,
        )
    else:
        raise ServiceNotSupported(retrieval_service.service_name)
    return dataset
