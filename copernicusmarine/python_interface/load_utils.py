from typing import Callable, Union

import pandas as pd
import xarray

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.credentials_utils import (
    get_and_check_username_password,
)
from copernicusmarine.core_functions.exceptions import (
    FormatNotSupported,
    ServiceNotSupported,
)
from copernicusmarine.core_functions.marine_datastore_config import (
    get_config_and_check_version_subset,
)
from copernicusmarine.core_functions.models import CommandType
from copernicusmarine.core_functions.request_structure import LoadRequest
from copernicusmarine.core_functions.services_utils import (
    RetrievalService,
    get_retrieval_service,
)
from copernicusmarine.download_functions.download_sparse import (
    read_dataframe_sparse,
)
from copernicusmarine.download_functions.download_zarr import (
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
    command_type: CommandType,
) -> Union[xarray.Dataset, pd.DataFrame]:
    marine_datastore_config = get_config_and_check_version_subset(
        staging=False,
    )
    retrieval_service: RetrievalService = get_retrieval_service(
        dataset_id=load_request.dataset_id,
        force_dataset_version_label=load_request.force_dataset_version,
        force_dataset_part_label=load_request.force_dataset_part,
        force_service_name_or_short_name=load_request.force_service,
        command_type=command_type,
        dataset_subset=load_request.to_subset_request(),
        marine_datastore_config=marine_datastore_config,
    )
    username, password = get_and_check_username_password(
        load_request.username,
        load_request.password,
        load_request.credentials_file,
    )
    load_request.dataset_url = retrieval_service.uri
    check_dataset_subset_bounds(
        service=retrieval_service.service,
        part=retrieval_service.dataset_part,
        dataset_subset=load_request.to_subset_request(),
        coordinates_selection_method=load_request.coordinates_selection_method,
        axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
    )
    if (
        retrieval_service.service.service_format
        == CopernicusMarineServiceFormat.SQLITE
    ) and command_type == CommandType.OPEN_DATASET:
        raise FormatNotSupported(
            CopernicusMarineServiceFormat.SQLITE.value,
            command_type.value[0].value,
            CommandType.READ_DATAFRAME.value[0].value,
        )
    elif (
        retrieval_service.service.service_format
        == CopernicusMarineServiceFormat.SQLITE
    ) and command_type == CommandType.READ_DATAFRAME:
        return read_dataframe_sparse(
            username=username,
            subset_request=load_request.to_subset_request(),
            metadata_url=retrieval_service.metadata_url,
            service=retrieval_service.service,
            product_doi=retrieval_service.product_doi,
            disable_progress_bar=load_request.disable_progress_bar,
        )

    load_request.dataset_url = retrieval_service.uri
    check_dataset_subset_bounds(
        service=retrieval_service.service,
        part=retrieval_service.dataset_part,
        dataset_subset=load_request.to_subset_request(),
        coordinates_selection_method=load_request.coordinates_selection_method,
        axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
    )
    load_request.update_attributes(
        retrieval_service.axis_coordinate_id_mapping
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
        if chunks_factor_size_limit and retrieval_service.dataset_chunking:
            optimum_dask_chunking = get_optimum_dask_chunking(
                service=retrieval_service.service,
                variables=load_request.variables,
                dataset_chunking=retrieval_service.dataset_chunking,
                chunk_size_limit=chunks_factor_size_limit,
                axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
            )
        else:
            optimum_dask_chunking = None
        dataset = arco_series_load_function(
            username=username,
            password=password,
            dataset_url=load_request.dataset_url,
            variables=load_request.variables,
            geographical_parameters=load_request.geographical_parameters,
            temporal_parameters=load_request.temporal_parameters,
            depth_parameters=load_request.depth_parameters,
            coordinates_selection_method=load_request.coordinates_selection_method,
            optimum_dask_chunking=optimum_dask_chunking,
        )
    else:
        raise ServiceNotSupported(retrieval_service.service_name)
    return dataset
