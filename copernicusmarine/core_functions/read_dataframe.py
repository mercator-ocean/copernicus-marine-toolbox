import pandas as pd

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.exceptions import ServiceNotSupported
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.services_utils import RetrievalService
from copernicusmarine.core_functions.subset import (
    retrieve_metadata_and_check_request,
)
from copernicusmarine.download_functions.download_sparse import (
    read_dataframe_sparse,
)
from copernicusmarine.download_functions.download_zarr import (
    get_dataset_and_parameters,
)


def read_dataframe_function(
    subset_request: SubsetRequest,
) -> pd.DataFrame:
    retrieval_service: RetrievalService = retrieve_metadata_and_check_request(
        subset_request
    )
    if retrieval_service.service_name not in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.PLATFORMSERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        raise ServiceNotSupported(retrieval_service.service_name)
    if (
        retrieval_service.service.service_format
        == CopernicusMarineServiceFormat.SQLITE
    ):
        return read_dataframe_sparse(
            username=subset_request.username,
            subset_request=subset_request,
            metadata_url=retrieval_service.metadata_url,
            service=retrieval_service.service,
            product_doi=retrieval_service.product_doi,
            disable_progress_bar=subset_request.disable_progress_bar,
        )
    else:
        dataset, _, _ = get_dataset_and_parameters(
            subset_request=subset_request,
            dataset_url=retrieval_service.uri,
            axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
            service=retrieval_service.service,
            dataset_chunking=retrieval_service.dataset_chunking,
            is_original_grid=retrieval_service.is_original_grid,
            dataset_valid_start_date=retrieval_service.dataset_valid_start_date,
        )
        return dataset.to_dataframe()
