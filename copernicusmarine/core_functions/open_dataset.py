import xarray

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceFormat,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.exceptions import (
    FormatNotSupported,
    ServiceNotSupported,
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.services_utils import RetrievalService
from copernicusmarine.core_functions.subset import (
    retrieve_metadata_and_check_request,
)
from copernicusmarine.download_functions.download_zarr import (
    get_dataset_and_parameters,
)


def open_dataset_function(
    subset_request: SubsetRequest,
) -> xarray.Dataset:
    retrieval_service: RetrievalService = retrieve_metadata_and_check_request(
        subset_request
    )
    if (
        retrieval_service.service.service_format
        == CopernicusMarineServiceFormat.SQLITE
    ):
        raise FormatNotSupported(
            CopernicusMarineServiceFormat.SQLITE.value,
            "open_dataset",
            "read_dataframe",
        )
    if retrieval_service.service_name not in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        raise ServiceNotSupported(retrieval_service.service_name)

    dataset, _, _ = get_dataset_and_parameters(
        subset_request=subset_request,
        dataset_url=retrieval_service.uri,
        axis_coordinate_id_mapping=retrieval_service.axis_coordinate_id_mapping,
        service=retrieval_service.service,
        dataset_chunking=retrieval_service.dataset_chunking,
        is_original_grid=retrieval_service.is_original_grid,
        dataset_valid_start_date=retrieval_service.dataset_valid_start_date,
    )

    return dataset
