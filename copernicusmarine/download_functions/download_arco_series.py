import logging
import pathlib
from typing import Hashable, Iterable, Literal, Optional, Union

import click
import pandas
import xarray

from copernicusmarine.catalogue_parser.models import CopernicusMarineService
from copernicusmarine.catalogue_parser.request_structure import SubsetRequest
from copernicusmarine.core_functions import custom_open_zarr
from copernicusmarine.core_functions.models import (
    CoordinatesSelectionMethod,
    ResponseSubset,
)
from copernicusmarine.core_functions.utils import (
    FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE,
    add_copernicusmarine_version_in_dataset_attributes,
    get_unique_filename,
)
from copernicusmarine.download_functions.common_download import (
    download_delayed_dataset,
    get_delayed_download,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    LatitudeParameters,
    LongitudeParameters,
    TemporalParameters,
)
from copernicusmarine.download_functions.subset_xarray import subset
from copernicusmarine.download_functions.utils import (
    FileFormat,
    get_approximation_size_data_downloaded,
    get_approximation_size_final_result,
    get_dataset_coordinates_extent,
    get_filename,
    get_message_formatted_dataset_size_estimation,
    timestamp_or_datestring_to_datetime,
)

logger = logging.getLogger("copernicusmarine")


def _rechunk(dataset: xarray.Dataset) -> xarray.Dataset:
    preferred_chunks = {}
    for variable in dataset:
        preferred_chunks = dataset[variable].encoding["preferred_chunks"]
        del dataset[variable].encoding["chunks"]

    if "depth" in preferred_chunks:
        preferred_chunks["elevation"] = preferred_chunks["depth"]
    elif "elevation" in preferred_chunks:
        preferred_chunks["depth"] = preferred_chunks["elevation"]

    return dataset.chunk(
        _filter_dimensions(preferred_chunks, dataset.sizes.keys())
    )


def _filter_dimensions(
    rechunks: dict[str, int], dimensions: Iterable[Hashable]
) -> dict[str, int]:
    return {k: v for k, v in rechunks.items() if k in dimensions}


def download_dataset(
    username: str,
    password: str,
    dataset_id: str,
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
    dataset_url: str,
    output_directory: pathlib.Path,
    output_filename: Optional[str],
    file_format: FileFormat,
    variables: Optional[list[str]],
    disable_progress_bar: bool,
    netcdf_compression_level: int,
    netcdf3_compatible: bool,
    service: CopernicusMarineService,
    dry_run: bool,
    force_download: bool,
    overwrite_output_data: bool,
) -> ResponseSubset:
    dataset = _rechunk(
        open_dataset_from_arco_series(
            username=username,
            password=password,
            dataset_url=dataset_url,
            variables=variables,
            geographical_parameters=geographical_parameters,
            temporal_parameters=temporal_parameters,
            depth_parameters=depth_parameters,
            coordinates_selection_method=coordinates_selection_method,
            chunks="auto",
        )
    )

    dataset = add_copernicusmarine_version_in_dataset_attributes(dataset)

    filename = get_filename(output_filename, dataset, dataset_id, file_format)
    output_path = pathlib.Path(output_directory, filename)
    final_result_size_estimation = get_approximation_size_final_result(dataset)
    data_needed_approximation = get_approximation_size_data_downloaded(
        dataset, service
    )
    message_formatted_dataset_size_estimation = (
        get_message_formatted_dataset_size_estimation(
            final_result_size_estimation, data_needed_approximation
        )
    )
    if not output_directory.is_dir():
        pathlib.Path.mkdir(output_directory, parents=True)
    if not force_download:
        logger.info(dataset)
        logger.info(message_formatted_dataset_size_estimation)
        click.confirm(
            FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE,
            default=True,
            abort=True,
            err=True,
        )
    else:
        logger.info(message_formatted_dataset_size_estimation)

    output_path = get_unique_filename(
        filepath=output_path, overwrite_option=overwrite_output_data
    )
    response = ResponseSubset(
        output=output_path,
        size=final_result_size_estimation,
        data_needed=data_needed_approximation,
        coordinates_extent=get_dataset_coordinates_extent(dataset),
    )

    if dry_run:
        return response

    logger.info("Writing to local storage. Please wait...")
    delayed = get_delayed_download(
        dataset,
        output_path,
        netcdf_compression_level,
        netcdf3_compatible,
    )
    download_delayed_dataset(delayed, disable_progress_bar)
    logger.info(f"Successfully downloaded to {output_path}")

    return response


def download_zarr(
    username: str,
    password: str,
    subset_request: SubsetRequest,
    dataset_id: str,
    disable_progress_bar: bool,
    dataset_valid_start_date: Optional[Union[str, int, float]],
    service: CopernicusMarineService,
) -> ResponseSubset:
    geographical_parameters = GeographicalParameters(
        latitude_parameters=LatitudeParameters(
            minimum_latitude=subset_request.minimum_latitude,
            maximum_latitude=subset_request.maximum_latitude,
        ),
        longitude_parameters=LongitudeParameters(
            minimum_longitude=subset_request.minimum_longitude,
            maximum_longitude=subset_request.maximum_longitude,
        ),
    )
    start_datetime = subset_request.start_datetime
    if dataset_valid_start_date:
        minimum_start_date = timestamp_or_datestring_to_datetime(
            dataset_valid_start_date
        )
        if (
            not subset_request.start_datetime
            or subset_request.start_datetime < minimum_start_date
        ):
            start_datetime = minimum_start_date

    temporal_parameters = TemporalParameters(
        start_datetime=start_datetime,
        end_datetime=subset_request.end_datetime,
    )
    depth_parameters = DepthParameters(
        minimum_depth=subset_request.minimum_depth,
        maximum_depth=subset_request.maximum_depth,
        vertical_dimension_output=subset_request.vertical_dimension_output,
    )
    dataset_url = str(subset_request.dataset_url)
    output_directory = (
        subset_request.output_directory
        if subset_request.output_directory
        else pathlib.Path(".")
    )
    variables = subset_request.variables
    force_download = subset_request.force_download

    response = download_dataset(
        username=username,
        password=password,
        dataset_id=dataset_id,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        coordinates_selection_method=subset_request.coordinates_selection_method,
        dataset_url=dataset_url,
        output_directory=output_directory,
        output_filename=subset_request.output_filename,
        file_format=subset_request.file_format,
        variables=variables,
        disable_progress_bar=disable_progress_bar,
        force_download=force_download,
        overwrite_output_data=subset_request.overwrite_output_data,
        netcdf_compression_level=subset_request.netcdf_compression_level,
        netcdf3_compatible=subset_request.netcdf3_compatible,
        dry_run=subset_request.dry_run,
        service=service,
    )
    return response


def open_dataset_from_arco_series(
    username: str,
    password: str,
    dataset_url: str,
    variables: Optional[list[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
    chunks=Optional[Literal["auto"]],
) -> xarray.Dataset:
    dataset = custom_open_zarr.open_zarr(
        dataset_url,
        chunks=chunks,
        copernicus_marine_username=username,
    )
    dataset = subset(
        dataset=dataset,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        coordinates_selection_method=coordinates_selection_method,
    )
    return dataset


def read_dataframe_from_arco_series(
    username: str,
    password: str,
    dataset_url: str,
    variables: Optional[list[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
    chunks: Optional[Literal["auto"]],
) -> pandas.DataFrame:
    dataset = open_dataset_from_arco_series(
        username=username,
        password=password,
        dataset_url=dataset_url,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        coordinates_selection_method=coordinates_selection_method,
        chunks=chunks,
    )
    return dataset.to_dataframe()
