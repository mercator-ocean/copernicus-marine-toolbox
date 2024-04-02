import logging
import pathlib
from typing import Hashable, Iterable, Literal, Optional, Union

import click
import pandas
import xarray

from copernicusmarine.catalogue_parser.request_structure import SubsetRequest
from copernicusmarine.core_functions import sessions
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
from copernicusmarine.download_functions.subset_xarray import (
    date_to_datetime,
    subset,
)
from copernicusmarine.download_functions.utils import (
    FileFormat,
    get_filename,
    get_formatted_dataset_size_estimation,
)

logger = logging.getLogger("copernicus_marine_root_logger")


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
        _filter_dimensions(preferred_chunks, dataset.dims.keys())
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
    dataset_url: str,
    output_directory: pathlib.Path,
    output_filename: Optional[str],
    file_format: FileFormat,
    variables: Optional[list[str]],
    disable_progress_bar: bool,
    netcdf_compression_enabled: bool,
    netcdf_compression_level: Optional[int],
    netcdf3_compatible: bool,
    force_download: bool = False,
    overwrite_output_data: bool = False,
):
    dataset = _rechunk(
        open_dataset_from_arco_series(
            username=username,
            password=password,
            dataset_url=dataset_url,
            variables=variables,
            geographical_parameters=geographical_parameters,
            temporal_parameters=temporal_parameters,
            depth_parameters=depth_parameters,
            chunks="auto",
        )
    )

    dataset = add_copernicusmarine_version_in_dataset_attributes(dataset)

    filename = get_filename(output_filename, dataset, dataset_id, file_format)
    output_path = pathlib.Path(output_directory, filename)
    if not output_directory.is_dir():
        pathlib.Path.mkdir(output_directory, parents=True)

    if not force_download:
        logger.info(dataset)
        logger.info(
            "Estimated size of the dataset file is "
            f"{get_formatted_dataset_size_estimation(dataset)}."
        )
        click.confirm(
            FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE, default=True, abort=True
        )
    else:
        logger.info(
            "Estimated size of the dataset file is "
            f"{get_formatted_dataset_size_estimation(dataset)}."
        )
    logger.info("Writing to local storage. Please wait...")

    output_path = get_unique_filename(
        filepath=output_path, overwrite_option=overwrite_output_data
    )

    delayed = get_delayed_download(
        dataset,
        output_path,
        netcdf_compression_enabled,
        netcdf_compression_level,
        netcdf3_compatible,
    )
    download_delayed_dataset(delayed, disable_progress_bar)
    logger.info(f"Successfully downloaded to {output_path}")

    return output_path


def download_zarr(
    username: str,
    password: str,
    subset_request: SubsetRequest,
    dataset_id: str,
    disable_progress_bar: bool,
    dataset_valid_start_date: Optional[Union[str, int]],
):
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
        minimum_start_date = date_to_datetime(dataset_valid_start_date)
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
        vertical_dimension_as_originally_produced=subset_request.vertical_dimension_as_originally_produced,  # noqa
    )
    dataset_url = str(subset_request.dataset_url)
    output_directory = (
        subset_request.output_directory
        if subset_request.output_directory
        else pathlib.Path(".")
    )
    variables = subset_request.variables
    force_download = subset_request.force_download

    output_path = download_dataset(
        username=username,
        password=password,
        dataset_id=dataset_id,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        dataset_url=dataset_url,
        output_directory=output_directory,
        output_filename=subset_request.output_filename,
        file_format=subset_request.file_format,
        variables=variables,
        disable_progress_bar=disable_progress_bar,
        force_download=force_download,
        overwrite_output_data=subset_request.overwrite_output_data,
        netcdf_compression_enabled=subset_request.netcdf_compression_enabled,
        netcdf_compression_level=subset_request.netcdf_compression_level,
        netcdf3_compatible=subset_request.netcdf3_compatible,
    )
    return output_path


def open_dataset_from_arco_series(
    username: str,
    password: str,
    dataset_url: str,
    variables: Optional[list[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    chunks=Optional[Literal["auto"]],
) -> xarray.Dataset:
    dataset = sessions.open_zarr(
        dataset_url, chunks=chunks, copernicus_marine_username=username
    )
    dataset = subset(
        dataset=dataset,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
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
        chunks=chunks,
    )
    return dataset.to_dataframe()
