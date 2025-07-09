import logging
import os
import pathlib
import warnings
from copy import deepcopy
from typing import Optional, Tuple, Union

import pandas as pd
import xarray
import zarr

if zarr.__version__.startswith("2"):
    from zarr.storage import DirectoryStore

    # If zarr client is version 2 there are no
    # `zarr_format` argument
    ZARR_FORMAT = None
else:
    from zarr.storage import LocalStore as DirectoryStore

    # for client zarr>2 we need to specify the format
    # otherwise it uses zarr format 3 by default
    ZARR_FORMAT = 2

from tqdm.dask import TqdmCallback

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCoordinate,
    CopernicusMarineService,
)
from copernicusmarine.core_functions import custom_open_zarr
from copernicusmarine.core_functions.exceptions import (
    NetCDFCompressionNotAvailable,
)
from copernicusmarine.core_functions.models import (
    CoordinatesSelectionMethod,
    DatasetChunking,
    FileStatus,
    ResponseSubset,
    StatusCode,
    StatusMessage,
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.utils import (
    add_copernicusmarine_version_in_dataset_attributes,
    get_unique_filepath,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    TemporalParameters,
)
from copernicusmarine.download_functions.subset_xarray import subset
from copernicusmarine.download_functions.utils import (
    FileFormat,
    get_approximation_size_data_downloaded,
    get_approximation_size_final_result,
    get_dataset_coordinates_extent,
    get_filename,
    timestamp_or_datestring_to_datetime,
)

logger = logging.getLogger("copernicusmarine")


def download_dataset(
    username: str,
    password: str,
    dataset_id: str,
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
    axis_coordinate_id_mapping: dict[str, str],
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
    overwrite: bool,
    chunk_size_limit: int,
    skip_existing: bool,
    dataset_chunking: Optional[DatasetChunking],
) -> ResponseSubset:
    if chunk_size_limit and dataset_chunking:
        optimum_dask_chunking = get_optimum_dask_chunking(
            service=service,
            variables=variables,
            dataset_chunking=dataset_chunking,
            chunk_size_limit=chunk_size_limit,
            axis_coordinate_id_mapping=axis_coordinate_id_mapping,
        )
    else:
        optimum_dask_chunking = None

    logger.debug(f"Dask chunking selected: {optimum_dask_chunking}")
    dataset = open_dataset_from_arco_series(
        username=username,
        password=password,
        dataset_url=dataset_url,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        coordinates_selection_method=coordinates_selection_method,
        optimum_dask_chunking=optimum_dask_chunking,
    )

    dataset = add_copernicusmarine_version_in_dataset_attributes(dataset)

    if depth_parameters.vertical_axis == "elevation":
        axis_coordinate_id_mapping["z"] = "elevation"

    filename = get_filename(
        output_filename,
        dataset,
        dataset_id,
        file_format,
        axis_coordinate_id_mapping,
        geographical_parameters,
    )
    output_path = pathlib.Path(output_directory, filename)
    final_result_size_estimation = get_approximation_size_final_result(
        dataset, axis_coordinate_id_mapping
    )
    if dataset_chunking:
        data_needed_approximation = get_approximation_size_data_downloaded(
            dataset, dataset_chunking
        )
    else:
        data_needed_approximation = None

    if not output_directory.is_dir():
        pathlib.Path.mkdir(output_directory, parents=True)

    if not overwrite and not skip_existing:
        output_path = get_unique_filepath(
            filepath=output_path,
        )
    logger.debug(f"Xarray Dataset: {dataset}")
    response = ResponseSubset(
        file_path=output_path,
        output_directory=output_directory,
        filename=output_path.name,
        file_size=final_result_size_estimation,
        data_transfer_size=data_needed_approximation,
        variables=list(dataset.data_vars),
        coordinates_extent=get_dataset_coordinates_extent(
            dataset, axis_coordinate_id_mapping
        ),
        status=StatusCode.SUCCESS,
        message=StatusMessage.SUCCESS,
        file_status=FileStatus.DOWNLOADED,
    )

    if dry_run:
        response.status = StatusCode.DRY_RUN
        response.message = StatusMessage.DRY_RUN
        return response
    elif skip_existing and os.path.exists(output_path):
        response.file_status = FileStatus.IGNORED
        return response

    logger.info("Starting download. Please wait...")
    if disable_progress_bar:
        _save_dataset_locally(
            dataset,
            output_path,
            netcdf_compression_level,
            netcdf3_compatible,
        )
    else:
        with TqdmCallback():
            _save_dataset_locally(
                dataset,
                output_path,
                netcdf_compression_level,
                netcdf3_compatible,
            )
    logger.info(f"Successfully downloaded to {output_path}")
    if overwrite:
        response.status = StatusCode.SUCCESS
        response.message = StatusMessage.SUCCESS
        response.file_status = FileStatus.OVERWRITTEN

    return response


def download_zarr(
    username: str,
    password: str,
    subset_request: SubsetRequest,
    dataset_id: str,
    disable_progress_bar: bool,
    dataset_valid_start_date: Optional[Union[str, int, float]],
    service: CopernicusMarineService,
    is_original_grid: bool,
    axis_coordinate_id_mapping: dict[str, str],
    chunk_size_limit: int,
    dataset_chunking: Optional[DatasetChunking],
) -> ResponseSubset:
    geographical_parameters = subset_request.get_geographical_parameters(
        axis_coordinate_id_mapping, is_original_grid
    )
    if dataset_valid_start_date:
        minimum_start_date = timestamp_or_datestring_to_datetime(
            dataset_valid_start_date
        )
        if (
            not subset_request.start_datetime
            or subset_request.start_datetime < minimum_start_date
        ):
            subset_request.start_datetime = minimum_start_date

    temporal_parameters = subset_request.get_temporal_parameters(
        axis_coordinate_id_mapping
    )
    depth_parameters = subset_request.get_depth_parameters(
        axis_coordinate_id_mapping
    )
    dataset_url = str(subset_request.dataset_url)
    output_directory = (
        subset_request.output_directory
        if subset_request.output_directory
        else pathlib.Path(".")
    )
    variables = subset_request.variables

    response = download_dataset(
        username=username,
        password=password,
        dataset_id=dataset_id,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        coordinates_selection_method=subset_request.coordinates_selection_method,
        axis_coordinate_id_mapping=axis_coordinate_id_mapping,
        dataset_url=dataset_url,
        output_directory=output_directory,
        output_filename=subset_request.output_filename,
        file_format=subset_request.file_format,
        variables=variables,
        disable_progress_bar=disable_progress_bar,
        overwrite=subset_request.overwrite,
        netcdf_compression_level=subset_request.netcdf_compression_level,
        netcdf3_compatible=subset_request.netcdf3_compatible,
        dry_run=subset_request.dry_run,
        service=service,
        chunk_size_limit=chunk_size_limit,
        skip_existing=subset_request.skip_existing,
        dataset_chunking=dataset_chunking,
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
    optimum_dask_chunking: Optional[dict[str, int]],
) -> xarray.Dataset:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        dataset = custom_open_zarr.open_zarr(
            dataset_url,
            chunks=optimum_dask_chunking,
            copernicus_marine_username=username,
        )
    for variable in dataset:
        del dataset[variable].encoding["chunks"]
    dataset = subset(
        dataset=dataset,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        coordinates_selection_method=coordinates_selection_method,
    )
    if "depth" in dataset.coords and optimum_dask_chunking:
        optimum_chunks_depth = deepcopy(optimum_dask_chunking)
        if "elevation" in optimum_chunks_depth:
            optimum_chunks_depth["depth"] = optimum_chunks_depth["elevation"]
            del optimum_chunks_depth["elevation"]
        dataset = dataset.chunk(optimum_chunks_depth)
    elif optimum_dask_chunking:
        dataset = dataset.chunk(optimum_dask_chunking)
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
    optimum_dask_chunking: Optional[dict[str, int]],
) -> pd.DataFrame:
    dataset = open_dataset_from_arco_series(
        username=username,
        password=password,
        dataset_url=dataset_url,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        coordinates_selection_method=coordinates_selection_method,
        optimum_dask_chunking=optimum_dask_chunking,
    )
    return dataset.to_dataframe()


def get_coordinates_dask_and_zarr_chunks_info(
    service: CopernicusMarineService,
    variables: Optional[list[str]],
    dataset_chunking: DatasetChunking,
) -> Tuple[dict, dict]:
    """
    Return
    -------

    Tuple[dict, dict]
        A tuple containing:
        - a dict with the maximum dask chunk factor for each coordinate id.
          It tells us how many times we can multiply the zarr chunking per coordinate.
        - a dict with the zarr chunking for each coordinate id. i.e. the number of values per zarr chunk.
    """  # noqa
    set_variables = set(variables) if variables else set()
    coordinate_for_subset: dict[str, CopernicusMarineCoordinate] = {}
    for variable in service.variables:
        if (
            not set_variables
            or variable.short_name in set_variables
            or variable.standard_name in set_variables
        ):
            for coordinate in variable.coordinates:
                coordinate_name = coordinate.coordinate_id
                if coordinate.coordinate_id not in coordinate_for_subset:
                    coordinate_for_subset[coordinate_name] = coordinate

    max_dask_chunk_factor = {}
    coordinate_zarr_chunk_length = {}
    for coordinate_id, coordinate in coordinate_for_subset.items():
        chunking_length = coordinate.chunking_length
        if chunking_length is None:
            continue
        coordinate_zarr_chunk_length[coordinate_id] = int(chunking_length)
        number_of_zarr_chunks_needed = (
            dataset_chunking.get_number_chunks_coordinate(coordinate_id)
        )
        if number_of_zarr_chunks_needed is None:
            continue
        max_dask_chunk_factor[coordinate_id] = number_of_zarr_chunks_needed
    return (
        max_dask_chunk_factor,
        coordinate_zarr_chunk_length,
    )


def get_optimum_dask_chunking(
    service: CopernicusMarineService,
    variables: Optional[list[str]],
    dataset_chunking: DatasetChunking,
    chunk_size_limit: int,
    axis_coordinate_id_mapping: dict[str, str],
) -> Optional[dict[str, int]]:
    """
    We have some problems with overly big dask graphs (we think) that introduces huge overheads
    and memory usage. We are trying to find the optimum chunking for dask arrays.

    By default, the chunking is the zarr chunking ie 1MB for each tile.
    The rule of thumb for dask chunking is to have chunks of size 100MB.

    To avoid downloading too much data we should also be careful not to
    increase the size of the chunks more than the size of the data we are interested in.
    Eg: you want 1 time point, increasing size of the chunk on time dimension by x
    will lead to downloading x times more data than needed.

    Knowing that, we should cap the size of the chunk to 100MB and use multiples of the zarr chunking.

    If the factors sum up to less than 100, no chunking is needed.

    Returns
    -------

    Optional[dict[str, int]]
        A dictionary with the optimum dask chunking for each coordinate id. In the form:
        {
            "longitude": 2,
            "latitude": 2,
            "time": 1,
            "elevation": 1, # forced to work with ARCO standard
        }
        Can also be None if the dataset is not large enough to require dask chunking.
    """  # noqa
    (
        max_dask_chunk_factor,
        coordinate_zarr_chunk_length,
    ) = get_coordinates_dask_and_zarr_chunks_info(
        service, variables, dataset_chunking
    )
    zarr_chunks_to_download = dataset_chunking.number_chunks
    # it seems that dask chunks increases more
    # with the number of variables
    chunks_and_variables_size = zarr_chunks_to_download * len(
        dataset_chunking.chunking_per_variable
    )
    if chunk_size_limit == -1:
        # TODO: investigate in depth what are the optimal values.
        # For now, we use the default values that seem to work well
        # for our examples but we should investigate more.
        if zarr_chunks_to_download <= 50:
            return None
        elif chunks_and_variables_size <= 1500:
            chunk_size_limit = 20
        elif chunks_and_variables_size <= 4000:
            chunk_size_limit = 50
        else:
            chunk_size_limit = 100
    logger.debug(f"Chunk size limit: {chunk_size_limit}")
    logger.debug(f"Zarr chunking: {coordinate_zarr_chunk_length}")
    logger.debug(f"Max dask chunk factor: {max_dask_chunk_factor}")
    optimum_dask_factors = _get_optimum_factors(
        max_dask_chunk_factor,
        chunk_size_limit,
        axis_coordinate_id_mapping,
    )
    optimum_dask_chunking = {
        coordinate_id: coordinate_zarr_chunk_length[coordinate_id]
        * optimum_dask_factors[coordinate_id]
        for coordinate_id in optimum_dask_factors
    }
    logger.debug(f"Optimum dask chunking: {optimum_dask_chunking}")

    if (
        "z" in axis_coordinate_id_mapping
        and (z_coordinate := axis_coordinate_id_mapping["z"]) != "elevation"
        and z_coordinate in optimum_dask_chunking
    ):
        optimum_dask_chunking["elevation"] = optimum_dask_chunking[
            z_coordinate
        ]
        del optimum_dask_chunking[z_coordinate]
    return optimum_dask_chunking


def _product(iterable) -> int:
    result = 1
    for i in iterable:
        result *= i
    return result


def _get_optimum_factors(
    coordinate_max_dask_chunk_factor: dict[str, int],
    limit: int,
    axis_coordinate_id_mapping: dict[str, str],
) -> dict[str, int]:
    optimum_factors = {
        coordinate_name: 1
        for coordinate_name in coordinate_max_dask_chunk_factor
    }
    coordinate_selection_pool = {
        coordinate_name
        for coordinate_name, max_factor in coordinate_max_dask_chunk_factor.items()
        if max_factor > 1
    }
    while (
        _product(optimum_factors.values()) < limit
        and coordinate_selection_pool
    ):
        selected_coordinate = max(
            coordinate_selection_pool,
            key=lambda x: (
                coordinate_max_dask_chunk_factor[x],
                x in axis_coordinate_id_mapping.get("z", ""),
                x in axis_coordinate_id_mapping.get("t", ""),
                x in axis_coordinate_id_mapping.get("y", ""),
                x in axis_coordinate_id_mapping.get("x", ""),
            ),
        )

        optimum_factors[selected_coordinate] += 1
        if optimum_factors[
            selected_coordinate
        ] == coordinate_max_dask_chunk_factor[selected_coordinate] or (
            len(coordinate_selection_pool) > 1
            and optimum_factors[selected_coordinate] >= limit // 2
        ):
            coordinate_selection_pool.remove(selected_coordinate)
    return optimum_factors


def _save_dataset_locally(
    dataset: xarray.Dataset,
    output_path: pathlib.Path,
    netcdf_compression_level: int,
    netcdf3_compatible: bool,
):
    if output_path.suffix == ".zarr":
        if netcdf_compression_level > 0:
            raise NetCDFCompressionNotAvailable(
                "--netcdf-compression-level option cannot be used when "
                "writing to ZARR"
            )
        _download_dataset_as_zarr(dataset, output_path)
    else:
        _download_dataset_as_netcdf(
            dataset,
            output_path,
            netcdf_compression_level,
            netcdf3_compatible,
        )


def _download_dataset_as_zarr(
    dataset: xarray.Dataset, output_path: pathlib.Path
):
    logger.debug("Writing dataset to Zarr")
    store = DirectoryStore(output_path)
    if ZARR_FORMAT is None:
        return dataset.to_zarr(store=store, mode="w")
    else:
        return dataset.to_zarr(store=store, mode="w", zarr_format=ZARR_FORMAT)


def _download_dataset_as_netcdf(
    dataset: xarray.Dataset,
    output_path: pathlib.Path,
    netcdf_compression_level: int,
    netcdf3_compatible: bool,
):
    logger.debug("Writing dataset to NetCDF")
    for coord in dataset.coords:
        dataset[coord].encoding["_FillValue"] = None
    if netcdf_compression_level > 0:
        logger.info(
            f"NetCDF compression enabled with level {netcdf_compression_level}"
        )
        comp = dict(
            zlib=True,
            complevel=netcdf_compression_level,
            contiguous=False,
            shuffle=True,
        )
        keys_to_keep = {
            "scale_factor",
            "add_offset",
            "dtype",
            "_FillValue",
            "units",
        }
        encoding = {
            name: {
                **{
                    key: value
                    for key, value in var.encoding.items()
                    if key in keys_to_keep
                },
                **comp,
            }
            for name, var in dataset.data_vars.items()
        }
    else:
        encoding = None

    xarray_download_format = "NETCDF3_CLASSIC" if netcdf3_compatible else None
    engine = "h5netcdf" if not netcdf3_compatible else "netcdf4"
    return dataset.to_netcdf(
        output_path,
        mode="w",
        encoding=encoding,
        format=xarray_download_format,
        engine=engine,
    )
