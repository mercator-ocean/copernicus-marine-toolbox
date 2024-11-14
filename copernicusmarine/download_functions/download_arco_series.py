import logging
import os
import pathlib
from typing import Hashable, Iterable, Literal, Optional, Union

import pandas
import xarray
import zarr
from tqdm.dask import TqdmCallback

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCoordinate,
    CopernicusMarineService,
)
from copernicusmarine.catalogue_parser.request_structure import SubsetRequest
from copernicusmarine.core_functions import custom_open_zarr
from copernicusmarine.core_functions.exceptions import (
    NetCDFCompressionNotAvailable,
)
from copernicusmarine.core_functions.models import (
    CoordinatesSelectionMethod,
    FileStatus,
    ResponseSubset,
    StatusCode,
    StatusMessage,
)
from copernicusmarine.core_functions.utils import (
    add_copernicusmarine_version_in_dataset_attributes,
    get_unique_filename,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    LatitudeParameters,
    LongitudeParameters,
    TemporalParameters,
)
from copernicusmarine.download_functions.subset_xarray import (
    COORDINATES_LABEL,
    apply_longitude_modulus,
    subset,
)
from copernicusmarine.download_functions.utils import (
    FileFormat,
    get_approximation_size_data_downloaded,
    get_approximation_size_final_result,
    get_dataset_coordinates_extent,
    get_filename,
    get_message_formatted_dataset_size_estimation,
    get_number_of_chunks_for_coordinate,
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
    overwrite: bool,
    chunk_size_limit: Optional[int],
    skip_existing: bool,
) -> ResponseSubset:
    if chunk_size_limit:
        optimum_dask_chunking = get_optimum_dask_chunking(
            service,
            geographical_parameters,
            temporal_parameters,
            depth_parameters,
            variables,
            chunk_size_limit,
        )
    else:
        optimum_dask_chunking = None
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
            chunks=optimum_dask_chunking,  # type: ignore
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
    if dry_run:
        logger.info(dataset)
    else:
        logger.debug(dataset)
    logger.info(message_formatted_dataset_size_estimation)

    if not overwrite and not skip_existing:
        output_path = get_unique_filename(
            filepath=output_path,
        )

    response = ResponseSubset(
        file_path=output_path,
        output_directory=output_directory,
        filename=output_path.name,
        file_size=final_result_size_estimation,
        data_transfer_size=data_needed_approximation,
        variables=list(dataset.data_vars),
        coordinates_extent=get_dataset_coordinates_extent(dataset),
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

    logger.info("Writing to local storage. Please wait...")
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
    chunk_size_limit: Optional[int],
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
        vertical_axis=subset_request.vertical_axis,
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
    chunks: Optional[Literal["auto"]],
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
        chunks=chunks,  # type: ignore
    )
    return dataset.to_dataframe()


def get_optimum_dask_chunking(
    service: CopernicusMarineService,
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    variables: Optional[list[str]],
    chunk_size_limit: int,
) -> Optional[dict[str, Union[int, float]]]:
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
    """  # noqa
    set_variables = set(variables) if variables else set()
    coordinate_for_subset: dict[str, CopernicusMarineCoordinate] = {}
    for variable in service.variables:
        if (
            not variables
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
        coordinate_zarr_chunk_length[coordinate_id] = chunking_length
        requested_minimum, requested_maximum = _extract_requested_min_max(
            coordinate_id,
            geographical_parameters,
            temporal_parameters,
            depth_parameters,
        )
        number_of_zarr_chunks_needed = get_number_of_chunks_for_coordinate(
            requested_minimum,
            requested_maximum,
            coordinate,
            chunking_length,
        )
        if number_of_zarr_chunks_needed is None:
            continue
        max_dask_chunk_factor[coordinate_id] = number_of_zarr_chunks_needed
    if _product(max_dask_chunk_factor.values()) < 100:
        return None
    logger.debug(f"Zarr chunking: {coordinate_zarr_chunk_length}")
    logger.debug(f"Max dask chunk factor: {max_dask_chunk_factor}")
    optimum_dask_factors = _get_optimum_factors(
        max_dask_chunk_factor,
        chunk_size_limit,
    )
    logger.debug(f"Optimum dask factors: {optimum_dask_factors}")
    optimum_dask_chunking = {
        coordinate_id: coordinate_zarr_chunk_length[coordinate_id]
        * optimum_dask_factors[coordinate_id]
        for coordinate_id in optimum_dask_factors
    }
    logger.debug(f"Optimum dask chunking: {optimum_dask_chunking}")
    return optimum_dask_chunking


def _product(iterable) -> int:
    result = 1
    for i in iterable:
        result *= i
    return result


def _extract_requested_min_max(
    coordinate_id: str,
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
) -> tuple[Optional[float], Optional[float]]:
    # TODO: should work the same as the custom_sel we do
    if coordinate_id in COORDINATES_LABEL["time"]:
        min_time = temporal_parameters.start_datetime
        if min_time:
            min_time = min_time.timestamp() * 1e3
        max_time = temporal_parameters.end_datetime
        if max_time:
            max_time = max_time.timestamp() * 1e3
        return min_time, max_time
    if coordinate_id in COORDINATES_LABEL["latitude"]:
        return (
            geographical_parameters.latitude_parameters.minimum_latitude,
            geographical_parameters.latitude_parameters.maximum_latitude,
        )
    if coordinate_id in COORDINATES_LABEL["longitude"]:
        longitude_moduli = apply_longitude_modulus(
            geographical_parameters.longitude_parameters
        )
        if longitude_moduli:
            (
                minimum_longitude_modulus,
                maximum_longitude_modulus,
            ) = longitude_moduli
            if (
                maximum_longitude_modulus
                and minimum_longitude_modulus
                and maximum_longitude_modulus < minimum_longitude_modulus
            ):
                maximum_longitude_modulus += 360
            return (
                minimum_longitude_modulus,
                maximum_longitude_modulus,
            )
        else:
            return (None, None)
    if coordinate_id in COORDINATES_LABEL["depth"]:
        return depth_parameters.minimum_depth, depth_parameters.maximum_depth
    return None, None


def _get_optimum_factors(
    coordinate_max_dask_chunk_factor: dict[str, int],
    limit: int,
) -> dict[str, int]:
    optimum_factors = {
        coodinate_name: 1
        for coodinate_name in coordinate_max_dask_chunk_factor
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
                x in COORDINATES_LABEL["depth"],
                x in COORDINATES_LABEL["time"],
                x in COORDINATES_LABEL["latitude"],
                x in COORDINATES_LABEL["longitude"],
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
    store = zarr.DirectoryStore(output_path)
    return dataset.to_zarr(store=store, mode="w")


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
        encoding = {var: comp for var in dataset.data_vars}
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
