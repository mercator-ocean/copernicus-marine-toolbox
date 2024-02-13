import itertools
import logging
import pathlib
import re
from http.client import IncompleteRead
from typing import List, Optional, Tuple

import click
import pandas
import xarray
from dask.diagnostics import ProgressBar
from pydap.net import HTTPError
from xarray.backends import PydapDataStore

from copernicusmarine.catalogue_parser.request_structure import SubsetRequest
from copernicusmarine.core_functions.sessions import (
    get_configured_request_session,
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
    get_filename,
    get_formatted_dataset_size_estimation,
)

logger = logging.getLogger("copernicus_marine_root_logger")


def __parse_limit(message: str) -> Optional[float]:
    match = re.search(r", max=.+\";", message)
    if match:
        limit = match.group().strip(', max=";')
        return float(limit)
    else:
        return None


def split_by_chunks(dataset):
    chunk_slices = {}
    for dim, chunks in dataset.chunks.items():
        slices = []
        start = 0
        for chunk in chunks:
            if start >= dataset.sizes[dim]:
                break
            stop = start + chunk
            slices.append(slice(start, stop))
            start = stop
        chunk_slices[dim] = slices
    for slices in itertools.product(*chunk_slices.values()):
        selection = dict(zip(chunk_slices.keys(), slices))
        yield dataset[selection]


def find_chunk(ds: xarray.Dataset, limit: float) -> Optional[int]:
    N = ds["time"].shape[0]
    for i in range(N, 0, -1):
        ds = ds.chunk({"time": i})
        ts = list(split_by_chunks(ds))
        if (ts[0].nbytes / (1000 * 1000)) < limit:
            return i
    return None


def chunked_download(
    store: PydapDataStore,
    dataset: xarray.Dataset,
    limit: Optional[int],
    error: HTTPError,
    output_directory: pathlib.Path,
    output_filename: str,
    variables: Optional[List[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
):
    filepath = output_directory / output_filename
    if filepath.is_file():
        try:
            filepath.unlink()
        except OSError:
            logger.error("Error while deleting file: ", filepath)

    logger.info("Dataset must be chunked.")
    if limit is None:
        size_limit = __parse_limit(str(error.comment))
    else:
        size_limit = limit

    if size_limit:
        logger.info(f"Server download limit is {size_limit} MB")
        i_chunk = find_chunk(dataset, size_limit)
        dataset = xarray.open_dataset(
            store, mask_and_scale=True, chunks={"time": i_chunk}
        )

        dataset = subset(
            dataset=dataset,
            variables=variables,
            geographical_parameters=geographical_parameters,
            temporal_parameters=temporal_parameters,
            depth_parameters=depth_parameters,
        )

        dataset_slices = list(split_by_chunks(dataset))

        slice_paths = [
            pathlib.Path(output_directory, str(dataset_slice) + ".nc")
            for dataset_slice in range(len(dataset_slices))
        ]

        logger.info("Downloading " + str(len(dataset_slices)) + " files...")
        delayed = xarray.save_mfdataset(
            datasets=dataset_slices, paths=slice_paths, compute=False
        )
        with ProgressBar():
            delayed.compute()
        logger.info("Files downloaded")

        if output_filename is not None:
            logger.info(f"Concatenating files into {output_filename}...")
            dataset = xarray.open_mfdataset(slice_paths)
            delayed = dataset.to_netcdf(filepath, compute=False)
            with ProgressBar():
                delayed.compute()
            logger.info("Files concatenated")

            logger.info("Removing temporary files")
            for path in slice_paths:
                try:
                    path.unlink()
                except OSError:
                    logger.error("Error while deleting file: ", path)
            logger.info("Done")

    else:
        logger.info("No limit found in the returned server error")


def download_dataset(
    username: str,
    password: str,
    dataset_id: str,
    dataset_url: str,
    output_directory: pathlib.Path,
    output_filename: Optional[str],
    file_format: FileFormat,
    variables: Optional[List[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    limit: Optional[int],
    confirmation: Optional[bool],
    overwrite_output_data: bool,
    disable_progress_bar: bool,
    netcdf_compression_enabled: bool,
    netcdf_compression_level: Optional[int],
):
    dataset, store = open_dataset_from_opendap(
        username=username,
        password=password,
        dataset_url=dataset_url,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
    )

    dataset = add_copernicusmarine_version_in_dataset_attributes(dataset)
    filename = get_filename(output_filename, dataset, dataset_id, file_format)
    output_path = pathlib.Path(output_directory, filename)

    if confirmation:
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

    if not output_directory.is_dir():
        pathlib.Path.mkdir(output_directory, parents=True)

    try:
        logger.info("Trying to download as one file...")
        delayed = get_delayed_download(
            dataset,
            output_path,
            netcdf_compression_enabled,
            netcdf_compression_level,
        )
        download_delayed_dataset(delayed, disable_progress_bar)
        logger.info(f"Successfully downloaded to {output_path}")
    except HTTPError as error:
        chunked_download(
            store=store,
            dataset=dataset,
            limit=limit,
            error=error,
            output_directory=output_directory,
            output_filename=filename,
            variables=variables,
            geographical_parameters=geographical_parameters,
            temporal_parameters=temporal_parameters,
            depth_parameters=depth_parameters,
        )
    return output_path


def download_opendap(
    username: str,
    password: str,
    subset_request: SubsetRequest,
    dataset_id: str,
    disable_progress_bar: bool,
) -> pathlib.Path:
    if subset_request.dataset_url is None:
        e = ValueError("Dataset url is required at this stage")
        logger.error(e)
        raise e
    else:
        dataset_url = subset_request.dataset_url
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
    temporal_parameters = TemporalParameters(
        start_datetime=subset_request.start_datetime,
        end_datetime=subset_request.end_datetime,
    )
    depth_parameters = DepthParameters(
        minimum_depth=subset_request.minimum_depth,
        maximum_depth=subset_request.maximum_depth,
        vertical_dimension_as_originally_produced=subset_request.vertical_dimension_as_originally_produced,  # noqa
    )

    output_directory = (
        subset_request.output_directory
        if subset_request.output_directory
        else pathlib.Path(".")
    )
    limit = None
    output_path = download_dataset(
        username=username,
        password=password,
        dataset_id=dataset_id,
        dataset_url=dataset_url,
        output_directory=output_directory,
        output_filename=subset_request.output_filename,
        file_format=subset_request.file_format,
        variables=subset_request.variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
        limit=limit,
        confirmation=not subset_request.force_download,
        overwrite_output_data=subset_request.overwrite_output_data,
        disable_progress_bar=disable_progress_bar,
        netcdf_compression_enabled=subset_request.netcdf_compression_enabled,
        netcdf_compression_level=subset_request.netcdf_compression_level,
    )
    return output_path


def open_dataset_from_opendap(
    username: str,
    password: str,
    dataset_url: str,
    variables: Optional[list[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
) -> Tuple[xarray.Dataset, PydapDataStore]:
    session = get_configured_request_session()
    session.auth = (username, password)
    try:
        store = PydapDataStore.open(dataset_url, session=session, timeout=300)
    except IncompleteRead:
        raise ConnectionError(
            "Unable to retrieve data through opendap.\n"
            "This error usually comes from wrong credentials."
        )

    dataset = xarray.open_dataset(store)
    dataset = subset(
        dataset=dataset,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
    )
    return dataset, store


def read_dataframe_from_opendap(
    username: str,
    password: str,
    dataset_url: str,
    variables: Optional[list[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
) -> pandas.DataFrame:
    dataset, _ = open_dataset_from_opendap(
        username=username,
        password=password,
        dataset_url=dataset_url,
        variables=variables,
        geographical_parameters=geographical_parameters,
        temporal_parameters=temporal_parameters,
        depth_parameters=depth_parameters,
    )
    return dataset.to_dataframe()
