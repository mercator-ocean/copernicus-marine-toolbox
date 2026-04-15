import logging
import pathlib
import shutil
import warnings
from collections import defaultdict
from copy import deepcopy
from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr
from arcosparse import (
    Entity,
    UserConfiguration,
    get_entities,
    subset_and_return_dataframe,
)

from copernicusmarine.catalogue_parser.models import CopernicusMarineService
from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_DISABLE_SSL_CONTEXT,
    COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH,
)
from copernicusmarine.core_functions.exceptions import (
    NotEnoughPlatformMetadata,
    WrongPlatformID,
)
from copernicusmarine.core_functions.models import (  # TimeExtent,
    FileStatus,
    ResponseSubset,
    StatusCode,
    StatusMessage,
    VerticalAxis,
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.sessions import TRUST_ENV
from copernicusmarine.core_functions.temporary_path_saver import (
    TemporaryPathSaver,
)
from copernicusmarine.core_functions.utils import (
    construct_query_params_for_marine_data_store_monitoring,
    datetime_parser,
    datetime_to_isoformat,
    datetime_to_timestamp,
    get_unique_directorypath,
    get_unique_filepath,
    timestamp_parser,
)
from copernicusmarine.download_functions.utils import (
    build_filename_from_request,
    get_file_extension,
)
from copernicusmarine.versioner import __version__ as toolbox_version

logger = logging.getLogger("copernicusmarine")

COLUMNS_RENAME = {
    "entity_id": "platform_id",
    "entity_type": "platform_type",
}

COLUMNS_ORDER_DEPTH = [
    "variable",
    "platform_id",
    "platform_type",
    "time",
    "longitude",
    "latitude",
    "depth",
    "pressure",
    "is_depth_from_producer",
    "value",
    "value_qc",
    "institution",
    "doi",
    "product_doi",
]

COLUMNS_ORDER_ELEVATION = deepcopy(COLUMNS_ORDER_DEPTH)
COLUMNS_ORDER_ELEVATION[COLUMNS_ORDER_ELEVATION.index("depth")] = "elevation"

SORTING = {
    "variable": True,
    "platform_id": True,
    "platform_type": True,
    "time": True,
}


def download_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    axis_coordinate_id_mapping: dict[str, str],
    product_doi: str | None,
    product_id: str | None,
    disable_progress_bar: bool,
) -> ResponseSubset:

    if subset_request.dry_run:
        _, variables, platform_ids = _read_dataframe_sparse(
            username,
            subset_request,
            metadata_url,
            service,
            product_doi,
            disable_progress_bar,
            dry_run=True,
        )
        response = _get_response_subset(
            subset_request,
            variables=variables,
            platform_ids=platform_ids,
            axis_coordinate_id_mapping=axis_coordinate_id_mapping,
        )
        response.status = StatusCode.DRY_RUN
        response.message = StatusMessage.DRY_RUN
        return response

    df, variables, platform_ids = _read_dataframe_sparse(
        username,
        subset_request,
        metadata_url,
        service,
        product_doi,
        disable_progress_bar,
    )
    response = _get_response_subset(
        subset_request,
        variables=variables,
        platform_ids=platform_ids,
        axis_coordinate_id_mapping=axis_coordinate_id_mapping,
    )
    output_path = response.file_path
    if subset_request.skip_existing and output_path.exists():
        response.file_status = FileStatus.IGNORED
        return response
    elif (
        subset_request.overwrite
        and output_path.exists()
        and output_path.is_dir()
    ):
        shutil.rmtree(output_path)

    if subset_request.output_directory:
        subset_request.output_directory.mkdir(parents=True, exist_ok=True)
    if df.empty:
        return response

    if subset_request.file_format == "netcdf":
        user_configuration = _get_user_configuration(username)
        platforms_metadata = {
            entity.entity_id: entity
            for entity in get_entities(metadata_url, user_configuration)
        }
        file_names = _dataframe_to_netcdf_per_platform(
            df=df,
            vertical_axis=subset_request.vertical_axis,
            output_path=output_path,
            platforms_metadata=platforms_metadata,
            subset_request=subset_request,
            product_id=product_id,
            service=service,
            netcdf_compression_level=subset_request.netcdf_compression_level,
            netcdf3_compatible=subset_request.netcdf3_compatible,
        )
        response.file_names = file_names
    else:
        with TemporaryPathSaver(output_path) as tmp_path:
            if subset_request.file_format == "parquet":
                df.to_parquet(tmp_path, index=False)
            else:
                df.to_csv(tmp_path, index=False)

    return response


def read_dataframe_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    product_doi: str | None,
    disable_progress_bar: bool,
) -> pd.DataFrame:
    df, _, _ = _read_dataframe_sparse(
        username,
        subset_request,
        metadata_url,
        service,
        product_doi,
        disable_progress_bar,
    )
    return df


def _read_dataframe_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    product_doi: str | None,
    disable_progress_bar: bool,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Shared function for subset and read dataframe

    Returns also the variables and the platform_ids
    """
    user_configuration = _get_user_configuration(username)
    platforms_metadata = {
        entity.entity_id: entity
        for entity in get_entities(metadata_url, user_configuration)
    }
    if subset_request.platform_ids:
        platform_ids = _get_plaform_ids_to_subset(
            subset_request.platform_ids or [],
            set(platforms_metadata.keys()),
            service,
        )
    else:
        platform_ids = []
    variables = subset_request.variables or [
        variable.short_name for variable in service.variables
    ]
    if dry_run:
        return pd.DataFrame(), variables, platform_ids
    progress_bar_configuration = {
        "disable": disable_progress_bar,
        "bar_format": "{l_bar}{bar}| [{elapsed}<{remaining}]",
    }
    # see https://github.com/pandas-dev/pandas/issues/55928
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        df = subset_and_return_dataframe(
            minimum_latitude=subset_request.minimum_y,
            maximum_latitude=subset_request.maximum_y,
            minimum_longitude=subset_request.minimum_x,
            maximum_longitude=subset_request.maximum_x,
            minimum_elevation=(
                -subset_request.maximum_depth
                if subset_request.maximum_depth is not None
                else None
            ),
            maximum_elevation=(
                -subset_request.minimum_depth
                if subset_request.minimum_depth is not None
                else None
            ),
            minimum_time=(
                subset_request.start_datetime.timestamp()
                if subset_request.start_datetime is not None
                else None
            ),
            maximum_time=(
                subset_request.end_datetime.timestamp()
                if subset_request.end_datetime is not None
                else None
            ),
            variables=variables,
            entities=platform_ids,
            vertical_axis=subset_request.vertical_axis,
            url_metadata=metadata_url,
            user_configuration=user_configuration,
            progress_bar_configuration=progress_bar_configuration,
            columns_rename=COLUMNS_RENAME,
        )

        df = _transform_dataframe(
            df,
            subset_request.vertical_axis,
            platforms_metadata,
            product_doi,
        )
    if df.empty:
        logger.info(
            "No data found for the given parameters. "
            "Please check your request and try again."
        )
    return (
        df,
        variables,
        platform_ids,
    )


def _get_user_configuration(username: str) -> UserConfiguration:
    return UserConfiguration(
        disable_ssl=COPERNICUSMARINE_DISABLE_SSL_CONTEXT == "True",
        trust_env=TRUST_ENV,
        ssl_certificate_path=COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH,
        max_concurrent_requests=20,
        extra_params=construct_query_params_for_marine_data_store_monitoring(
            username
        ),
    )


def _get_plaform_ids_to_subset(
    platform_ids: list[str],
    platforms_metadata_names: set[str],
    retrieval_service: CopernicusMarineService,
) -> list[str]:
    platforms_to_subset = []
    if not platforms_metadata_names:
        raise NotEnoughPlatformMetadata()
    platforms_names_with_types: set[str] = set()
    platforms_without_types_mapping: dict[str, list] = defaultdict(list)
    for platform_name in platforms_metadata_names:
        platform_name_without_type = platform_name.split("___")[0]
        platforms_without_types_mapping[platform_name_without_type].append(
            platform_name
        )
        platforms_names_with_types.add(platform_name)
    for platform_id in platform_ids:
        if platform_id in platforms_names_with_types:
            platforms_to_subset.append(platform_id)
        if platform_id in platforms_without_types_mapping:
            platforms_to_subset.extend(
                platforms_without_types_mapping[platform_id]
            )
    if not platforms_to_subset:
        raise WrongPlatformID(
            platform_ids, retrieval_service.platforms_metadata
        )
    return platforms_to_subset


def _build_filename_and_output_path(
    subset_request: SubsetRequest,
    variables: list[str],
    platform_ids: list[str],
    axis_coordinate_id_mapping: dict[str, str],
) -> tuple[str, pathlib.Path]:
    extension_file = get_file_extension(subset_request.file_format)
    filename = subset_request.output_filename or build_filename_from_request(
        request=subset_request,
        variables=variables,
        platform_ids=platform_ids,
        axis_coordinate_id_mapping=axis_coordinate_id_mapping,
    )
    if subset_request.file_format == "netcdf":
        filename = pathlib.Path(filename).stem
    elif pathlib.Path(filename).suffix != extension_file:
        filename = f"{filename}{extension_file}"
    output_path = pathlib.Path(
        subset_request.output_directory,
        filename,
    )
    if not subset_request.overwrite and not subset_request.skip_existing:
        output_path = (
            get_unique_filepath(output_path)
            if not subset_request.file_format == "netcdf"
            else get_unique_directorypath(output_path)
        )
        filename = output_path.name
    return filename, output_path


def _get_response_subset(
    subset_request: SubsetRequest,
    variables: list[str],
    platform_ids: list[str],
    axis_coordinate_id_mapping: dict[str, str],
) -> ResponseSubset:
    filename, output_path = _build_filename_and_output_path(
        subset_request,
        variables,
        platform_ids,
        axis_coordinate_id_mapping,
    )
    return ResponseSubset(
        file_path=output_path,
        output_directory=subset_request.output_directory,
        filename=filename,
        file_size=None,
        data_transfer_size=None,
        variables=variables,
        # TODO: handle thoses extents maybe opening the dataframe
        coordinates_extent=[],
        status=StatusCode.SUCCESS,
        message=StatusMessage.SUCCESS,
        file_status=FileStatus.DOWNLOADED,
    )


def _transform_dataframe(
    df: pd.DataFrame,
    vertical_axis: VerticalAxis,
    platforms_metadata: dict[str, Entity],
    product_doi: str | None,
) -> pd.DataFrame:
    """
    Transform the dataframe to match the expected format to be consistent with MyOceanPro
    and Copernicus Marine Services.
    """  # noqa
    if df.empty:
        return df
    # Needs to be done before striping the type to the platform_id
    df["institution"] = df["platform_id"].apply(
        lambda x: (
            platforms_metadata[x].institution
            if x in platforms_metadata
            else None
        )
    )
    df["doi"] = df["platform_id"].apply(
        lambda x: (
            platforms_metadata[x].doi if x in platforms_metadata else None
        )
    )

    df["product_doi"] = (
        f"https://doi.org/{product_doi}"
        if product_doi and "https://" not in product_doi
        else product_doi
    )

    # From "platform___type" to "platform" since the type is in
    # column platform_type
    df["platform_id"] = df["platform_id"].str.split("___").str[0]

    df["time"] = df["time"].apply(
        lambda x: datetime_to_isoformat(timestamp_parser(x, unit="s"))
    )

    # Some depth values comes from the arcoification of the data
    # and are calculated from the pressure some others come
    # directly from the producer (ie native/original data)
    df["is_depth_from_producer"] = df["is_approx_elevation"].apply(
        lambda x: 0 if x else 1
    )
    df.drop(columns=["is_approx_elevation"], inplace=True)

    if vertical_axis == "elevation":
        df = df[COLUMNS_ORDER_ELEVATION]
    else:
        df = df[COLUMNS_ORDER_DEPTH]

    df.sort_values(
        by=list(SORTING.keys()),
        ascending=list(SORTING.values()),
        inplace=True,
    )

    df.reset_index(drop=True, inplace=True)
    return df


def _dataframe_to_netcdf_per_platform(
    df: pd.DataFrame,
    vertical_axis: VerticalAxis,
    output_path: pathlib.Path,
    platforms_metadata: dict[str, Entity],
    subset_request: SubsetRequest,
    product_id: str | None,
    service: CopernicusMarineService,
    netcdf_compression_level: int = 0,
    netcdf3_compatible: bool = False,
) -> list[str]:
    metadata_cols = [
        "platform_id",
        "platform_type",
        "institution",
        "doi",
        "product_doi",
    ]

    produced_paths: list[str] = []
    with TemporaryPathSaver(output_path, is_directory=True) as tmp_output_path:
        for platform_id, platform_df in df.groupby("platform_id"):
            platform_filename = f"{platform_id}.nc"
            output_path_with_plaform_id = tmp_output_path / platform_filename

            ds = _platform_dataframe_to_dataset(
                platform_df,
                metadata_cols=metadata_cols,
                vertical_axis=vertical_axis,
                arco_sparse_type=service.arco_sparse_type,
            )

            ds = _add_attributes_to_dataset(
                ds=ds,
                platform_df=platform_df,
                vertical_axis=vertical_axis,
                platform_id=str(platform_id),
                metadata_cols=metadata_cols,
                platforms_metadata=platforms_metadata,
                subset_request=subset_request,
                product_id=product_id,
                service=service,
            )

            encoding = _build_netcdf_encoding(ds, netcdf_compression_level)
            netcdf_format = "NETCDF3_CLASSIC" if netcdf3_compatible else None
            engine = "h5netcdf" if not netcdf3_compatible else "netcdf4"

            with TemporaryPathSaver(output_path_with_plaform_id) as tmp_file:
                ds.to_netcdf(
                    tmp_file,
                    encoding=encoding,
                    format=netcdf_format,
                    engine=engine,
                )

            produced_paths.append(platform_filename)
            logger.debug(
                f"Written NetCDF file for platform '{platform_id}': "
                f"{output_path}"
            )

        logger.info(
            f"Produced {len(produced_paths)} NetCDF file(s) "
            f"in {output_path}"
        )
    return produced_paths


def _platform_dataframe_to_dataset(
    platform_df: pd.DataFrame,
    metadata_cols: list[str],
    vertical_axis: VerticalAxis,
    arco_sparse_type: str | None,
) -> xr.Dataset:
    """
    ``depth_level`` is an integer index: for each time step, depth
    observations are ranked by depth. The maximum number of depth
    points across all time steps determines the size of the
    ``depth_level`` dimension.
    """
    index_cols = ["time", vertical_axis]
    variable_cols = ["variable", "value", "value_qc"]
    obs_df = platform_df.drop(columns=metadata_cols, errors="ignore")
    obs_df = obs_df.dropna(axis=1, how="all")
    non_nans_cols = obs_df.columns.to_list()
    aux_cols = [
        col for col in non_nans_cols if col not in index_cols + variable_cols
    ]
    vertical_col = [c for c in index_cols if c != "time"][0]
    if arco_sparse_type != "cmemsAltimetry":
        pivot_columns = ["time", vertical_col]
        values_columns = ["value", "value_qc"]
    else:  # Specific case for altimetry data
        pivot_columns = ["time"]
        values_columns = ["value"]

    obs_df = obs_df.sort_values(pivot_columns)
    pivot = obs_df.pivot_table(
        index=pivot_columns,
        columns="variable",
        values=values_columns,  # type: ignore[list-item]
        aggfunc="first",
    )
    pivot.columns = [
        f"{var}_QC" if metric == "value_qc" else var
        for metric, var in pivot.columns
    ]

    spatial_and_aux = obs_df.groupby(pivot_columns)[aux_cols].first()

    merged = pd.concat([pivot, spatial_and_aux], axis=1)
    merged = merged.reset_index()
    merged["time"] = merged["time"].apply(
        lambda x: datetime_to_timestamp(datetime_parser(x), unit="s")
    )
    if arco_sparse_type != "cmemsAltimetry":
        merged["depth_level"] = merged.groupby("time").cumcount()
        merged = merged.set_index(["time", "depth_level"])
    else:
        merged = merged.set_index(["time"])

    if vertical_col in non_nans_cols:
        merged.loc[
            merged["is_depth_from_producer"] == 0, vertical_col
        ] = np.nan
        merged = merged.drop(
            columns=["is_depth_from_producer"], errors="ignore"
        )
        if merged[vertical_col].isna().all():
            merged = merged.drop(columns=[vertical_col], errors="ignore")
            non_nans_cols = [
                coln for coln in non_nans_cols if coln != vertical_col
            ]

    ds = merged.to_xarray()
    if "pressure" in non_nans_cols:
        ds = ds.set_coords("pressure")
    if "latitude" in non_nans_cols:
        ds = ds.set_coords("latitude")
    if "longitude" in non_nans_cols:
        ds = ds.set_coords("longitude")
    if vertical_col in non_nans_cols:
        ds = ds.set_coords(vertical_col)

    ds = ds.set_coords("time")
    if (
        "depth_level" in ds.data_vars
        or "depth_level" in ds.coords
        or "depth_level" in ds.indexes
    ):
        ds = ds.drop_vars("depth_level")
    ds.time.attrs["units"] = "seconds since 1970-01-01T00:00:00Z"
    return ds


def _build_netcdf_encoding(
    ds: xr.Dataset,
    compression_level: int,
) -> dict:
    if compression_level <= 0:
        return {}

    encoding: dict = {}
    for var_name in ds.data_vars:
        encoding[var_name] = {
            "zlib": True,
            "complevel": compression_level,
            "contiguous": False,
            "shuffle": True,
            "scale_factor": 1.0,
            "add_offset": 0.0,
        }
    return encoding


def _add_attributes_to_dataset(
    ds: xr.Dataset,
    platform_df: pd.DataFrame,
    vertical_axis: VerticalAxis,
    platform_id: str,
    metadata_cols: list[str],
    platforms_metadata: dict[str, Entity],
    subset_request: SubsetRequest,
    product_id: str | None,
    service: CopernicusMarineService,
) -> xr.Dataset:
    def unpack(value_from_df):
        if isinstance(value_from_df, np.ndarray) and value_from_df.size == 1:
            return value_from_df.reshape(-1)[0].item()
        return value_from_df[0]

    # global attributes
    first_row = platform_df.iloc[0]
    metadata_info: dict[str, str] = {
        col: str(first_row[col])
        for col in metadata_cols
        if col in first_row.index
        and first_row[col] is not None
        and not pd.isna(first_row[col])  # type: ignore[arg-type]
    }
    if service.arco_sparse_type != "cmemsAltimetry":
        if metadata_info.get("platform_id"):
            ds.attrs["platform_code"] = metadata_info["platform_id"]
        if metadata_info.get("platform_type"):
            ds.attrs["platform_type"] = metadata_info["platform_type"]
    if metadata_info.get("institution"):
        ds.attrs["institution"] = metadata_info["institution"]
    if metadata_info.get("doi"):
        ds.attrs["doi"] = metadata_info["doi"]
    if metadata_info.get("product_doi"):
        if "doi" in ds.attrs:
            ds.attrs["doi"] += " " + metadata_info["product_doi"]
        else:
            ds.attrs["doi"] = metadata_info["product_doi"]
    if metadata_info.get("platform_type"):
        platform_id_type = platform_id + "___" + metadata_info["platform_type"]
        platform_metadata = platforms_metadata.get(
            platform_id_type
        ) or platforms_metadata.get(platform_id)
        if platform_metadata and platform_metadata.institution_edmo_code:
            ds.attrs[
                "institution_edmo_code"
            ] = platform_metadata.institution_edmo_code

    ds.attrs["time_coverage_start"] = datetime_to_isoformat(
        datetime_parser(str(platform_df["time"].min()))
    )
    ds.attrs["time_coverage_end"] = datetime_to_isoformat(
        datetime_parser(str(platform_df["time"].max()))
    )
    ds.attrs["last_date_observation"] = ds.attrs["time_coverage_end"]
    if "latitude" in platform_df.columns:
        ds.attrs["geospatial_lat_min"] = float(platform_df["latitude"].min())
        ds.attrs["geospatial_lat_max"] = float(platform_df["latitude"].max())
        ds.attrs["last_latitude_observation"] = unpack(
            ds.sel(
                time=datetime_to_timestamp(
                    datetime_parser(ds.attrs["last_date_observation"])
                ),
                method="nearest",
            )["latitude"].values
        )
    if "longitude" in platform_df.columns:
        ds.attrs["geospatial_lon_min"] = float(platform_df["longitude"].min())
        ds.attrs["geospatial_lon_max"] = float(platform_df["longitude"].max())
        ds.attrs["last_longitude_observation"] = unpack(
            ds.sel(
                time=datetime_to_timestamp(
                    datetime_parser(ds.attrs["last_date_observation"])
                ),
                method="nearest",
            )["longitude"].values
        )
    if vertical_axis in platform_df.columns:
        ds.attrs["geospatial_vertical_min"] = float(
            platform_df[vertical_axis].min()
        )
        ds.attrs["geospatial_vertical_max"] = float(
            platform_df[vertical_axis].max()
        )
    ds.attrs["references"] = "http://marine.copernicus.eu"
    ds.attrs[
        "license"
    ] = "https://marine.copernicus.eu/user-corner/service-commitments-and-licence"
    ds.attrs["download_date"] = datetime_to_isoformat(datetime.now())
    ds.attrs["copernicusmarine_toolbox_version"] = toolbox_version
    if product_id:
        ds.attrs["product_id"] = product_id
    ds.attrs["dataset_id"] = subset_request.dataset_id
    ds.attrs["Conventions"] = "CF-1.9 ACDD-1.3"
    ds.attrs["title"] = (
        f"Subset of dataset {subset_request.dataset_id} "
        f"with Copernicus Marine Toolbox"
    )
    ds.attrs["contact"] = "servicedesk.cmems@mercator-ocean.eu"
    ds.attrs[
        "history"
    ] = f"{datetime_to_isoformat(datetime.now())}: Subset from ARCO data created with Copernicus Marine Toolbox"  # noqa

    # time attributes
    if "time" in ds.coords:
        ds["time"].attrs["axis"] = "T"
        ds["time"].attrs["standard_name"] = "time"
        ds["time"].attrs["calendar"] = "standard"
        # unit is set before

    # depth attributes
    if vertical_axis in ds.coords:
        if vertical_axis == "elevation":
            ds["elevation"].attrs["positive"] = "up"
        else:
            ds["depth"].attrs["positive"] = "down"
        ds[vertical_axis].attrs["units"] = "m"
        ds[vertical_axis].attrs["valid_min"] = -12000.0
        ds[vertical_axis].attrs["valid_max"] = 12000.0
        ds[vertical_axis].attrs["standard_name"] = vertical_axis
        ds[vertical_axis].attrs["axis"] = "Z"

    # pressure attributes
    if "pressure" in ds.coords:
        ds["pressure"].attrs["units"] = "dbar"
        ds["pressure"].attrs["positive"] = "down"
        ds["pressure"].attrs["standard_name"] = "reference_pressure"
        if vertical_axis not in ds.coords:
            ds["pressure"].attrs["axis"] = "Z"

    # latitude and longitude attributes
    if "latitude" in ds.coords:
        ds["latitude"].attrs["units"] = "degrees_north"
        ds["latitude"].attrs["axis"] = "Y"
        ds["latitude"].attrs["valid_min"] = -90.0
        ds["latitude"].attrs["valid_max"] = 90.0
        ds["latitude"].attrs["standard_name"] = "latitude"
    if "longitude" in ds.coords:
        ds["longitude"].attrs["units"] = "degrees_east"
        ds["longitude"].attrs["axis"] = "X"
        ds["longitude"].attrs["valid_min"] = -180.0
        ds["longitude"].attrs["valid_max"] = 180.0
        ds["longitude"].attrs["standard_name"] = "longitude"

    # variables
    variables_info = {
        variable.short_name: variable for variable in service.variables
    }
    axis_order = ["T", "Z", "Y", "X", "other"]
    coordinates = [str(axis) for axis in ds.coords]
    coordinates = sorted(
        coordinates,
        key=lambda x: axis_order.index(ds[x].attrs.get("axis", "other")),
    )
    coordinates_str = " ".join(coordinates)
    for variable in ds.data_vars:
        variable = str(variable)
        ds[variable].encoding["coordinates"] = coordinates_str
        if "_QC" not in variable:
            variable_info = variables_info[variable]
            ds[variable].attrs["standard_name"] = variable_info.standard_name
            ds[variable].attrs["units"] = variable_info.units
            if service.arco_sparse_type != "cmemsAltimetry":
                ds[variable].attrs["ancillary_variable"] = variable + "_QC"
        else:
            variable_without_qc = variable.replace("_QC", "")
            variable_info = variables_info[variable_without_qc]
            ds[variable].attrs[
                "long_name"
            ] = f"Quality control flag for {variable_without_qc}"
            ds[variable].attrs["flags_values"] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            ds[variable].attrs["flags_meanings"] = (
                "no_qc_performed "
                "good_data "
                "probably_good_data "
                "bad_data_that_are_potentially_correctable "
                "bad_data "
                "value_changed "
                "value_below_detection "
                "nominal_value "
                "interpolated_value "
                "missing_value"
            )

    return ds
