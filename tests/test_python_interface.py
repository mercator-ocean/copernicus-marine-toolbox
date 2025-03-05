import inspect
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import xarray

from copernicusmarine import (
    describe,
    get,
    login,
    open_dataset,
    read_dataframe,
    subset,
)
from copernicusmarine.download_functions.utils import (
    timestamp_or_datestring_to_datetime,
)


class TestPythonInterface:
    def test_get_function(self, tmp_path):
        get_result = get(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            output_directory=tmp_path,
        )
        assert get_result is not None
        assert all(
            map(
                lambda x: x.exists(),
                [result.file_path for result in get_result.files],
            )
        )

    @mock.patch("os.utime", side_effect=PermissionError)
    def test_permission_denied_for_modification_date(
        self, mock_utime, tmp_path, caplog
    ):
        get(
            dataset_id="METOFFICE-GLO-SST-L4-REP-OBS-SST",
            filter="*2022053112000*",
            output_directory=f"{tmp_path}",
            no_directories=True,
        )
        assert "Permission to modify the last modified date" in caplog.text
        assert "is denied" in caplog.text
        output_file = Path(
            tmp_path,
            "20220531120000-UKMO-L4_GHRSST-SSTfnd-OSTIA-GLOB_REP-v02.0-fv02.0.nc",
        )
        five_minutes_ago = datetime.now() - timedelta(minutes=5)
        assert datetime.fromtimestamp(os.path.getmtime(output_file)) > (
            five_minutes_ago
        )

    def test_subset_function(self, tmp_path):
        self.when_subset_function(tmp_path)
        self.then_the_same_with_skip_existing_does_not_download(tmp_path)

    def when_subset_function(self, tmp_path):
        subset_result = subset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            variables=["so"],
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
            output_directory=tmp_path,
        )

        assert subset_result is not None
        assert subset_result.file_path.exists()

    def then_the_same_with_skip_existing_does_not_download(self, tmp_path):
        subset_result = subset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            variables=["so"],
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
            output_directory=tmp_path,
            skip_existing=True,
        )
        assert subset_result.file_path.exists()
        assert "IGNORED" == subset_result.file_status
        assert "000" == subset_result.status

    def test_open_dataset(self):
        dataset = open_dataset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
        )
        assert dataset is not None

    def test_read_dataframe(self):
        dataframe = read_dataframe(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
        )
        assert dataframe is not None

    def test_signature_inspection_is_working(self):
        assert inspect.signature(describe).parameters["contains"]

        common_key_parameter = "username"
        assert inspect.signature(login).parameters[common_key_parameter]
        assert inspect.signature(get).parameters[common_key_parameter]
        assert inspect.signature(get).parameters[common_key_parameter]
        assert inspect.signature(subset).parameters[common_key_parameter]
        assert inspect.signature(open_dataset).parameters[common_key_parameter]
        assert inspect.signature(read_dataframe).parameters[
            common_key_parameter
        ]

    def test_ISO8601_datetime_format_as_string(
        self,
    ):
        dataset = open_dataset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime="2023-09-15T00:00:00.000Z",
            end_datetime="2023-09-20T00:00:00.000Z",
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
            vertical_axis="elevation",
        )
        assert dataset is not None
        assert (
            dataset.so.sel(
                latitude=0,
                longitude=0.2,
                elevation=0,
                time=datetime.strptime(
                    "2023-09-15T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                method="nearest",
            ).size
            == 1
        )

    def test_open_dataset_with_strict_method(self, caplog):
        dataset_id = "cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i"
        start_datetime = "2023-09-15T00:00:00.000Z"
        end_datetime = "2023-09-15T00:00:00.000Z"
        coordinates_selection_method = "strict-inside"

        dataset = open_dataset(
            dataset_id=dataset_id,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            coordinates_selection_method=coordinates_selection_method,
        )

        assert dataset.coords is not None
        assert "ERROR" not in caplog.text

    def test_read_dataframe_with_strict_method(self, caplog):
        dataframe = read_dataframe(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
            coordinates_selection_method="strict-inside",
        )

        assert dataframe is not None
        assert dataframe.size > 0
        assert "ERROR" not in caplog.text

    def test_open_dataset_with_retention_date(self):
        dataset = open_dataset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_obs-oc_atl_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D",
        )
        assert timestamp_or_datestring_to_datetime(
            dataset.time.values.min()
        ) >= datetime(2024, 8, 31, 0, 0, 0, tzinfo=timezone.utc)

    def test_open_dataset_with_retention_date_and_only_values_in_metadata(
        self,
    ):
        dataset = open_dataset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_obs-oc_atl_bgc-pp_nrt_l4-multi-1km_P1M",
        )
        assert timestamp_or_datestring_to_datetime(
            dataset.time.values.min()
        ) >= datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    def test_subset_modify_attr_for_depth(self):
        dataset = open_dataset(
            dataset_id="cmems_mod_arc_phy_anfc_6km_detided_P1D-m"
        )
        assert dataset.depth.attrs["positive"] == "down"
        assert dataset.depth.attrs["standard_name"] == "depth"
        assert dataset.depth.attrs["long_name"] == "Depth"

    def test_subset_keeps_fillvalue_empty(self, tmp_path):
        subset(
            dataset_id="cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            variables=["thetao"],
            minimum_longitude=-28.10,
            maximum_longitude=-27.94,
            minimum_latitude=40.20,
            maximum_latitude=40.44,
            start_datetime="2024-02-23T00:00:00",
            end_datetime="2024-02-23T23:59:59",
            minimum_depth=0,
            maximum_depth=1,
            output_directory=tmp_path,
            output_filename="netcdf_fillval.nc",
            overwrite=True,
        )

        subsetdata = xarray.open_dataset(
            f"{tmp_path}/netcdf_fillval.nc", decode_cf=False
        )
        assert "_FillValue" not in subsetdata.longitude.attrs
        assert "_FillValue" not in subsetdata.time.attrs
        assert "_FillValue" not in subsetdata.latitude.attrs
        assert "_FillValue" not in subsetdata.depth.attrs
        assert subsetdata.time.attrs["calendar"] == "gregorian"
        assert subsetdata.time.attrs["units"] == "hours since 1950-01-01"

    def test_subset_keeps_fillvalue_empty_w_compression(self, tmp_path):
        subset(
            dataset_id="cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            variables=["thetao"],
            minimum_longitude=-28.10,
            maximum_longitude=-27.94,
            minimum_latitude=40.20,
            maximum_latitude=40.44,
            start_datetime="2024-02-23T00:00:00",
            end_datetime="2024-02-23T23:59:59",
            minimum_depth=5,
            maximum_depth=10,
            output_directory=tmp_path,
            output_filename="netcdf_fillval_compressed.nc",
            netcdf_compression_level=1,
            overwrite=True,
        )

        subsetdata = xarray.open_dataset(
            f"{tmp_path}/netcdf_fillval_compressed.nc", decode_cf=False
        )
        assert "_FillValue" not in subsetdata.longitude.attrs
        assert "_FillValue" not in subsetdata.time.attrs
        assert "_FillValue" not in subsetdata.latitude.attrs
        assert "_FillValue" not in subsetdata.depth.attrs
        assert subsetdata.time.attrs["calendar"] == "gregorian"
        assert subsetdata.time.attrs["units"] == "hours since 1950-01-01"

    def test_compressed_and_uncompressed_no_diff(self, tmp_path):
        data_query = {
            "dataset_id": "cmems_mod_glo_phy_my_0.083deg_P1D-m",
            "start_datetime": "2019-01-31",
            "end_datetime": "2019-01-31",
            "minimum_depth": 0,
            "maximum_depth": 1,
            "variables": ["sea_water_potential_temperature"],
            "output_directory": tmp_path,
        }
        subset(**data_query, output_filename="uncompressed_data.nc")
        subset(
            **data_query,
            netcdf_compression_level=1,
            output_filename="compressed_data.nc",
        )
        dataset_uncompressed = xarray.open_dataset(
            tmp_path / "uncompressed_data.nc"
        )
        dataset_compressed = xarray.open_dataset(
            tmp_path / "compressed_data.nc"
        )
        size_uncompressed = (tmp_path / "uncompressed_data.nc").stat().st_size
        size_compressed = (tmp_path / "compressed_data.nc").stat().st_size
        assert len(dataset_uncompressed.longitude.values) > 4300
        assert len(dataset_compressed.longitude.values) > 4300
        assert len(dataset_uncompressed.latitude.values) > 2000
        assert len(dataset_compressed.latitude.values) > 2000

        assert size_uncompressed > 2 * size_compressed

        diff = dataset_uncompressed - dataset_compressed
        diff.attrs = dataset_uncompressed.attrs
        for var in diff.data_vars:
            diff[var].attrs = dataset_uncompressed[var].attrs

        diff.to_netcdf(tmp_path / "diff.nc")
        diff = xarray.open_dataset(tmp_path / "diff.nc")
        assert diff.thetao.mean().values == 0.0

    def test_lonlat_attributes_when_not_in_arco(self, tmp_path):
        dataset_response = subset(
            dataset_id="esa_obs-si_arc_phy-sit_nrt_l4-multi_P1D-m",
            variables=["density_of_ocean", "quality_flag"],
            minimum_longitude=-28.10,
            maximum_longitude=-27.94,
            minimum_latitude=40.20,
            maximum_latitude=40.44,
            start_datetime="2024-11-21T00:00:00",
            end_datetime="2024-11-21T00:00:00",
            minimum_depth=5,
            maximum_depth=10,
            output_directory=tmp_path,
            output_filename="without_lonlat_attrs_dataset.nc",
        )
        dataset = xarray.open_dataset(
            tmp_path / "without_lonlat_attrs_dataset.nc"
        )

        assert dataset_response.status == "000"
        assert dataset.longitude.attrs == {
            "axis": "X",
            "long_name": "Longitude",
            "standard_name": "longitude",
            "units": "degrees_east",
        }
        assert dataset.latitude.attrs == {
            "axis": "Y",
            "long_name": "Latitude",
            "standard_name": "latitude",
            "units": "degrees_north",
        }
        for coordinate in dataset_response.coordinates_extent:
            assert coordinate.coordinate_id in dataset.sizes
            if coordinate.coordinate_id in [
                "longitude",
                "latitude",
            ]:  # not time
                assert (
                    min(dataset[coordinate.coordinate_id].values)
                    == coordinate.minimum
                )
                assert (
                    dataset[coordinate.coordinate_id].values.max()
                    == coordinate.maximum
                )
