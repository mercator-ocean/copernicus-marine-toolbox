import inspect
import os
import subprocess
from datetime import datetime, timedelta
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
from tests.test_utils import execute_in_terminal


class TestPythonInterface:
    def test_describe_function(self):
        describe_result = describe()
        assert describe_result is not None
        assert isinstance(describe_result, dict)

    def test_describe_function_with_filter_twice_in_a_row(self):
        nwshelf_catalog = describe(contains=["NWSHELF"], include_datasets=True)
        assert len(nwshelf_catalog["products"]) == 7
        nwshelf_catalog = describe(contains=["NWSHELF"], include_datasets=True)
        assert len(nwshelf_catalog["products"]) == 7

    def test_get_function(self, tmp_path):
        get_result = get(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            output_directory=tmp_path,
            force_download=True,
        )
        assert get_result is not None
        assert all(map(lambda x: x.exists(), get_result))

    @mock.patch("os.utime", side_effect=PermissionError)
    def test_permission_denied_for_modification_date(
        self, mock_utime, tmp_path, caplog
    ):
        get(
            dataset_id="METOFFICE-GLO-SST-L4-REP-OBS-SST",
            force_download=True,
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
            force_download=True,
        )

        assert subset_result is not None
        assert subset_result.exists()

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

    def test_login_ok(self, tmp_path):
        non_existing_directory = Path(tmp_path, "i_dont_exist")
        is_valid = login(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            configuration_file_directory=non_existing_directory,
            overwrite_configuration_file=True,
        )

        assert is_valid is True
        assert (
            non_existing_directory / ".copernicusmarine-credentials"
        ).is_file()

        is_valid_with_skip = login(
            configuration_file_directory=non_existing_directory,
            skip_if_user_logged_in=True,
        )
        assert is_valid_with_skip is True

    def test_login_not_ok_with_wrong_credentials(self, tmp_path):
        non_existing_directory = Path(tmp_path, "i_dont_exist")
        is_valid = login(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password="FAKEPASSWORD",
            configuration_file_directory=non_existing_directory,
            overwrite_configuration_file=True,
        )

        assert is_valid is False
        assert non_existing_directory.is_dir() is False

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
            vertical_dimension_as_originally_produced=False,
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
        subset_method = "strict"

        dataset = open_dataset(
            dataset_id=dataset_id,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            subset_method=subset_method,
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
            subset_method="strict",
        )

        assert dataframe is not None

    def test_open_dataset_with_retention_date(self):
        dataset = open_dataset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_obs-oc_atl_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D",
        )
        assert int(dataset.time.min().values) >= 1720735200000

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
            force_download=True,
            output_directory=tmp_path,
            output_filename="netcdf_fillval.nc",
            overwrite_output_data=True,
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
            minimum_depth=0,
            maximum_depth=1,
            force_download=True,
            output_directory=tmp_path,
            output_filename="netcdf_fillval_compressed.nc",
            netcdf_compression_enabled=True,
            overwrite_output_data=True,
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

    def test_error_Coord_out_of_dataset_bounds(self):
        try:
            _ = subset(
                dataset_id="cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
                start_datetime=datetime.today() + timedelta(10),
                force_download=True,
                end_datetime=datetime.today()
                + timedelta(days=10, hours=23, minutes=59),
            )
        except core_functions.exceptions.CoordinatesOutOfDatasetBounds as e:
            assert "Some or all of your subset selection" in e.__str__()

    def test_bounding_box_method(self, tmp_path):
        output_filename = "output.nc"
        min_longitude = 0.01
        max_longitude = 2.97
        min_latitude = 0.01
        max_latitude = 3.1
        min_depth = 1.5
        max_depth = 50.0
        start_datetime = "2020-11-01T01:00:00"
        end_datetime = "2021-12-12T01:00:00"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--bounding-box-method",
            "outside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--force-download",
        ]
        output = subprocess.run(command)

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))
        assert output.returncode == 0
        assert dataset.longitude.values.min() <= min_longitude
        assert dataset.longitude.values.max() >= max_longitude
        assert dataset.latitude.values.min() <= min_latitude
        assert dataset.latitude.values.max() >= max_latitude
        assert dataset.depth.values.min() <= min_depth
        assert dataset.depth.values.max() >= max_depth
        assert datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
        assert datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")

    def test_bounding_box_method_outside_dataset_bounds(self, tmp_path):
        output_filename = "output.nc"
        min_longitude = 179.0
        max_longitude = 181.0
        min_latitude = 0.01
        max_latitude = 3.1
        start_datetime = "2024-01-01T01:00:00"
        end_datetime = "2024-05-12T01:00:00"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D",
            "--variable",
            "CHL",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--bounding-box-method",
            "outside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--force-download",
        ]
        output = execute_in_terminal(command)

        assert (
            b""""not all values found in index \'longitude\'"\nWARNING"""
            in output.stdout
        )
        assert (
            b"""not all values found in index \'time\'"\nWARNING"""
            in output.stdout
        )
        assert (
            b"""Bounding box method doesn\'t find a max outer value """
            b"""for time. Using the inside value instead.\nINFO"""
            in output.stdout
        )
        assert output.returncode == 0

    def test_bounding_box_method_depth(self, tmp_path):
        output_filename = "output.nc"
        min_longitude = 0.01
        max_longitude = 2.97
        min_latitude = 0.01
        max_latitude = 3.1
        min_depth = 0.4
        max_depth = 50.0
        start_datetime = "2020-11-01T01:00:00"
        end_datetime = "2021-12-12T01:00:00"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--bounding-box-method",
            "outside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--force-download",
        ]
        output = execute_in_terminal(command)

        assert (
            b"""Bounding box method doesn\'t find a min outer value """
            b"""for depth. Using the inside value instead.\nINFO"""
            in output.stdout
        )
