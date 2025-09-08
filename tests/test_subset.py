import itertools
import logging
import math
import os
import pathlib
from datetime import datetime
from json import loads
from pathlib import Path
from typing import Literal, Optional, Union

import pytest
import xarray

from copernicusmarine import WrongFormatRequested, open_dataset, subset
from tests.test_utils import (
    execute_in_terminal,
    get_file_size,
    main_checks_when_file_is_downloaded,
)


class TestSubset:
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

    def test_compressed_and_uncompressed_no_diff_with_ncdump(self, tmp_path):
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
        assert math.isclose(diff.thetao.mean().values, 0.0)
        output_uncompressed = execute_in_terminal(
            [
                "ncdump",
                "-h",
                str(tmp_path / "uncompressed_data.nc"),
            ]
        )
        output_compressed = execute_in_terminal(
            [
                "ncdump",
                "-h",
                str(tmp_path / "compressed_data.nc"),
            ]
        )
        # we skip the first line that contains the title
        assert output_compressed.stdout[23:] == output_uncompressed.stdout[25:]

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

    def test_file_format_option(self):
        response = subset(
            dataset_id="cmems_obs-sst_glo_phy_l3s_pir_P1D-m",
            start_datetime="2023-11-01T00:00:00",
            dry_run=True,
        )
        assert response.filename.endswith(".nc")

        response = subset(
            dataset_id="cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
            start_datetime="2023-11-25T00:00:00",
            file_format=None,
            dry_run=True,
        )
        assert response.filename.endswith(".csv")

        try:
            response = subset(
                dataset_id="cmems_obs-sst_glo_phy_l3s_pir_P1D-m",
                start_datetime="2023-11-01T00:00:00",
                file_format="parquet",
                dry_run=True,
            )
            assert False
        except WrongFormatRequested:
            pass

    def flatten_request_dict(
        self, request_dict: dict[str, Optional[Union[str, Path]]]
    ) -> list:
        flatten_list = list(
            itertools.chain.from_iterable(
                [[key, val] for key, val in request_dict.items()]
            )
        )
        flatten_list = list(filter(lambda x: x is not None, flatten_list))
        return flatten_list

    def test_subset_functionnalities(self, tmp_path):
        self.base_request_dict = {
            "--dataset-id": "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            "--variable": "so",
            "--start-datetime": "2022-01-05",
            "--end-datetime": "2022-01-06",
            "--minimum-latitude": "0.0",
            "--maximum-latitude": "0.1",
            "--minimum-longitude": "0.2",
            "--maximum-longitude": "0.3",
            "--output-directory": tmp_path,
        }
        self.check_subset_request_with_dataset_not_in_catalog()
        self.check_subset_request_with_no_subsetting(tmp_path)

    def check_subset_request_with_dataset_not_in_catalog(self):
        self.base_request_dict["--dataset-id"] = "FAKE_ID"

        unknown_dataset_request = [
            "copernicusmarine",
            "subset",
        ] + self.flatten_request_dict(self.base_request_dict)

        self.output = execute_in_terminal(unknown_dataset_request)
        assert (
            "Dataset not found: FAKE_ID Please check "
            "that the dataset exists and the input datasetID is correct"
        ) in self.output.stderr

    def check_subset_request_with_no_subsetting(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            f"{dataset_id}",
            "-o",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            "Missing subset option. Try 'copernicusmarine subset --help'."
            in self.output.stderr
        )
        assert (
            "To retrieve a complete dataset, please use instead: "
            f"copernicusmarine get --dataset-id {dataset_id}"
        ) in self.output.stderr

    def test_retention_period_works(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-oc_atl_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D",
            "--dataset-version",
            "202311",
            "--variable",
            "CHL",
            "--minimum-longitude",
            "-36.29005445972566",
            "--maximum-longitude",
            "-35.14832052107781",
            "--minimum-latitude",
            "47.122926204435295",
            "--maximum-latitude",
            "48.13780081656672",
            "--output-directory",
            tmp_path,
            "--output-filename",
            "dataset.nc",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        assert (
            "time       (time) datetime64[ns] 2023" not in self.output.stderr
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(tmp_path / "dataset.nc", response)

    def test_retention_period_works_when_only_values_in_metadata(
        self, tmp_path
    ):
        self.command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-oc_atl_bgc-pp_nrt_l4-multi-1km_P1M",
            "--variable",
            "PP",
            "--minimum-longitude",
            "-36.29005445972566",
            "--maximum-longitude",
            "-35.14832052107781",
            "--minimum-latitude",
            "47.122926204435295",
            "--maximum-latitude",
            "48.13780081656672",
            "--output-directory",
            tmp_path,
            "--output-filename",
            "dataset.nc",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        assert (
            "time       (time) datetime64[ns] 2023" not in self.output.stderr
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(tmp_path / "dataset.nc", response)

    def test_subset_wrong_input_response_fields_warning_and_error(self):
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
        response_fields = "status, wrong_field"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--dry-run",
            "-r",
            response_fields,
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            "Some ``--response-fields`` fields are invalid:"
            " wrong_field" in self.output.stderr
        )

        command[-1] = "wrong_field1, wrong_field2"
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            "Wrong fields error: All ``--response-fields`` "
            "fields are invalid: wrong_field1, wrong_field2"
            in self.output.stderr
        )

    def test_subset_with_dry_run_option(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--dry-run",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert str(tmp_path) in returned_value["file_path"]
        assert not os.path.exists(returned_value["file_path"])

    def test_subset_by_default_returns_status_message(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "-t",
            "2023",
            "-T",
            "2023",
            "-y",
            "55",
            "-Y",
            "56",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert returned_value["status"]
        assert returned_value["message"]
        assert "file_path" not in returned_value
        assert "coordinates_extent" not in returned_value

    def test_subset_can_choose_return_fields(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "-o",
            f"{tmp_path}",
            "--dry-run",
            "-r",
            "file_path",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert "status" not in returned_value
        assert "message" not in returned_value
        assert "file_path" in returned_value
        assert "coordinates_extent" not in returned_value

    def test_subset_error_when_forced_service_does_not_exist(self):
        self.when_i_run_copernicus_marine_subset_forcing_a_service_not_available()
        self.then_i_got_a_clear_output_with_available_service_for_subset()

    def when_i_run_copernicus_marine_subset_forcing_a_service_not_available(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1D-m",
            "--variable",
            "thetao",
            "--service",
            "unavailable-service",
        ]

        self.output = execute_in_terminal(command)

    def then_i_got_a_clear_output_with_available_service_for_subset(self):
        assert (
            "Service unavailable-service does not exist for command subset. "
            "Possible services: ['arco-geo-series', 'geoseries', "
            "'arco-time-series', 'timeseries', 'omi-arco', 'static-arco', "
            "'arco-platform-series', 'platformseries']"
        ) in self.output.stderr

    def when_i_request_subset_dataset_with_zarr_service(
        self,
        tmp_path,
        vertical_axis: Literal["depth", "elevation"] = "depth",
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "-t",
            "2023-05-10",
            "-T",
            "2023-05-12",
            "-x",
            "-18",
            "-X",
            "-10",
            "-y",
            "35",
            "-Y",
            "40",
            "-z",
            "1",
            "-Z",
            "10",
            "-v",
            "thetao",
            "--vertical-axis",
            f"{vertical_axis}",
            "--service",
            "arco-time-series",
            "-o",
            f"{tmp_path}",
            "-f",
            "data.zarr",
        ]

        self.output = execute_in_terminal(command)

    def then_i_have_correct_sign_for_depth_coordinates_values(
        self, output_path, sign
    ):
        filepath = pathlib.Path(output_path, "data.zarr")
        dataset = xarray.open_dataset(filepath, engine="zarr")

        assert self.output.returncode == 0
        if sign == "positive":
            assert dataset.depth.min() <= 10
            assert dataset.depth.max() >= 0
        elif sign == "negative":
            assert dataset.elevation.min() >= -10
            assert dataset.elevation.max() <= 0

    def then_i_have_correct_attribute_value(
        self, output_path, dimention_name, attribute_value
    ):
        filepath = pathlib.Path(output_path, "data.zarr")
        dataset = xarray.open_dataset(filepath, engine="zarr")
        assert dataset[dimention_name].attrs["standard_name"] == dimention_name
        assert dataset[dimention_name].attrs["positive"] == attribute_value

    def test_conversion_between_elevation_and_depth(self, tmp_path):
        self.when_i_request_subset_dataset_with_zarr_service(tmp_path, "depth")
        self.then_i_have_correct_sign_for_depth_coordinates_values(
            tmp_path, "positive"
        )
        self.then_i_have_correct_attribute_value(tmp_path, "depth", "down")

    def test_force_no_conversion_between_elevation_and_depth(self, tmp_path):
        self.when_i_request_subset_dataset_with_zarr_service(
            tmp_path, "elevation"
        )
        self.then_i_have_correct_sign_for_depth_coordinates_values(
            tmp_path, "negative"
        )
        self.then_i_have_correct_attribute_value(tmp_path, "elevation", "up")

    def test_default_service_for_subset_command(self):
        self.when_i_run_copernicus_marine_subset_with_default_service()
        self.then_i_can_see_the_arco_geo_series_service_is_choosen()

    def when_i_run_copernicus_marine_subset_with_default_service(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1M-m",
            "--variable",
            "thetao",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)

    def then_i_can_see_the_arco_geo_series_service_is_choosen(self):
        assert 'Selected service: "arco-geo-series"' in self.output.stderr

    def test_subset_with_dataset_sensitive_to_chunking(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
            "-t",
            "2024-01-01T00:00:00",
            "-T",
            "2024-01-05T23:59:59",
            "-v",
            "uo",
            "-x",
            "0",
            "-X",
            "180",
            "-y",
            "-80",
            "-Y",
            "90",
            "-z",
            "0.49",
            "-Z",
            "8",
            "-o",
            f"{tmp_path}",
            "-f",
            "output.nc",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(tmp_path / "output.nc", response)

    def test_error_log_for_variable_does_not_exist(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            "-v",
            "theta",
        ]

        self.output = execute_in_terminal(command)

        assert (
            "The variable 'theta' is neither a "
            "variable or a standard name in the dataset" in self.output.stderr
        )

    def test_error_log_for_service_does_not_exist(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            "-t",
            "2023-01-01",
            "-T",
            "2023-01-03",
            "--service",
            "ft",
        ]

        self.output = execute_in_terminal(command)

        assert (
            "Service ft does not exist for command subset"
            in self.output.stderr
        )

    def then_i_can_read_copernicusmarine_version_in_the_dataset_attributes(
        self, filepath
    ):
        dataset = xarray.open_dataset(filepath, engine="zarr")
        assert "copernicusmarine_version" in dataset.attrs

    def test_copernicusmarine_version_in_dataset_attributes_with_arco(
        self, tmp_path
    ):
        self.when_i_request_subset_dataset_with_zarr_service(tmp_path)
        self.then_i_can_read_copernicusmarine_version_in_the_dataset_attributes(
            tmp_path / "data.zarr"
        )

    def test_subset_filter_by_standard_name(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        output_filename = "data.zarr"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "sea_water_potential_temperature",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "34.2",
            "--minimum-depth",
            "0.5",
            "--maximum-depth",
            "1.6",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            "thetao"
            in xarray.open_zarr(f"{tmp_path}/{output_filename}").variables
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )

    # see https://github.com/pytest-dev/pytest-xdist/issues/385
    @pytest.mark.xdist_group(name="sequential")
    def test_arco_subset_is_fast_with_timeout(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Y",
            "43.1",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "-o",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command, timeout_second=15)
        assert self.output.returncode == 0, self.output.stderr

    def test_name_dataset_with_subset_parameters(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Z",
            "100",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "--file-format",
            "zarr",
            "-o",
            f"{tmp_path}",
        ]
        expected_dataset_id = "med-cmcc-cur-rean-h"
        expected_variables = "uo-vo"
        expected_longitude = "3.08E-3.17E"
        expected_latitude = "42.94N-45.98N"
        expected_datetime = "1993-01-01-1993-01-31"
        expected_extension = ".zarr"
        expected_filename = (
            expected_dataset_id
            + "_"
            + expected_variables
            + "_"
            + expected_longitude
            + "_"
            + expected_latitude
            + "_"
            + expected_datetime
            + expected_extension
        )
        expected_filepath = Path(tmp_path, expected_filename)
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert expected_filepath.is_dir()

    def then_i_can_read_dataset_size_in_the_response(self):
        response_subset = loads(self.output.stdout)
        assert "file_size" in response_subset
        assert "data_transfer_size" in response_subset
        assert response_subset["file_size"] > 0
        assert int(response_subset["data_transfer_size"]) == 50

    def test_dataset_size_is_displayed_when_downloading_with_arco_service(
        self, tmp_path
    ):
        self.when_i_request_subset_dataset_with_zarr_service(tmp_path)
        self.then_i_can_read_dataset_size_in_the_response()

    def test_dataset_has_always_every_dimensions(self, tmp_path):
        output_filename = "data.nc"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i",
            "-v",
            "uo",
            "-v",
            "vo",
            "-x",
            "-12",
            "-X",
            "-12",
            "-y",
            "30",
            "-Y",
            "30",
            "-t",
            "2023-11-20 00:00:00",
            "-T",
            "2023-11-20 00:00:00",
            "-z",
            "0.5",
            "-Z",
            "0.5",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            len(
                xarray.open_dataset(
                    Path(tmp_path) / output_filename
                ).sizes.keys()
            )
            == 4
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )

    def test_netcdf_compression_option(self, tmp_path):
        filename_without_option = "without_option.nc"
        filename_with_option = "with_option.nc"
        filename_zarr_without_option = "filename_without_option.zarr"
        filename_zarr_with_option = "filename_with_option.zarr"

        netcdf_compression_option = "--netcdf-compression-level"

        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Y",
            "43.1",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "-o",
            f"{tmp_path}",
        ]

        output_without_option = execute_in_terminal(
            base_command + ["-f", filename_without_option]
        )
        output_with_option = execute_in_terminal(
            base_command
            + ["-f", filename_with_option, netcdf_compression_option]
        )
        output_zarr_without_option = execute_in_terminal(
            base_command + ["-f", filename_zarr_without_option]
        )
        output_zarr_with_option = execute_in_terminal(
            base_command
            + ["-f", filename_zarr_with_option, netcdf_compression_option]
        )

        assert output_without_option.returncode == 0
        assert output_with_option.returncode == 0
        assert output_zarr_without_option.returncode == 0
        assert output_zarr_with_option.returncode != 0

        filepath_without_option = Path(tmp_path / filename_without_option)
        filepath_with_option = Path(tmp_path / filename_with_option)

        size_without_option = get_file_size(filepath_without_option)
        size_with_option = get_file_size(filepath_with_option)
        assert size_with_option < size_without_option

        dataset_without_option = xarray.open_dataset(filepath_without_option)
        dataset_with_option = xarray.open_dataset(filepath_with_option)
        assert os.path.exists(
            pathlib.Path(tmp_path, filename_zarr_without_option)
        )

        assert dataset_without_option.uo.encoding["zlib"] is False
        assert dataset_without_option.uo.encoding["complevel"] == 0

        assert dataset_with_option.uo.encoding["zlib"] is True
        assert dataset_with_option.uo.encoding["complevel"] == 1
        assert dataset_with_option.uo.encoding["contiguous"] is False
        assert dataset_with_option.uo.encoding["shuffle"] is True

    def test_netcdf_compression_with_optimised_files(self, tmp_path):
        filename_without_option = "without_option.nc"
        filename_with_option = "with_option.nc"

        netcdf_compression_option = "--netcdf-compression-level"

        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "-35",
            "-Y",
            "-30",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1995-01-31T00:00:00",
            "-v",
            "thetao",
            "-o",
            f"{tmp_path}",
        ]

        output_without_option = execute_in_terminal(
            base_command + ["-f", filename_without_option]
        )
        output_with_option = execute_in_terminal(
            base_command
            + ["-f", filename_with_option, netcdf_compression_option]
        )

        assert output_without_option.returncode == 0
        assert output_with_option.returncode == 0

        filepath_without_option = Path(tmp_path / filename_without_option)
        filepath_with_option = Path(tmp_path / filename_with_option)

        size_without_option = get_file_size(filepath_without_option)
        size_with_option = get_file_size(filepath_with_option)
        assert 1.6 * size_with_option < size_without_option

    def test_omi_arco_service(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "blksea_omi_circulation_rim_current_index",
            "-v",
            "BSRCI",
            "-o",
            f"{tmp_path}",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(base_command)
        assert self.output.returncode == 0
        assert 'Selected service: "omi-arco"' in self.output.stderr

        self.output = execute_in_terminal(base_command + ["-s", "omi-arco"])
        assert self.output.returncode == 0
        assert 'Selected service: "omi-arco"' in self.output.stderr

    def test_static_arco_service(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_blk_phy_anfc_2.5km_static",
            "-v",
            "deptho",
            "--dataset-part",
            "bathy",
            "-o",
            f"{tmp_path}",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(base_command)
        assert self.output.returncode == 0
        assert 'Selected service: "static-arco"' in self.output.stderr

        self.output = execute_in_terminal(base_command + ["-s", "static-arco"])
        assert self.output.returncode == 0
        assert 'Selected service: "static-arco"' in self.output.stderr

    def test_subset_dataset_part_option(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_blk_phy_anfc_2.5km_static",
            "-v",
            "deptho",
            "-o",
            f"{tmp_path}",
            "--dry-run",
        ]

        self.output = execute_in_terminal(
            base_command + ["--dataset-part", "bathy"]
        )
        assert self.output.returncode == 0

    def test_netcdf_compression_level(self, tmp_path):
        forced_comp_level = 4

        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Y",
            "43.1",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "-o",
            f"{tmp_path}",
            "-f",
            "data.nc",
            "--netcdf-compression-level",
            f"{forced_comp_level}",
        ]

        output_with_netcdf_compression_enabled = execute_in_terminal(
            base_command
        )

        assert output_with_netcdf_compression_enabled.returncode == 0

        filepath = Path(tmp_path / "data.nc")
        dataset = xarray.open_dataset(filepath)

        assert dataset.uo.encoding["zlib"] is True
        assert dataset.uo.encoding["complevel"] == forced_comp_level
        assert dataset.uo.encoding["contiguous"] is False
        assert dataset.uo.encoding["shuffle"] is True

    def test_subset_approximation_of_big_data_needs_to_be_downloaded(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-v",
            "thetao_oras",
            "-v",
            "uo_oras",
            "-v",
            "vo_oras",
            "-v",
            "so_oras",
            "-v",
            "zos_oras",
            "-x",
            "50",
            "-X",
            "110",
            "-y",
            "-10.0",
            "-Y",
            "30.0",
            "-t",
            "2010-03-01T00:00:00",
            "-T",
            "2010-06-30T00:00:00",
            "-z",
            "0.5057600140571594",
            "-Z",
            "500",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        response_subset = loads(self.output.stdout)
        assert int(response_subset["data_transfer_size"]) == 56876

    def test_requested_interval_fully_included_with_coords_sel_method_outside(
        self, tmp_path
    ):
        output_filename = "output.nc"
        min_longitude = 0.01
        max_longitude = 1.55
        min_latitude = 0.01
        max_latitude = 1.1
        min_depth = 30.5
        max_depth = 50.0
        start_datetime = "2023-12-01T01:00:00"
        end_datetime = "2023-12-12T01:00:00"
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
            "--coordinates-selection-method",
            "outside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))
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

    def test_requested_interval_is_correct_with_coords_sel_method_inside(
        self, tmp_path
    ):
        output_filename = "output.nc"
        min_longitude = 0.01
        max_longitude = 1.567
        min_latitude = 0.013
        max_latitude = 1.123
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-12-01T01:00:23"
        end_datetime = "2023-12-12T01:10:03"
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
            "--coordinates-selection-method",
            "inside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))
        assert dataset.longitude.values.min() >= min_longitude
        assert dataset.longitude.values.max() <= max_longitude
        assert dataset.latitude.values.min() >= min_latitude
        assert dataset.latitude.values.max() <= max_latitude
        assert dataset.depth.values.min() >= min_depth
        assert dataset.depth.values.max() <= max_depth
        assert datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
        assert datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")

    def test_requested_interval_is_correct_with_coords_sel_method_nearest(
        self, tmp_path
    ):
        output_filename = "output.nc"
        min_longitude = 0.08
        max_longitude = 1.567
        min_latitude = 0.013
        max_latitude = 1.123
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-01-01T00:00:00"
        end_datetime = "2023-01-03T23:04:00"
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
            "--coordinates-selection-method",
            "nearest",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))

        assert math.isclose(dataset.longitude.values.min(), 0.083343505859375)
        assert math.isclose(dataset.longitude.max().values, 1.583343505859375)
        assert math.isclose(dataset.latitude.values.min(), 0.0)
        assert math.isclose(dataset.latitude.values.max(), 1.0833358764648438)
        assert math.isclose(dataset.depth.values.min(), 29.444730758666992)
        assert math.isclose(dataset.depth.values.max(), 47.37369155883789)
        assert datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) == datetime.strptime("2023-01-01", "%Y-%m-%d")
        assert datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) == datetime.strptime("2023-01-04", "%Y-%m-%d")

    def test_coordinates_selection_method_outside_w_elevation(self, tmp_path):
        """dataset characteristics:
        * depth      (depth) float32 500B 1.018 3.166 5.465 ... 4.062e+03 4.153e+03
        * latitude   (latitude) float32 2kB 30.19 30.23 30.27 ... 45.9 45.94 45.98
        * longitude  (longitude) float32 4kB -5.542 -5.5 -5.458 ... 36.21 36.25 36.29
        * time       (time) datetime64[ns] 14kB 2020-01-01 2020-01-02 ... 2024-09-13
        """
        output_filename = "output.nc"
        min_longitude = -6
        max_longitude = -5
        min_latitude = 40
        max_latitude = 50
        min_depth = 1.1
        max_depth = 2.3
        start_datetime = "2023-01-01T00:00:00"
        end_datetime = "2023-01-03T23:04:00"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_med_bgc-bio_anfc_4.2km_P1D-m",
            "--variable",
            "nppv",
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
            "--coordinates-selection-method",
            "outside",
            "--vertical-axis",
            "elevation",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))

        assert dataset.longitude.values.min() <= -5.5416  # dataset limit
        assert dataset.longitude.max().values >= -5.0  # our limit
        assert dataset.latitude.values.min() <= 40  # our limit
        assert dataset.latitude.values.max() >= 45.9791  # dataset limit
        assert dataset.elevation.values.max() >= -1.01823665  # dataset limit
        assert dataset.elevation.values.min() <= -2.3  # our limit
        assert datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.strptime("2023-01-01", "%Y-%m-%d")
        assert datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.strptime("2023-01-03", "%Y-%m-%d")

    def test_subset_goes_to_staging(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--staging",
            "--log-level",
            "DEBUG",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert (
            "mdl-metadata-dta/dataset_product_id_mapping.json"
            in self.output.stderr
        )

    def test_subset_optimise_chunks(self, tmp_path):
        """
        This command can take several minutes or even end up in a memory issue
        because of the dask graph

        copernicusmarine subset -i cmems_mod_glo_phy_my_0.083deg_P1D-m -t "2013-08-01" -T "2013-08-01" -x 113.896034  -y -11.045679 -Y -6.366948 -z 0 -Z 5000
        """  # noqa
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
            "-t",
            "2013-08-01",
            "-T",
            "2013-08-01",
            "-x",
            "113.896034",
            "-y",
            "-11.045679",
            "-Y",
            "-6.366948",
            "-z",
            "0",
            "-Z",
            "5000",
            "--output-directory",
            f"{tmp_path}",
        ]
        timeout = 700  # TODO: back to 70 after issue is solved
        import platform

        if platform.system() == "Windows" or platform.system() == "Darwin":
            timeout = 1500  # TODO: back to 150 after issue is solved
        self.output = execute_in_terminal(command, timeout_second=timeout)
        assert self.output.returncode == 0

    def test_requested_interval_is_correct_w_weird_windowing(self, tmp_path):
        output_filename = "output.nc"
        min_longitude = -180.001
        max_longitude = -178.001
        min_latitude = 34.001
        max_latitude = 37.001
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-12-01T01:00:23"
        end_datetime = "2023-12-01T01:10:03"
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
            "--coordinates-selection-method",
            "inside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--dry-run",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        response = loads(output.stdout)
        assert response["coordinates_extent"][0]["minimum"] >= min_longitude
        assert (
            response["coordinates_extent"][0]["maximum"] <= max_longitude + 360
        )
        assert response["coordinates_extent"][1]["minimum"] >= min_latitude
        assert response["coordinates_extent"][1]["maximum"] <= max_latitude
        assert response["coordinates_extent"][3]["minimum"] >= min_depth
        assert response["coordinates_extent"][3]["maximum"] <= max_depth

    def test_nearest_works_correctly_when_moving_windows(self, tmp_path):
        output_filename = "output.nc"
        min_longitude = 179.92
        max_longitude = 181.999
        min_latitude = 34.001
        max_latitude = 37.001
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-12-01T01:00:23"
        end_datetime = "2023-12-01T01:10:03"
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
            "--coordinates-selection-method",
            "nearest",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--dry-run",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        response = loads(output.stdout)
        assert response["coordinates_extent"][0]["minimum"] <= min_longitude
        assert response["coordinates_extent"][0]["maximum"] >= max_longitude
        assert response["coordinates_extent"][1]["minimum"] <= min_latitude
        assert response["coordinates_extent"][1]["maximum"] <= max_latitude
        assert response["coordinates_extent"][3]["minimum"] <= min_depth
        assert response["coordinates_extent"][3]["maximum"] <= max_depth

    def test_requested_formats_subset_gridded_dataset(self):
        # zarr done in another test
        self.command_base = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_obs-sst_glo_phy_l3s_pir_P1D-m",
            "-t",
            "2023-01-01T00:00:00",
            "-T",
            "2023-01-01T23:59:59",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]
        output = execute_in_terminal(self.command_base)
        assert output.returncode == 0
        assert "in netcdf format" in output.stderr
        response = loads(output.stdout)
        assert response["filename"].endswith(".nc")

        wrong_command = self.command_base + [
            "--file-format",
            "csv",
        ]
        output = execute_in_terminal(wrong_command)
        assert output.returncode == 1
        assert "Wrong format requested" in output.stderr

        very_wrong_command = self.command_base + [
            "--file-format",
            "lkdjflkjsf",
        ]
        output = execute_in_terminal(very_wrong_command)
        # click error is a return code 2
        assert output.returncode == 2
        assert (
            "Invalid value for '--file-format': "
            "'lkdjflkjsf' is not one of 'netcdf', 'zarr', 'csv', 'parquet'."
            in output.stderr
        )

    def test_log_level_debug(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        output_filename = "data.zarr"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "sea_water_potential_temperature",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "34.2",
            "--minimum-depth",
            "0.5",
            "--maximum-depth",
            "1.6",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command, safe_quoting=True)
        assert self.output.returncode == 0
        assert "DEBUG - " in self.output.stderr
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )

    def test_netcdf3_option_with_ncdump(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_PT6H-i",
            "-v",
            "thetao",
            "-t",
            "2022-01-01T00:00:00",
            "-T",
            "2022-12-31T23:59:59",
            "-x",
            "-6.17",
            "-X",
            "-5.08",
            "-y",
            "35.75",
            "-Y",
            "36.30",
            "-z",
            "0.0",
            "-Z",
            "5.0",
            "-f",
            "dataset.nc",
            "-o",
            f"{tmp_path}",
            "--netcdf3-compatible",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        output_netcdf_format = execute_in_terminal(
            ["ncdump", "-k", f"{tmp_path / 'dataset.nc'}"]
        )
        assert output_netcdf_format.returncode == 0
        assert output_netcdf_format.stdout == "classic\n"

    def test_invert_min_max_raises_error_or_warning(self, caplog):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        with pytest.raises(
            ValueError,
            match="Minimum latitude greater than maximum latitude",
        ):
            subset(
                dataset_id=dataset_id,
                minimum_latitude=1.0,
                maximum_latitude=0.0,
                dry_run=True,
            )

        with pytest.raises(
            ValueError,
            match="Minimum depth greater than maximum depth",
        ):
            subset(
                dataset_id=dataset_id,
                minimum_latitude=1.0,
                maximum_latitude=2.0,
                minimum_depth=1.0,
                maximum_depth=0.0,
                dry_run=True,
            )

        with pytest.raises(
            ValueError,
            match="Start datetime greater than end datetime",
        ):
            subset(
                dataset_id=dataset_id,
                minimum_latitude=1.0,
                maximum_latitude=2.0,
                minimum_depth=1.0,
                maximum_depth=2.0,
                start_datetime="2023-01-02T00:00:00",
                end_datetime="2023-01-01T00:00:00",
                dry_run=True,
            )

        with caplog.at_level(logging.INFO):
            subset(
                dataset_id=dataset_id,
                minimum_longitude=1.0,
                maximum_longitude=0.0,
                dry_run=True,
            )
            assert "WARNING" in caplog.text
            assert (
                "Minimum longitude greater than maximum longitude"
                in caplog.text
            )
