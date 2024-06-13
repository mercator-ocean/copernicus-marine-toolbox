import pathlib
import subprocess

import numpy
import xarray

SUBSET_NEAREST_LAYER_OPTIONS = {
    "dataset_id": "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
    "requested_depth": 11.7,
    "expected_depth": numpy.float32(11.405),
    "requested_datetime": "2023-04-26 17:05:02",
    "expected_datetime": numpy.datetime64("2023-04-27 00:00:00"),
    "requested_longitude": -9.12,
    "expected_longitude": numpy.float32(-9.083328),
    "requested_latitude": 33.37,
    "expected_latitude": numpy.float32(33.333336),
}


class TestCommandLineInterfaceNearestLayerSubset:
    def _nearest_layer_subset(
        self,
        output_folder: str,
        output_filename: str,
        force_service: str,
        same_depth: bool = False,
        same_datetime: bool = False,
        same_longitude: bool = False,
        same_latitude: bool = False,
    ):
        minimum_depth, maximum_depth = (
            (
                SUBSET_NEAREST_LAYER_OPTIONS["requested_depth"],
                SUBSET_NEAREST_LAYER_OPTIONS["requested_depth"],
            )
            if same_depth
            else (0.0, 50.0)
        )
        minimum_datetime, maximum_datetime = (
            (
                SUBSET_NEAREST_LAYER_OPTIONS["requested_datetime"],
                SUBSET_NEAREST_LAYER_OPTIONS["requested_datetime"],
            )
            if same_datetime
            else ("2023-04-26 00:00:00", "2023-04-28 23:59:59")
        )
        minimum_longitude, maximum_longitude = (
            (
                SUBSET_NEAREST_LAYER_OPTIONS["requested_longitude"],
                SUBSET_NEAREST_LAYER_OPTIONS["requested_longitude"],
            )
            if same_longitude
            else (-9.8, -4.8)
        )
        minimum_latitude, maximum_latitude = (
            (
                SUBSET_NEAREST_LAYER_OPTIONS["requested_latitude"],
                SUBSET_NEAREST_LAYER_OPTIONS["requested_latitude"],
            )
            if same_latitude
            else (33.9, 38.0)
        )

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{SUBSET_NEAREST_LAYER_OPTIONS['dataset_id']}",
            "--variable",
            "thetao",
            "--minimum-depth",
            f"{minimum_depth}",
            "--maximum-depth",
            f"{maximum_depth}",
            "--start-datetime",
            f"{minimum_datetime}",
            "--end-datetime",
            f"{maximum_datetime}",
            "--minimum-longitude",
            f"{minimum_longitude}",
            "--maximum-longitude",
            f"{maximum_longitude}",
            "--minimum-latitude",
            f"{minimum_latitude}",
            "--maximum-latitude",
            f"{maximum_latitude}",
            "-o",
            f"{output_folder}",
            "-f",
            f"{output_filename}",
            "--service",
            f"{force_service}",
            "--force-download",
        ]
        return command

    # -------------------#
    # Test on same depth #
    # -------------------#
    def test_subset_same_depth_surface_zarr(self, tmp_path):
        output_filename = "test_subset_same_depth_surface_zarr.zarr"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{SUBSET_NEAREST_LAYER_OPTIONS['dataset_id']}",
            "--variable",
            "thetao",
            "--minimum-depth",
            "0",
            "--maximum-depth",
            "0",
            "--start-datetime",
            "2023-04-26 00:00:00",
            "--end-datetime",
            "2023-04-28 23:59:59",
            "--minimum-longitude",
            "-9.8",
            "--maximum-longitude",
            "-4.8",
            "--minimum-latitude",
            "33.9",
            "--maximum-latitude",
            "38.0",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--service",
            "arco-geo-series",
            "--force-download",
        ]
        self.output = subprocess.run(command)

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        min_depth = dataset.depth.values.min()
        max_depth = dataset.depth.values.max()

        assert self.output.returncode == 0
        assert dataset.depth.size == 1
        assert min_depth == numpy.float32(0.494025)
        assert max_depth == numpy.float32(0.494025)

    def test_subset_same_depth_zarr(self, tmp_path):
        output_filename = "test_subset_same_depth_zarr.zarr"

        command = self._nearest_layer_subset(
            tmp_path, output_filename, "arco-geo-series", same_depth=True
        )

        self.output = subprocess.run(command)

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        min_depth = dataset.depth.values.min()
        max_depth = dataset.depth.values.max()

        assert self.output.returncode == 0
        assert dataset.depth.size == 1
        assert min_depth == SUBSET_NEAREST_LAYER_OPTIONS["expected_depth"]
        assert max_depth == SUBSET_NEAREST_LAYER_OPTIONS["expected_depth"]

    def test_subset_same_depth_with_vertical_dimension_as_originally_produced(
        self, tmp_path
    ):
        dataset_id = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
        output_filename = (
            "test_subset_same_depth_with_originial_vertical_dimension.nc"
        )

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--start-datetime",
            "2020-12-30 00:00:00",
            "--end-datetime",
            "2020-12-31 00:00:00",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "34.2",
            "--minimum-depth",
            "0",
            "--maximum-depth",
            "0",
            "--output-directory",
            f"{tmp_path}",
            "--output-filename",
            f"{output_filename}",
            "--force-download",
        ]

        self.output = subprocess.run(command, capture_output=True)

        assert self.output.returncode == 0

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        min_depth = dataset.depth.values.min()

        assert dataset.depth.size == 1
        assert min_depth >= 0

    # ----------------------#
    # Test on same datetime #
    # ----------------------#
    def test_subset_same_datetime_zarr(self, tmp_path):
        output_filename = "test_subset_same_datetime_zarr.zarr"

        command = self._nearest_layer_subset(
            tmp_path,
            output_filename,
            "arco-geo-series",
            same_datetime=True,
        )
        self.output = subprocess.run(command)

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        min_datetime = dataset.time.values.min()
        max_datetime = dataset.time.values.max()

        assert self.output.returncode == 0
        assert dataset.time.size == 1
        assert (
            min_datetime == SUBSET_NEAREST_LAYER_OPTIONS["expected_datetime"]
        )
        assert (
            max_datetime == SUBSET_NEAREST_LAYER_OPTIONS["expected_datetime"]
        )

    # -----------------------#
    # Test on same longitude #
    # -----------------------#
    def test_subset_same_longitude_zarr(self, tmp_path):
        output_filename = "test_subset_same_longitude_zarr.zarr"

        command = self._nearest_layer_subset(
            tmp_path,
            output_filename,
            "arco-geo-series",
            same_longitude=True,
        )
        self.output = subprocess.run(command)

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        min_elevation = dataset.longitude.values.min()
        max_elevation = dataset.longitude.values.max()

        assert self.output.returncode == 0
        assert dataset.longitude.size == 1
        assert (
            min_elevation == SUBSET_NEAREST_LAYER_OPTIONS["expected_longitude"]
        )
        assert (
            max_elevation == SUBSET_NEAREST_LAYER_OPTIONS["expected_longitude"]
        )

    # ----------------------#
    # Test on same latitude #
    # ----------------------#
    def test_subset_same_latitude_zarr(self, tmp_path):
        output_filename = "test_subset_same_latitude_zarr.zarr"

        command = self._nearest_layer_subset(
            tmp_path,
            output_filename,
            "arco-geo-series",
            same_latitude=True,
        )
        self.output = subprocess.run(command)

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        min_elevation = dataset.latitude.values.min()
        max_elevation = dataset.latitude.values.max()

        assert self.output.returncode == 0
        assert dataset.latitude.size == 1
        assert (
            min_elevation == SUBSET_NEAREST_LAYER_OPTIONS["expected_latitude"]
        )
        assert (
            max_elevation == SUBSET_NEAREST_LAYER_OPTIONS["expected_latitude"]
        )

    def test_subset_with_coordinates_range_falling_between_two_values(
        self, tmp_path
    ):
        output_filename = "data.nc"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-hcmr-wav-rean-h",
            "-x",
            "13.723",
            "-X",
            "13.724",
            "-y",
            "38.007",
            "-Y",
            "38.028",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-01T06:00:00",
            "-v",
            "VHM0",
            "-s",
            "timeseries",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--force-download",
        ]

        self.output = subprocess.run(command)
        dataset = xarray.open_dataset(f"{tmp_path}/{output_filename}")
        assert self.output.returncode == 0
        for dimension in dataset.sizes:
            assert dataset.sizes[dimension] > 0
