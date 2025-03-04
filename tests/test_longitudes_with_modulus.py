from pathlib import Path

import numpy
import xarray

from tests.test_utils import execute_in_terminal


class TestLongitudesWithModulus:
    def _build_custom_command(
        self, folder, output_filename, min_longitude, max_longitude
    ):
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
        return [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--minimum-depth",
            "0.5",
            "--maximum-depth",
            "0.5",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "35.03",
            "--start-datetime",
            "2024-01-01",
            "--end-datetime",
            "2024-01-05",
            "-o",
            f"{folder}",
            "-f",
            f"{output_filename}",
        ]

    def test_subset_with_modulus_longitude(self, tmp_path):
        filename_dataset1 = "dataset1.nc"
        filename_dataset2 = "dataset2.nc"

        command1 = self._build_custom_command(
            tmp_path, filename_dataset1, -9.84, -4.82
        )
        command2 = self._build_custom_command(
            tmp_path, filename_dataset2, 350.16, 355.18
        )

        output1 = execute_in_terminal(command1)
        output2 = execute_in_terminal(command2)

        dataset1 = xarray.open_dataset(Path(tmp_path, filename_dataset1))
        dataset2 = xarray.open_dataset(Path(tmp_path, filename_dataset2))

        longitudes1 = dataset1.longitude.values
        longitudes2 = dataset2.longitude.values

        values1 = dataset1.sel(
            latitude=34.5,
            longitude=-8.25,
            time="2022-01-01T00:00:00.000000000",
            method="nearest",
        )["thetao"].values
        values2 = dataset2.sel(
            latitude=34.5,
            longitude=-8.25,
            time="2022-01-01T00:00:00.000000000",
            method="nearest",
        )["thetao"].values

        assert output1.returncode == 0
        assert output2.returncode == 0
        assert numpy.array_equal(longitudes1, longitudes2)
        assert numpy.array_equal(values1, values2)

    def test_subset_with_equal_longitudes(self, tmp_path):
        filename_dataset1 = "dataset1.nc"
        filename_dataset2 = "dataset2.nc"

        command1 = self._build_custom_command(
            tmp_path, filename_dataset1, -9.84, -9.84
        )
        command2 = self._build_custom_command(
            tmp_path, filename_dataset2, 350.16, 350.16
        )

        output1 = execute_in_terminal(command1)
        output2 = execute_in_terminal(command2)

        dataset1 = xarray.open_dataset(Path(tmp_path, filename_dataset1))
        dataset2 = xarray.open_dataset(Path(tmp_path, filename_dataset2))

        longitudes1 = dataset1.longitude.values
        longitudes2 = dataset2.longitude.values

        values1 = dataset1.sel(
            latitude=34.5,
            time="2022-01-01T00:00:00.000000000",
            method="nearest",
        )["thetao"].values
        values2 = dataset2.sel(
            latitude=34.5,
            time="2022-01-01T00:00:00.000000000",
            method="nearest",
        )["thetao"].values

        assert output1.returncode == 0
        assert output2.returncode == 0
        assert numpy.array_equal(longitudes1, longitudes2)
        assert numpy.array_equal(values1, values2)

    def test_subset_with_longitude_over_antemeridian(self, tmp_path):
        filename_dataset = "dataset.nc"

        command = self._build_custom_command(
            tmp_path, filename_dataset, -190, -170
        )

        self.output = execute_in_terminal(command)

        dataset = xarray.open_dataset(Path(tmp_path, filename_dataset))

        longitudes = dataset.longitude.values

        assert self.output.returncode == 0
        assert not numpy.isnan(dataset.thetao.max().values.item())
        assert longitudes.min() == 170
        assert longitudes.max() == 190

    def test_subset_with_longitude_over_antemeridian_and_below_0(
        self, tmp_path
    ):
        filename_dataset = "dataset.nc"

        command = self._build_custom_command(
            tmp_path, filename_dataset, -145, 180
        )

        self.output = execute_in_terminal(command)

        dataset = xarray.open_dataset(Path(tmp_path, filename_dataset))

        longitudes = dataset.longitude.values

        assert self.output.returncode == 0
        assert not numpy.isnan(dataset.thetao.max().values.item())
        assert longitudes.min() == -145
        assert longitudes.max() == 180

    def test_subset_with_longitude_range_over_360(self, tmp_path):
        filename_dataset = "dataset.nc"

        command = self._build_custom_command(
            tmp_path, filename_dataset, 240, 800
        )

        self.output = execute_in_terminal(command)

        dataset = xarray.open_dataset(Path(tmp_path, filename_dataset))

        longitudes = dataset.longitude.values

        assert self.output.returncode == 0
        assert longitudes.min() == numpy.float32(-180)
        assert longitudes.max() == numpy.float32(179.91669)

    def test_minimum_longitude_greater_than_maximum_longitude(self, tmp_path):
        filename_dataset = "dataset.nc"

        command = self._build_custom_command(
            tmp_path, filename_dataset, 60, 30
        )

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 1
        assert self.output.stderr.endswith(
            b"Minimum longitude greater than maximum longitude: "
            b"--minimum-longitude option must be smaller or equal to "
            b"--maximum-longitude\n"
        )
