import pathlib
from json import loads

import xarray

from copernicusmarine import open_dataset, read_dataframe, subset
from copernicusmarine.core_functions.utils import datetime_parser
from tests.test_utils import execute_in_terminal

dataset_name = "cmems_mod_arc_bgc_my_ecosmo_P1D-m"
variable = "po4"
datasets_w_originalGrid = [
    [
        "DMI-ARC-SEAICE_BERG_MOSAIC-L4-NRT-OBS",
        "2024",
        "220000",
        "2150000",
        "215000",
        "215000",
    ],
    [
        "cmems_obs-wind_arc_phy_my_l3-s1a-sar-asc-0.01deg_P1D-i",
        "2024",
        "215972.0",
        "234972.0",
        "-2799378.0",
        "-2150378.0",
    ],
    [
        "cmems_obs-wind_arc_phy_my_l3-s1a-sar-desc-0.01deg_P1D-i",
        "2024",
        "215972.0",
        "234972.0",
        "-2799378.0",
        "-2150378.0",
    ],
    ["cmems_mod_arc_bgc_anfc_ecosmo_P1D-m", "2020"],
    ["cmems_mod_arc_bgc_anfc_ecosmo_P1M-m", "2020"],
    ["cmems_mod_arc_phy_anfc_6km_detided_PT1H-i", "2022"],
    ["cmems_mod_arc_phy_anfc_6km_detided_PT6H-m", "2024"],
    ["cmems_mod_arc_phy_anfc_6km_detided_P1D-m", "2024"],
    ["cmems_mod_arc_phy_anfc_6km_detided_P1M-m", "2024"],
    # "cmems_mod_arc_phy_anfc_nextsim_P1M-m", "2020", # not yet available
    # "cmems_mod_arc_phy_anfc_nextsim_hm", # not yet available
    # "dataset-topaz6-arc-15min-3km-be", # not yet available
    ["cmems_mod_arc_bgc_my_ecosmo_P1D-m", "2020"],
    ["cmems_mod_arc_bgc_my_ecosmo_P1M", "2020"],
    ["cmems_mod_arc_bgc_my_ecosmo_P1Y", "2020"],
    ["cmems_mod_arc_phy_my_topaz4_P1D-m", "2020"],
    ["cmems_mod_arc_phy_my_topaz4_P1M", "2020"],
    ["cmems_mod_arc_phy_my_topaz4_P1Y", "2020"],
    [
        "cmems_mod_arc_phy_my_hflux_P1D-m",
        "2020",
        "0",
        "25000",
        "12500",
        "100000",
    ],
    [
        "cmems_mod_arc_phy_my_hflux_P1M-m",
        "2020",
        "0",
        "25000",
        "12500",
        "100000",
    ],
    [
        "cmems_mod_arc_phy_my_mflux_P1D-m",
        "2020",
        "0",
        "25000",
        "12500",
        "100000",
    ],
    [
        "cmems_mod_arc_phy_my_mflux_P1M-m",
        "2020",
        "0",
        "25000",
        "12500",
        "100000",
    ],
    [
        "cmems_mod_arc_phy_my_nextsim_P1M-m",
        "2020",
        "-99000",
        "-3000",
        "-100000",
        "-1000",
    ],
    [
        "DMI-ARC-SEAICE_BERG_MOSAIC_IW-L4-NRT-OBS",
        "2020",
        "220000",
        "2150000",
        "215000",
        "215000",
    ],
]


class TestOriginalGridDatasets:
    def test_originalGrid_error_when_geospatial(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "-v",
            variable,
            "--dataset-part",
            "originalGrid",
            "--minimum-longitude",
            "0",
            "--dry-run",
            "--log-level",
            "DEBUG",
            "--staging",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 1
        assert b"DEBUG" in self.output.stderr
        assert (
            b"Lon lat subset not available in original grid datasets: "
            b"You cannot specify longitude and latitude when using the"
            b" 'originalGrid' dataset part. Try using ``--minimum-x``"
            b", ``--maximum-x``, ``--minimum-y`` and ``--maximum-y``."
            in self.output.stderr
        )

    def test_originalGrid_error_with_open_dataset(self):
        try:
            _ = open_dataset(
                dataset_id="cmems_mod_arc_phy_my_topaz4_P1Y",
                dataset_part="originalGrid",
                maximum_longitude=8,
                minimum_x=6,
                maximum_y=10,
                minimum_y=5,
                start_datetime="2020",
                end_datetime="2020",
                coordinates_selection_method="outside",
            )
            assert 1 == 0
        except Exception as e:
            assert str(e) == (
                "You cannot specify longitude and latitude when using the "
                "'originalGrid' dataset part. Try using "
                "``--minimum-x``, ``--maximum-x``, ``--minimum-y`` "
                "and ``--maximum-y``."
            )

    def test_originalGrid_alias_work(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "-v",
            variable,
            "--dataset-part",
            "originalGrid",
            "-x",
            "0",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0
        assert b"DEBUG" in self.output.stderr
        assert (
            b"Because you are using an originalGrid dataset, we are considering"
            b" the options -x, -X, -y, -Y to be in m/km, not in degrees."
        ) in self.output.stderr

    def test_originalGrid_works_when_time_and_depth_subsetting(self, tmp_path):
        output_filename = "output.nc"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "-v",
            variable,
            "--dataset-part",
            "originalGrid",
            "-t",
            "2020-01-01",
            "-T",
            "2020-01-01",
            "-z",
            "3",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--log-level",
            "DEBUG",
            "--response-fields",
            "all",
            "--staging",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        assert self.output.returncode == 0
        assert b"DEBUG" in self.output.stderr
        assert datetime_parser("2020-01-01") == datetime_parser(
            dataset.time.values[0]
        )
        assert len(dataset.x.values) > 0
        assert len(dataset.y.values) > 0
        returned_value = loads(self.output.stdout)
        coordinates = sorted(
            returned_value["coordinates_extent"],
            key=lambda the_dict: the_dict["coordinate_id"],
            reverse=True,
        )
        assert coordinates[0]["coordinate_id"] == "y"
        assert coordinates[1]["coordinate_id"] == "x"
        assert coordinates[2]["coordinate_id"] == "time"
        assert coordinates[3]["coordinate_id"] == "depth"
        assert len(coordinates) == 4

    def test_originalGrid_works_when_subsetting(self):
        for dataset_info in datasets_w_originalGrid:
            self.run_one_dataset(dataset_info)

    def run_one_dataset(self, dataset_info):
        dataset_name = dataset_info[0]
        dataset_year = dataset_info[1]
        min_x = dataset_info[2] if len(dataset_info) > 2 else "6"
        max_x = dataset_info[3] if len(dataset_info) > 3 else "8"
        min_y = dataset_info[4] if len(dataset_info) > 4 else "5"
        max_y = dataset_info[5] if len(dataset_info) > 5 else "10"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "--dataset-part",
            "originalGrid",
            "--maximum-x",
            max_x,
            "--minimum-x",
            min_x,
            "--maximum-y",
            max_y,
            "--minimum-y",
            min_y,
            "-t",
            dataset_year,
            "-T",
            dataset_year,
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        coordinates = sorted(
            returned_value["coordinates_extent"],
            key=lambda the_dict: the_dict["coordinate_id"],
            reverse=True,
        )
        assert coordinates[0]["coordinate_id"] == "y"
        assert coordinates[0]["maximum"] == float(max_y)
        assert coordinates[0]["minimum"] == float(min_y)
        assert coordinates[1]["coordinate_id"] == "x"
        assert coordinates[1]["maximum"] == float(max_x)
        assert coordinates[1]["minimum"] == float(min_x)

    def test_out_of_bounds(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1D-m",
            "--dataset-part",
            "originalGrid",
            "--maximum-x",
            "100",
            "--minimum-x",
            "-100",
            "--maximum-y",
            "100",
            "--minimum-y",
            "-100",
            "-t",
            "2020",
            "-T",
            "2020",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert b"WARNING" in self.output.stderr
        assert (
            b"Some of your subset selection [-100.0, 100.0] for the"
            b" x dimension exceed the dataset coordinates [-36.0, 38.0]"
            in self.output.stderr
        )
        assert (
            b"Some of your subset selection [-100.0, 100.0] for the "
            b"y dimension exceed the dataset coordinates [-43.0, 28.0]"
            in self.output.stderr
        )

    def test_out_of_bounds_w_error(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1D-m",
            "--dataset-part",
            "originalGrid",
            "--maximum-x",
            "1",
            "--minimum-x",
            "-1",
            "--maximum-y",
            "100",
            "--minimum-y",
            "-100",
            "-t",
            "2020",
            "-T",
            "2020",
            "--dry-run",
            "--coordinates-selection-method",
            "strict-inside",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert b"ERROR" in self.output.stderr
        assert (
            b"Some of your subset selection [-100.0, 100.0] for the y"
            b" dimension exceed the dataset coordinates [-43.0, 28.0]"
            in self.output.stderr
        )

    def test_subset_w_python_interface(self, tmp_path):
        _ = subset(
            dataset_id="cmems_mod_arc_phy_my_topaz4_P1Y",
            dataset_part="originalGrid",
            maximum_x=8,
            minimum_x=6,
            maximum_y=10,
            minimum_y=5,
            start_datetime="2020",
            end_datetime="2020",
            output_filename="output.nc",
            output_directory=tmp_path,
            coordinates_selection_method="outside",
        )
        assert (tmp_path / "output.nc").exists()
        dataset = xarray.open_dataset(tmp_path / "output.nc")
        assert dataset is not None
        assert dataset.x.max() >= 8
        assert dataset.x.min() <= 6
        assert dataset.y.max() >= 10
        assert dataset.y.min() <= 5
        assert dataset.latitude is not None
        assert dataset.longitude is not None

    def test_open_dataset_w_python_interface(self):
        dataset = open_dataset(
            dataset_id="cmems_mod_arc_phy_my_topaz4_P1Y",
            dataset_part="originalGrid",
            maximum_x=8,
            minimum_x=6,
            maximum_y=10,
            minimum_y=5,
            start_datetime="2020",
            end_datetime="2020",
            coordinates_selection_method="outside",
        )
        assert dataset is not None
        assert dataset.x.max() >= 8
        assert dataset.x.min() <= 6
        assert dataset.y.max() >= 10
        assert dataset.y.min() <= 5
        assert dataset.latitude is not None
        assert dataset.longitude is not None

    def test_read_dataframe_w_python_interface(self):
        dataframe = read_dataframe(
            dataset_id="cmems_mod_arc_phy_my_topaz4_P1Y",
            dataset_part="originalGrid",
            maximum_x=8,
            minimum_x=6,
            maximum_y=10,
            minimum_y=5,
            start_datetime="2020",
            end_datetime="2020",
            coordinates_selection_method="nearest",
        )
        assert dataframe is not None
        assert dataframe["so"] is not None
        assert dataframe["latitude"] is not None
        assert dataframe["longitude"] is not None

    def test_y_axis_negative_step(self):
        dataset = open_dataset(
            "DMI-ARC-SEAICE_BERG_MOSAIC_IW-L4-NRT-OBS",
            dataset_part="originalGrid",
            maximum_y=235000,
            minimum_y=215000,
        )

        assert dataset is not None
        assert dataset.y.max() >= 235000
        assert dataset.y.min() >= 215000
