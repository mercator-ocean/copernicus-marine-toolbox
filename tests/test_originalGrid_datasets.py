import pathlib
from json import loads

import xarray

from copernicusmarine.core_functions.utils import datetime_parser
from tests.test_utils import execute_in_terminal

dataset_name = "cmems_mod_arc_bgc_my_ecosmo_P1D-m"
variable = "po4"
datasets_w_originalGrid = [
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
    def test_toolbox_identifies_originalGrid_datasets(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "-v",
            variable,
            "--dataset-part",
            "originalGrid",
            "--dry-run",
            "--log-level",
            "DEBUG",
            "--staging",
        ]
        self.output = execute_in_terminal(command)
        returned_value = loads(self.output.stdout)
        assert returned_value["status"] == "001"
        assert b"DEBUG" in self.output.stderr
        assert self.output.returncode == 0
        assert (
            b"Dataset part has the non lat lon projection."
            in self.output.stderr
        )

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
        assert b"ERROR" in self.output.stderr
        assert (
            b"You cannot specify longitude and latitude when using"
            b" the originalGrid "
            b"dataset part. Try using x and y instead."
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
        assert (
            b"Dataset part has the non lat lon projection."
            in self.output.stderr
        )
        assert datetime_parser("2020-01-01") == datetime_parser(
            dataset.time.values[0]
        )
        assert len(dataset.x.values) > 0
        assert len(dataset.y.values) > 0
        returned_value = loads(self.output.stdout)
        assert returned_value["coordinates_extent"][0]["coordinate_id"] == "y"
        assert returned_value["coordinates_extent"][1]["coordinate_id"] == "x"
        assert (
            returned_value["coordinates_extent"][2]["coordinate_id"] == "time"
        )
        assert (
            returned_value["coordinates_extent"][3]["coordinate_id"] == "depth"
        )
        assert len(returned_value["coordinates_extent"]) == 4

    def test_originalGrid_works_when_subsetting(self):
        for dataset_info in datasets_w_originalGrid:
            self.run_one_dataset(dataset_info)

    def run_one_dataset(self, dataset_info):
        dataset_name = dataset_info[0]
        dataset_year = dataset_info[1]
        max_x = dataset_info[3] if len(dataset_info) > 2 else "8"
        min_x = dataset_info[2] if len(dataset_info) > 3 else "6"
        max_y = dataset_info[5] if len(dataset_info) > 4 else "10"
        min_y = dataset_info[4] if len(dataset_info) > 5 else "5"
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
        assert returned_value["coordinates_extent"][0]["coordinate_id"] == "y"
        assert returned_value["coordinates_extent"][0]["maximum"] == float(
            max_y
        )
        assert returned_value["coordinates_extent"][0]["minimum"] == float(
            min_y
        )
        assert returned_value["coordinates_extent"][1]["coordinate_id"] == "x"
        assert returned_value["coordinates_extent"][1]["maximum"] == float(
            max_x
        )
        assert returned_value["coordinates_extent"][1]["minimum"] == float(
            min_x
        )
