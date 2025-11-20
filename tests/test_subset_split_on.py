import json
import os
from datetime import datetime

import xarray

from copernicusmarine import subset_split_on
from tests.test_utils import execute_in_terminal

error_message = (
    "The dataset cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m"
    ", version '202012', part 'default' is currently being updated."
    " Data after 2023-05-01T00:00:00Z may not be up to date."
)


class TestSubsetSplitOn:
    def test_split_on_year(self, tmp_path):
        res = subset_split_on(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            on_time="year",
            minimum_latitude=49,
            maximum_latitude=50,
            minimum_longitude=-1,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=1,
            output_directory=tmp_path,
            disable_progress_bar=True,
        )
        assert isinstance(res, list)
        assert len(res) == 2
        ds_2022_path = res[0].file_path
        assert os.path.exists(ds_2022_path)
        ds_2022 = xarray.open_dataset(ds_2022_path)
        assert (
            ds_2022.time.min().item()
            == datetime.fromisoformat("2022-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_2022.time.max().item()
            == datetime.fromisoformat("2022-12-01T00:00:00Z").timestamp() * 1e9
        )
        ds_2022.close()

        ds_2023_path = res[1].file_path
        assert os.path.exists(ds_2023_path)

        ds_2023 = xarray.open_dataset(ds_2023_path)
        assert (
            ds_2023.time.min().item()
            == datetime.fromisoformat("2023-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_2023.time.max().item()
            == datetime.fromisoformat("2023-05-01T00:00:00Z").timestamp() * 1e9
        )
        ds_2023.close()

    def test_split_on_year_with_one_year(self, tmp_path):
        res = subset_split_on(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-12-31",
            on_time="year",
            minimum_latitude=49,
            maximum_latitude=50,
            minimum_longitude=-1,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=1,
            output_directory=tmp_path,
            disable_progress_bar=True,
        )
        assert len(res) == 1
        ds_2022_path = res[0].file_path
        assert os.path.exists(ds_2022_path)
        ds_2022 = xarray.open_dataset(ds_2022_path)
        assert (
            ds_2022.time.min().item()
            == datetime.fromisoformat("2022-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_2022.time.max().item()
            == datetime.fromisoformat("2022-12-01T00:00:00Z").timestamp() * 1e9
        )
        ds_2022.close()

    def test_split_on_month(self, tmp_path):
        res = subset_split_on(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-04-01",
            minimum_latitude=49,
            maximum_latitude=50,
            minimum_longitude=-1,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=1,
            on_time="month",
            output_directory=tmp_path,
            dry_run=True,
        )
        assert isinstance(res, list)
        assert len(res) == 4
        filenames = [f.filename for f in res]
        for period in [
            "2022-01",
            "2022-02",
            "2022-03",
            "2022-04",
        ]:
            assert any(period in filename for filename in filenames)

    def test_split_on_multi_variables(self, tmp_path):
        res = subset_split_on(
            dataset_id="cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            start_datetime="2022-01-01",
            end_datetime="2022-02-10",
            minimum_longitude=-9.9,
            maximum_longitude=-9.5,
            minimum_latitude=33.9,
            maximum_latitude=35,
            minimum_depth=5,
            maximum_depth=6,
            variables=["thetao_cglo", "siconc_glor", "vo_cglo"],
            on_variables=True,
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 3
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_glo_phy-all_my_0.25deg_P1D-m_siconc_glor_9.90W-9.50W_33.90N-35.00N_5.00-6.00m_2022-01-01-2022-02-10.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_glo_phy-all_my_0.25deg_P1D-m_thetao_cglo_9.90W-9.50W_33.90N-35.00N_5.00-6.00m_2022-01-01-2022-02-10.nc",
            )
        )
        path_vo_cglo = os.path.join(
            tmp_path,
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m_vo_cglo_9.90W-9.50W_33.90N-35.00N_5.00-6.00m_2022-01-01-2022-02-10.nc",
        )
        assert os.path.exists(path_vo_cglo)
        ds_vo_cglo = xarray.open_dataset(path_vo_cglo)
        assert (
            ds_vo_cglo.time.min().item()
            == datetime.fromisoformat("2022-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_vo_cglo.time.max().item()
            == datetime.fromisoformat("2022-02-10T00:00:00Z").timestamp() * 1e9
        )
        assert len(ds_vo_cglo.data_vars) == 1
        assert "vo_cglo" in ds_vo_cglo.data_vars
        ds_vo_cglo.close()

    def test_split_on_day_monthly_dataset(self, tmp_path):
        res = subset_split_on(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-04-01",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            on_time="day",
            output_directory=tmp_path,
            dry_run=True,
        )
        assert isinstance(res, list)
        assert len(res) == 4
        filenames = [f.filename for f in res]
        assert (
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_10.00W-0.00E_45.00N-50.00N_0.00-100.00m_2022-01-01.nc"
            in filenames
        )

        assert (
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_10.00W-0.00E_45.00N-50.00N_0.00-100.00m_2022-02-01.nc"
            in filenames
        )

        assert (
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_10.00W-0.00E_45.00N-50.00N_0.00-100.00m_2022-03-01.nc"
            in filenames
        )

        assert (
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_10.00W-0.00E_45.00N-50.00N_0.00-100.00m_2022-04-01.nc"
            in filenames
        )

    def test_split_on_day(self, tmp_path):
        res = subset_split_on(
            dataset_id="cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
            start_datetime="2024-01-01",
            end_datetime="2024-01-05",
            minimum_latitude=49,
            maximum_latitude=50,
            minimum_longitude=-1,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=1,
            on_time="day",
            output_directory=tmp_path,
            dry_run=True,
        )
        assert isinstance(res, list)
        assert len(res) == 5
        filenames = [f.filename for f in res]
        print(filenames)
        assert (
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m_ist-mlotst-pbo-siage-sialb-siconc-sisnthick-sithick-sivelo-sob-tob-usi-vsi-zos_1.00W-0.00E_49.00N-50.00N_2024-01-01.nc"
            in filenames
        )

    def test_split_on_hour(self, tmp_path):
        res = subset_split_on(
            dataset_id="cmems_mod_glo_phy_anfc_0.083deg_PT1H-m",
            start_datetime="2024-01-01T00:00:00",
            end_datetime="2024-01-01T5:05:00",
            minimum_latitude=49,
            maximum_latitude=50,
            minimum_longitude=-1,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=1,
            on_time="hour",
            output_directory=tmp_path,
            dry_run=True,
        )
        assert len(res) == 6
        filenames = [f.filename for f in res]
        assert (
            "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m_so-thetao-uo-vo-zos_1.00W-0.00E_49.00N-50.00N_0.00-1.00m_2024-01-01T00:00:00.nc"
            in filenames
        )

    def test_split_on_cli(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-x",
            "-9.0",
            "-X",
            "-8.0",
            "-y",
            "34.0",
            "-Y",
            "35.0",
            "-z",
            "0.5",
            "-Z",
            "2",
            "--dry-run",
            "-t",
            "2020-01-01",
            "-T",
            "2023-05-01",
            "split-on",
            "--on-time",
            "year",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        responses = json.loads(self.output.stdout)
        assert len(responses) == 4

    def test_split_on_invalid_value_cli(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-x",
            "-9.0",
            "-X",
            "-8.0",
            "-y",
            "34.0",
            "-Y",
            "35.0",
            "-z",
            "0.5",
            "-Z",
            "2",
            "--dry-run",
            "-t",
            "2020-01-01",
            "-T",
            "2023-05-01",
            "split-on",
            "--on-time",
            "invalid_value",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode != 0
        assert "Error: Invalid value for '--on-time'" in self.output.stderr

    def test_split_on_zarr_format_cli(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-x",
            "-9.0",
            "-X",
            "-8.0",
            "-y",
            "34.0",
            "-Y",
            "35.0",
            "-z",
            "0.5",
            "-Z",
            "2",
            "--file-format",
            "zarr",
            "--dry-run",
            "-t",
            "2020-01-01",
            "-T",
            "2023-05-01",
            "split-on",
            "--on-time",
            "year",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        responses = json.loads(self.output.stdout)
        assert len(responses) == 4
        for response in responses:
            assert response["filename"].endswith(".zarr")

    def test_split_on_invalid_file_format_sqlite_cli(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
            "--dataset-part",
            "history",
            "--dry-run",
            "-t",
            "2020-09-01",
            "-T",
            "2020-09-15",
            "split-on",
            "--on-time",
            "year",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
