import os
from datetime import datetime as dt

import xarray

from copernicusmarine import subset
from copernicusmarine.core_functions.models import ResponseSubset

error_message = (
    "The dataset cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m"
    ", version '202012', part 'default' is currently being updated."
    " Data after 2023-05-01T00:00:00Z may not be up to date."
)


class TestSubsetSplitOn:
    def test_no_split(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2023-01-01",
            end_datetime="2023-05-10",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            output_directory=tmp_path,
        )
        assert isinstance(res, ResponseSubset)
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2023-01-01-2023-05-01.nc",
            )
        )

    def test_split_on_year(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            split_on="year",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            output_directory=tmp_path,
            disable_progress_bar=True,
        )
        assert isinstance(res, list)
        assert len(res) == 2
        ds_2022_path = os.path.join(
            tmp_path,
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01-2022-12-01.nc",
        )
        assert os.path.exists(ds_2022_path)
        ds_2022 = xarray.open_dataset(ds_2022_path)
        assert (
            ds_2022.time.min().item()
            == dt.fromisoformat("2022-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_2022.time.max().item()
            == dt.fromisoformat("2022-12-01T00:00:00Z").timestamp() * 1e9
        )
        ds_2022.close()

        ds_2023_path = os.path.join(
            tmp_path,
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2023-01-01-2023-05-01.nc",
        )
        assert os.path.exists(ds_2023_path)

        ds_2023 = xarray.open_dataset(ds_2023_path)
        assert (
            ds_2023.time.min().item()
            == dt.fromisoformat("2023-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_2023.time.max().item()
            == dt.fromisoformat("2023-05-01T00:00:00Z").timestamp() * 1e9
        )
        ds_2023.close()

    def test_split_on_year_with_one_year(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-12-31",
            split_on="year",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            output_directory=tmp_path,
            disable_progress_bar=True,
        )
        assert isinstance(res, ResponseSubset)
        ds_2022_path = os.path.join(
            tmp_path,
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01-2022-12-01.nc",
        )
        assert os.path.exists(ds_2022_path)
        ds_2022 = xarray.open_dataset(ds_2022_path)
        assert (
            ds_2022.time.min().item()
            == dt.fromisoformat("2022-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_2022.time.max().item()
            == dt.fromisoformat("2022-12-01T00:00:00Z").timestamp() * 1e9
        )
        ds_2022.close()

    def test_split_on_month(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-04-01",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            split_on="month",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 4
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-02-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-03-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-04-01.nc",
            )
        )

    def test_split_on_variable_without_progress_bar(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-12-31",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            split_on="variable",
            output_directory=tmp_path,
            disable_progress_bar=True,
        )
        assert isinstance(res, ResponseSubset)
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01-2022-12-01.nc",
            )
        )

    def test_split_on_variable_with_progress_bar(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-12-31",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            split_on="variable",
            output_directory=tmp_path,
        )
        assert isinstance(res, ResponseSubset)
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01-2022-12-01.nc",
            )
        )

    def test_split_on_multi_variables(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            start_datetime="2022-01-01",
            end_datetime="2022-05-10",
            minimum_longitude=-9.9,
            maximum_longitude=-9.5,
            minimum_latitude=33.9,
            maximum_latitude=35,
            minimum_depth=5,
            maximum_depth=6,
            variables=["thetao_cglo", "siconc_glor", "vo_cglo"],
            split_on="variable",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 3
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_glo_phy-all_my_0.25deg_P1D-m_thetao_cglo_9.75W-9.50W_34.00N-35.00N_5.14m_2022-01-01-2022-05-10.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_glo_phy-all_my_0.25deg_P1D-m_siconc_glor_9.75W-9.50W_34.00N-35.00N_2022-01-01-2022-05-10.nc",
            )
        )
        path_vo_cglo = os.path.join(
            tmp_path,
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m_vo_cglo_9.75W-9.50W_34.00N-35.00N_5.14m_2022-01-01-2022-05-10.nc",
        )
        assert os.path.exists(path_vo_cglo)
        ds_vo_cglo = xarray.open_dataset(path_vo_cglo)
        assert (
            ds_vo_cglo.time.min().item()
            == dt.fromisoformat("2022-01-01T00:00:00Z").timestamp() * 1e9
        )
        assert (
            ds_vo_cglo.time.max().item()
            == dt.fromisoformat("2022-05-10T00:00:00Z").timestamp() * 1e9
        )
        assert len(ds_vo_cglo.data_vars) == 1
        assert "vo_cglo" in ds_vo_cglo.data_vars
        ds_vo_cglo.close()

    def test_split_on_day(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-04-01",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            split_on="day",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 4
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-02-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-03-01.nc",
            )
        )

        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-04-01.nc",
            )
        )

    def test_split_on_hour(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-01-10",
            minimum_latitude=45,
            maximum_latitude=50,
            minimum_longitude=-10,
            maximum_longitude=0,
            minimum_depth=0,
            maximum_depth=100,
            split_on="hour",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 13
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_9.89W-0.00W_45.00N-49.93N_0.00-100.00m_2022-01-01.nc",
            )
        )
