import glob
import os
from unittest import mock

import pytest

from copernicusmarine import subset
from copernicusmarine.core_functions.models import ResponseSubset
from tests.resources.mock_stac_catalog_WAW3.mock_marine_data_store_stac_metadata import (  # noqa: E501
    mocked_stac_requests_get,
)

error_message = (
    "The dataset cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m"
    ", version '202012', part 'default' is currently being updated."
    " Data after 2023-05-01T00:00:00Z may not be up to date."
)


class TestSubsetUpdatingDate:
    @pytest.fixture(autouse=True)
    def cleanup_files(monkeypatch):
        yield
        files = glob.glob(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m*"
        )
        for f in files:
            os.remove(f)

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_no_split(self, snapshot):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2023-01-01",
            end_datetime="2023-05-10",
        )
        assert isinstance(res, ResponseSubset)

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_split_on_year(self, snapshot):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            split_on="year",
        )
        assert isinstance(res, list)
        assert len(res) == 2
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2023.nc"
        )

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_split_on_season(self, snapshot):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            split_on="season",
        )
        assert isinstance(res, list)
        assert len(res) == 4
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_DJF.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_MAM.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_JJA.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_SON.nc"
        )

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_split_on_month(self, snapshot):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            split_on="month",
        )
        assert isinstance(res, list)
        assert len(res) == 17
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-01.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-02.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-03.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-04.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-05.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-06.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-07.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-08.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-09.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-10.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-11.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-12.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2023-01.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2023-02.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2023-03.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2023-04.nc"
        )
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_2022-05.nc"
        )

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_split_on_variable(self, snapshot):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            split_on="variable",
        )
        assert isinstance(res, list)
        assert len(res) == 1
        assert os.path.exists(
            "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2023-05-01_chl.nc"
        )
