import os

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
            output_directory=tmp_path,
        )
        assert isinstance(res, ResponseSubset)
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-01-01-2023-05-01.nc",
            )
        )

    def test_split_on_year(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            split_on="year",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 2
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2022-12-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-01-01-2023-05-01.nc",
            )
        )

    def test_split_on_month(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-05-10",
            split_on="month",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 17
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-02-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-03-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-04-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-05-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-02-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-03-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-04-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-05-01.nc",
            )
        )

    def test_split_on_variable(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2022-12-31",
            split_on="variable",
            output_directory=tmp_path,
        )
        assert isinstance(res, ResponseSubset)
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01-2022-12-01.nc",
            )
        )

    def test_split_on_day(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-01-10",
            split_on="day",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 13
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-02-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2023-01-01.nc",
            )
        )

    def test_split_on_hour(self, tmp_path):
        res = subset(
            dataset_id="cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m",
            start_datetime="2022-01-01",
            end_datetime="2023-01-10",
            split_on="hour",
            output_directory=tmp_path,
        )
        assert isinstance(res, list)
        assert len(res) == 13
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01.nc",
            )
        )
        assert os.path.exists(
            os.path.join(
                tmp_path,
                "cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_chl_19.89W-13.00E_40.07N-65.00N_0.00-5000.00m_2022-01-01.nc",
            )
        )
