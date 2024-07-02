import os
import pathlib
from unittest import mock

from copernicusmarine import get
from tests.test_utils import execute_in_terminal

DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE = (
    "tests/resources/file_list_examples/direct_download_file_list.txt"
)
DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_WITH_ONE_WRONG = (
    "tests/resources/file_list_examples/"
    "direct_download_file_list_with_one_wrong.txt"
)
DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_DIIFERENT_PATH_TYPES = (
    "tests/resources/file_list_examples/"
    "direct_download_file_list_different_path_types.txt"
)

DIRECT_DOWNLOAD_FAILS_BUT_LISTING_SUCCEEDS = (
    "tests/resources/file_list_examples/"
    "direct_download_fails_listing_succeeds.txt"
)


class TestGetDirectDownload:
    def test_get_direct_download_file_list(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE,
            "--force-download",
            "--show-outputnames",
            "--overwrite-output-data",
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            b"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            b"history/BO/AR_PR_BO_58JM.nc"
        ) in self.output.stdout
        assert b"Skipping" not in self.output.stderr
        self._assert_insitu_file_exists_locally(
            tmp_path, "history/BO/AR_PR_BO_58JM.nc"
        )
        self._assert_insitu_file_exists_locally(
            tmp_path, "history/BO/AR_PR_BO_58US.nc"
        )
        assert self.output.returncode == 0

    # Mocking to skip the listing and search phase
    @mock.patch(
        "copernicusmarine.download_functions.download_original_files._download_header",
        return_value=None,
    )
    def test_get_direct_download_list_file_only_one_not_found(
        self, mock_download_header, tmp_path
    ):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_WITH_ONE_WRONG,
            "--force-download",
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"File s3://mdl-native-01/native/"
            b"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            b"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            b"lololo not found on the server. Skipping."
        ) in self.output.stderr
        assert (
            b"history/BO/AR_PR_BO_58JM.nc not found on the server. Skipping."
        ) not in self.output.stderr
        self._assert_insitu_file_exists_locally(
            tmp_path, file_name="history/BO/AR_PR_BO_58JM.nc"
        )
        assert self.output.returncode == 0

    def test_get_direct_download_different_path_types(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_DIIFERENT_PATH_TYPES,
            "--force-download",
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert b"WARNING" not in self.output.stderr
        assert b"Skipping" not in self.output.stderr
        assert self.output.returncode == 0

    def test_get_direct_download_fails_but_listing_succeeds(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--file-list",
            DIRECT_DOWNLOAD_FAILS_BUT_LISTING_SUCCEEDS,
            "--force-download",
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert b"Skipping" in self.output.stderr
        assert (
            b"No files found to download for direct download."
            in self.output.stdout
        )
        assert os.path.exists(
            f"{tmp_path}/"
            f"IBI_MULTIYEAR_PHY_005_002/"
            f"cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/"
            f"2021/"
            f"CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20211001_20211031_R20230101_RE01.nc"
        )
        assert self.output.returncode == 0

    # Python interface tests

    def test_get_direct_download_file_list_python(self, tmp_path):
        get_result = get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            file_list=pathlib.Path(DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE),
            force_download=True,
            show_outputnames=True,
            overwrite_output_data=True,
            output_directory=tmp_path,
        )
        assert set(get_result) == {
            pathlib.Path(
                f"{tmp_path}/"
                f"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
                f"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
                f"history/BO/AR_PR_BO_58JM.nc"
            ),
            pathlib.Path(
                f"{tmp_path}/"
                f"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
                f"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
                f"history/BO/AR_PR_BO_58US.nc"
            ),
        }
        for file_path in get_result:
            assert os.path.exists(file_path)

    def _assert_insitu_file_exists_locally(
        self,
        temp_path,
        file_name: str,
    ):
        assert os.path.exists(
            f"{temp_path}/"
            f"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            f"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            f"{file_name}"
        )
