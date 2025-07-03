import json
import os
import pathlib
from unittest import mock

from copernicusmarine import get
from tests.test_utils import FileToCheck, execute_in_terminal

DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE = FileToCheck(
    "tests/resources/file_list_examples/direct_download_file_list.txt"
).get_path()
DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_EXTENDED = FileToCheck(
    "tests/resources/file_list_examples/direct_download_file_list_extended.txt"
).get_path()

DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_WITH_ONE_WRONG = FileToCheck(
    "tests/resources/file_list_examples/"
    "direct_download_file_list_with_one_wrong.txt"
).get_path()
DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_DIFERENT_PATH_TYPES = FileToCheck(
    "tests/resources/file_list_examples/"
    "direct_download_file_list_different_path_types.txt"
).get_path()

DIRECT_DOWNLOAD_FAILS_BUT_LISTING_SUCCEEDS = FileToCheck(
    "tests/resources/file_list_examples/"
    "direct_download_fails_listing_succeeds.txt"
).get_path()


class TestGetDirectDownload:
    def test_get_direct_download_file_list_extended(self, tmp_path):
        self.when_get_direct_download_file_list(tmp_path)
        self.if_skip_option_skipped_with_same_list(tmp_path)
        self.if_skip_option_skipped_with_extended_list(tmp_path)

    def when_get_direct_download_file_list(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE,
            "-r",
            "file_path",
            "--overwrite",
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        response_get = json.loads(self.output.stdout)
        to_check = FileToCheck(
            "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            "history/BO/AR_PR_BO_58JM.nc"
        ).get_path()
        assert [
            "nice"
            for file_get in response_get["files"]
            if to_check in file_get["file_path"]
        ]
        assert "Skipping" not in self.output.stderr
        self._assert_insitu_file_exists_locally(
            tmp_path, "history/BO/AR_PR_BO_58JM.nc"
        )
        self._assert_insitu_file_exists_locally(
            tmp_path, "history/BO/AR_PR_BO_58US.nc"
        )
        assert self.output.returncode == 0

    def if_skip_option_skipped_with_same_list(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE,
            "--skip-existing",
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert "No data to download" in self.output.stderr
        self._assert_insitu_file_exists_locally(
            tmp_path, FileToCheck("/history/BO/AR_PR_BO_58JM.nc").get_path()
        )
        self._assert_insitu_file_exists_locally(
            tmp_path, FileToCheck("/history/BO/AR_PR_BO_58US.nc").get_path()
        )
        assert self.output.returncode == 0

    def if_skip_option_skipped_with_extended_list(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_EXTENDED,
            "--skip-existing",
            "-o",
            str(tmp_path),
            "-r",
            "file_path",
        ]
        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        response_get = json.loads(self.output.stdout)
        file_to_check = FileToCheck(
            "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            "history/BO/AR_PR_BO_LHUW.nc"
        ).get_path()
        assert [
            "found"
            for file_get in response_get["files"]
            if file_to_check in file_get["file_path"]
        ]

        self._assert_insitu_file_exists_locally(
            tmp_path, FileToCheck("/history/BO/AR_PR_BO_58JM.nc").get_path()
        )
        self._assert_insitu_file_exists_locally(
            tmp_path, FileToCheck("history/BO/AR_PR_BO_58US.nc").get_path()
        )
        self._assert_insitu_file_exists_locally(
            tmp_path, FileToCheck("history/BO/AR_PR_BO_LHUW.nc").get_path()
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
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            "File s3://mdl-native-01/native/"
            "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            "lololo not found on the server. Skipping."
        ) in self.output.stderr
        assert (
            "history/BO/AR_PR_BO_58JM.nc not found on the server. Skipping."
        ) not in self.output.stderr
        self._assert_insitu_file_exists_locally(
            tmp_path,
            file_name=FileToCheck("/history/BO/AR_PR_BO_58JM.nc").get_path(),
        )
        assert self.output.returncode == 0

    def test_get_direct_download_different_path_types(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_DIFERENT_PATH_TYPES,
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert "WARNING" not in self.output.stderr
        assert "Skipping" not in self.output.stderr
        assert self.output.returncode == 0

    def test_get_direct_download_fails_but_listing_succeeds(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--file-list",
            DIRECT_DOWNLOAD_FAILS_BUT_LISTING_SUCCEEDS,
            "-o",
            str(tmp_path),
        ]
        self.output = execute_in_terminal(self.command)
        assert "Skipping" in self.output.stderr
        assert (
            "No files found to download for direct download."
            in self.output.stderr
        )
        to_check = FileToCheck(
            "/IBI_MULTIYEAR_PHY_005_002/"
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m_202012/"
            "2021/CMEMS_v5r1_IBI_PHY_MY_PdE_01mav_20211001_20211031_R20230101_RE01.nc"
        ).get_path()
        assert os.path.exists(f"{tmp_path}{to_check}")
        assert self.output.returncode == 0

    # Python interface tests

    def test_get_direct_download_file_list_python(self, tmp_path):
        get_result = get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            file_list=pathlib.Path(DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE),
            overwrite=True,
            output_directory=tmp_path,
        )
        result_paths = [result.file_path for result in get_result.files]
        assert set(result_paths) == {
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
        for file_path in result_paths:
            assert os.path.exists(file_path)

    def _assert_insitu_file_exists_locally(
        self,
        temp_path,
        file_name: str,
    ):
        to_check = FileToCheck(
            "/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
        ).get_path()
        file_name = FileToCheck(file_name).get_path()
        assert os.path.exists(f"{temp_path}{to_check}{file_name}")
