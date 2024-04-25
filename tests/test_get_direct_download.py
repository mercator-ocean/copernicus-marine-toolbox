import os
import pathlib
from unittest import mock

from copernicusmarine import get
from tests.test_utils import execute_in_terminal

DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE = (
    "tests/resources/direct_download_file_list.txt"
)
DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_WITH_ONE_WRONG = (
    "tests/resources/direct_download_file_list_with_one_wrong.txt"
)


class TestGetDirectDownload:
    def _get_direct_download(self, direct_download_passed_value: str):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--direct-download",
            direct_download_passed_value,
            "--force-download",
            "--show-outputnames",
            "--overwrite-output-data",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            b"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            b"index_history.txt"
        ) in self.output.stdout

        assert b"Skipping" not in self.output.stdout
        self._assert_file_exists_locally("index_history.txt")
        assert self.output.returncode == 0

    def test_get_direct_download_get_index_files(self):
        self._get_direct_download("index_history.txt")

    def test_get_direct_download_file_list(self):
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
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            b"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            b"history/BO/AR_PR_BO_58JM.nc"
        ) in self.output.stdout
        assert b"Skipping" not in self.output.stdout
        self._assert_file_exists_locally("history/BO/AR_PR_BO_58JM.nc")
        self._assert_file_exists_locally("history/BO/AR_PR_BO_58US.nc")
        assert self.output.returncode == 0

    @mock.patch(
        "copernicusmarine.download_functions.download_original_files._download_header",
        return_value=None,
    )
    def test_get_direct_download_not_found(self, mock_download_header):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--direct-download",
            "lololo",
            "--force-download",
            "--show-outputnames",
            "--overwrite-output-data",
        ]
        self.output = execute_in_terminal(self.command)
        assert b"Skipping" in self.output.stdout
        assert (
            b"No files found to download for direct download. "
            b"Please check the files to download."
        ) in self.output.stdout
        assert self.output.returncode == 0

    @mock.patch(
        "copernicusmarine.download_functions.download_original_files._download_header",
        return_value=None,
    )
    def test_get_direct_download_list_file_only_one_not_found(
        self, mock_download_header
    ):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--file-list",
            DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE_WITH_ONE_WRONG,
            "--force-download",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"File s3://mdl-native-01/native/"
            b"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            b"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            b"lololo not found on the server. Skipping."
        ) in self.output.stdout
        assert (
            b"history/BO/AR_PR_BO_58JM.nc not found on the server. Skipping."
        ) not in self.output.stdout
        self._assert_file_exists_locally("history/BO/AR_PR_BO_58JM.nc")
        assert self.output.returncode == 0

    def test_get_direct_download_different_path_types(self):
        s3_bucket = "s3://mdl-native-01/native"
        product_id = "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030"
        dataset_id = "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311"
        file_path = "index_history.txt"
        full_path = f"{s3_bucket}/{product_id}/{dataset_id}/{file_path}"
        with_product_id = f"{product_id}/{dataset_id}/{file_path}"
        with_dataset_id = f"{dataset_id}/{file_path}"
        with_file_path = f"{file_path}"
        self._get_direct_download(full_path)
        self._get_direct_download(with_product_id)
        self._get_direct_download(with_dataset_id)
        self._get_direct_download(with_file_path)

    # Python interface tests

    def test_get_direct_download_python(self):
        get_result = get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            direct_download="index_history.txt",
            force_download=True,
            show_outputnames=True,
            overwrite_output_data=True,
        )
        assert get_result == [
            pathlib.Path(
                "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
                "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
                "index_history.txt"
            )
        ]
        self._assert_file_exists_locally("index_history.txt")

    def test_get_direct_download_file_list_python(self):
        get_result = get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            file_list=pathlib.Path(DIRECT_DOWNLOAD_FILE_LIST_EXAMPLE),
            force_download=True,
            show_outputnames=True,
            overwrite_output_data=True,
        )
        assert set(get_result) == {
            pathlib.Path(
                "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
                "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
                "history/BO/AR_PR_BO_58JM.nc"
            ),
            pathlib.Path(
                "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
                "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
                "history/BO/AR_PR_BO_58US.nc"
            ),
        }
        for file_path in get_result:
            assert os.path.exists(file_path)

    def _assert_file_exists_locally(self, file_name: str):
        assert os.path.exists(
            f"INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            f"cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            f"{file_name}"
        )
