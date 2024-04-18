import pathlib

from copernicusmarine import get
from tests.test_utils import execute_in_terminal

DIRECT_DOWNLOAD_MULTIPLE_EXAMPLE = (
    "tests/resources/direct_download_multiple_file.txt"
)
DIRECT_DOWNLOAD_MULTIPLE_EXAMPLE_WITH_ONE_WRONG = (
    "tests/resources/direct_download_multiple_file_with_one_wrong.txt"
)


class TestGetDirectDownload:
    def _get_direct_download_one(self, direct_download_one_passed_value: str):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--direct-download-one",
            direct_download_one_passed_value,
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
        assert self.output.returncode == 0

    def test_get_direct_download_one_get_index_files(self):
        self._get_direct_download_one("index_history.txt")

    def test_get_direct_download_multiple(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--direct-download-multiple",
            DIRECT_DOWNLOAD_MULTIPLE_EXAMPLE,
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
        assert self.output.returncode == 0

    def test_get_direct_download_one_not_found(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--direct-download-one",
            "lololo",
            "--force-download",
            "--show-outputnames",
            "--overwrite-output-data",
        ]
        self.output = execute_in_terminal(self.command)
        assert b"Skipping" in self.output.stdout
        assert b"lololo does not seem to be valid" in self.output.stdout
        assert self.output.returncode == 1

    def test_get_direct_download_multiple_only_one_not_found(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--direct-download-multiple",
            DIRECT_DOWNLOAD_MULTIPLE_EXAMPLE_WITH_ONE_WRONG,
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
        assert self.output.returncode == 0

    def test_get_direct_download_one_different_path_types(self):
        s3_bucket = "s3://mdl-native-01/native"
        product_id = "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030"
        dataset_id = "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311"
        file_path = "index_history.txt"
        full_path = f"{s3_bucket}/{product_id}/{dataset_id}/{file_path}"
        with_product_id = f"{product_id}/{dataset_id}/{file_path}"
        with_dataset_id = f"{dataset_id}/{file_path}"
        with_file_path = f"{file_path}"
        self._get_direct_download_one(full_path)
        self._get_direct_download_one(with_product_id)
        self._get_direct_download_one(with_dataset_id)
        self._get_direct_download_one(with_file_path)

    # Python interface tests

    def test_get_direct_download_one_python(self):
        get_result = get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            direct_download_one="index_history.txt",
            force_download=True,
            show_outputnames=True,
            overwrite_output_data=True,
        )
        print(get_result)
        assert get_result == [
            pathlib.Path(
                "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
                "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
                "index_history.txt"
            )
        ]

    def test_get_direct_download_multiple_python(self):
        get_result = get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            direct_download_multiple=pathlib.Path(
                DIRECT_DOWNLOAD_MULTIPLE_EXAMPLE
            ),
            force_download=True,
            show_outputnames=True,
            overwrite_output_data=True,
        )
        print(get_result)
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
