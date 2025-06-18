from json import loads

from copernicusmarine import get
from tests.test_utils import execute_in_terminal


class TestGetIndexInsituFiles:
    def test_get_index_insitu_files(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--index-parts",
            "--dry-run",
        ]
        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        response_get = loads(self.output.stdout)
        s3_addresses_results = {
            result["s3_url"] for result in response_get["files"]
        }
        assert len(s3_addresses_results) == 4
        assert (
            "s3://mdl-native-01/native/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030"
            "/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/index_history.txt"
            in s3_addresses_results
        )
        assert (
            "s3://mdl-native-01/native/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030"
            "/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/index_latest.txt"
            in s3_addresses_results
        )
        assert (
            "s3://mdl-native-01/native/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030"
            "/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/index_monthly.txt"
            in s3_addresses_results
        )
        assert (
            "s3://mdl-native-01/native/INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030"
            "/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/index_platform.txt"
            in s3_addresses_results
        )

    def test_get_index_insitu_files_not_an_insitu(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-sl_eur_phy-ssh_my_al-l3-duacs_PT1S",
            "--index-parts",
        ]
        self.output = execute_in_terminal(self.command)

        assert "No data to download" in self.output.stderr

    def test_get_index_insitu_files_python(self):
        get_result = get(
            dataset_id="cmems_obs-ins_blk_phybgcwav_mynrt_na_irr",
            index_parts=True,
        )
        assert get_result is not None
        assert all(
            map(
                lambda x: x.exists(),
                [result.file_path for result in get_result.files],
            )
        )
