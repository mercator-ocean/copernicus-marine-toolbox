import json
import os

from tests.test_utils import execute_in_terminal


class TestGetSync:
    def test_get_sync(self, tmp_path):
        self.when_I_get_some_native_files_with_sync(tmp_path)
        self.then_same_command_should_not_download(tmp_path)
        self.when_I_delete_one_file(tmp_path)
        self.then_same_command_with_sync_should_download_only_one_file(
            tmp_path
        )

    def test_get_sync_delete(self, tmp_path):
        self.when_I_get_some_native_files_with_sync(tmp_path)
        self.when_I_add_a_file_locally(tmp_path)
        self.then_command_sync_delete_should_propose_to_delete_it_and_delete_it(
            tmp_path
        )

    def test_get_sync_works_for_dataset_with_default_parts(self, tmp_path):
        self.command = self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_arc_phy_anfc_6km_detided_P1D-m",
            "--sync",
            "--dataset-version",
            "202311",
            "-o",
            f"{tmp_path}",
            "--staging",  # TODO: staging dataset for the moment, update when needed
            "--filter",
            "*20230705_dm-metno-MODEL-topaz5*",
            "--response-fields",
            "all",
        ]
        self.output = execute_in_terminal(self.command)
        response_get = json.loads(self.output.stdout)
        file_to_check = (
            "cmems_mod_arc_phy_anfc_6km_detided_P1D-m_202311/"
            "2023/07/20230705_dm-metno-MODEL-topaz5-ARC-b20230710-fv02.0.nc"
        )
        assert [
            "found"
            for file_get in response_get["files"]
            if file_to_check in file_get["file_path"]
        ]
        assert self.output.returncode == 0

    def test_get_sync_works_for_dataset_with_no_default_parts(self, tmp_path):
        self.command = self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--sync",
            "--dataset-version",
            "202311",
            "-o",
            f"{tmp_path}",
            "--filter",
            "*/NO_TS_TG_ZwartsluisTG*",
            "--response-fields",
            "all",
        ]
        self.output = execute_in_terminal(self.command)
        response_get = json.loads(self.output.stdout)
        file_to_check = (
            "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
            "latest/"
        )
        assert [
            "found"
            for file_get in response_get["files"]
            if file_to_check in file_get["file_path"]
        ]
        assert self.output.returncode == 0

    def test_get_sync_with_dataset_part(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
            "--sync",
            "--dataset-version",
            "202311",
            "--dataset-part",
            "history",
            "--filter",
            "*GL_PR_BO_3*",
            "--response-fields",
            "all",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        response_get = json.loads(self.output.stdout)
        file_to_check = (
            "INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/"
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311"
            "/history/BO/GL_PR_BO_3YVG.nc"
        )
        assert [
            "found"
            for file_get in response_get["files"]
            if file_to_check in file_get["file_path"]
        ]

        # now there shouldn't be any files to download
        self.output = execute_in_terminal(self.command)
        assert b"No data to download" in self.output.stderr

    def test_get_sync_needs_version(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--sync",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"Value error: Sync requires to set a dataset version."
            in self.output.stderr
        )

    def when_I_get_some_native_files_with_sync(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--sync",
            "--filter",
            "*202105/2007/01/2007011*",
            "--dataset-version",
            "202105",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(self.command)

    def then_same_command_should_not_download(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--sync",
            "--filter",
            "*202105/2007/01/2007011*",
            "--dataset-version",
            "202105",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(self.command)
        assert b"No data to download" in self.output.stderr

    def when_I_delete_one_file(self, tmp_path):
        self.command = [
            "rm",
            f"{tmp_path}/ARCTIC_MULTIYEAR_BGC_002_005"
            "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            "/2007/01/"
            "20070110_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc",
        ]
        self.output = execute_in_terminal(self.command)

    def then_same_command_with_sync_should_download_only_one_file(
        self, tmp_path
    ):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--sync",
            "--filter",
            "*202105/2007/01/2007011*",
            "--dataset-version",
            "202105",
            "-o",
            f"{tmp_path}",
            "-r",
            "file_path,file_status",
        ]
        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        response_get = json.loads(self.output.stdout)
        to_check = (
            "ARCTIC_MULTIYEAR_BGC_002_005"
            "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            "/2007/01/"
            "20070110_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
        )
        assert [
            "nice"
            for file_get in response_get["files"]
            if to_check in file_get["file_path"]
        ]
        to_check_not_in = (
            "ARCTIC_MULTIYEAR_BGC_002_005"
            "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            "/2007/01/"
            "20070111_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
        )

        assert not [
            "not_nice"
            for file_get in response_get["files"]
            if to_check_not_in in file_get["file_path"]
            and file_get["file_status"] != "IGNORED"
        ]

    def when_I_add_a_file_locally(self, tmp_path):
        self.command = [
            "touch",
            f"{tmp_path}/ARCTIC_MULTIYEAR_BGC_002_005"
            "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            "/2007/01/"
            "20070120_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc",
        ]
        self.output = execute_in_terminal(self.command)

    def then_command_sync_delete_should_propose_to_delete_it_and_delete_it(
        self, tmp_path
    ):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--sync-delete",
            "--filter",
            "*202105/2007/01/2007011*",
            "--dataset-version",
            "202105",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"Some files will be deleted due to sync delete:"
            in self.output.stderr
        )
        assert (
            f"{tmp_path}".encode() + b"/ARCTIC_MULTIYEAR_BGC_002_005"
            b"/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            b"/2007/01/"
            b"20070120_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
            in self.output.stderr
        )
        assert (
            os.path.isfile(
                f"{tmp_path}/ARCTIC_MULTIYEAR_BGC_002_005"
                "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
                "/2007/01/"
                "20070120_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
            )
            is False
        )
