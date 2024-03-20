import os

from tests.test_utils import execute_in_terminal


class TestGetSync:
    def test_get_sync(self):
        self.when_I_get_some_native_files_with_sync()
        self.then_same_command_should_not_download()
        self.when_I_delete_one_file()
        self.then_same_command_with_sync_should_download_only_one_file()

    def test_get_sync_delete(self):
        self.when_I_get_some_native_files_with_sync()
        self.when_I_add_a_file_locally()
        self.then_command_sync_delete_should_propose_to_delete_it_and_delete_it()

    def test_get_sync_not_working_with_datasets_with_parts(self):
        self.command = self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_blk_phybgcwav_mynrt_na_irr",
            "--sync",
            "--dataset-version",
            "202311",
            "--force-download",
            "-o",
            "tests/downloads",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"Sync is not supported for datasets with multiple parts."
            in self.output.stdout
        )

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
            in self.output.stdout
        )

    def when_I_get_some_native_files_with_sync(self):
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
            "--force-download",
            "-o",
            "tests/downloads",
        ]
        self.output = execute_in_terminal(self.command)

    def then_same_command_should_not_download(self):
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
            "tests/downloads",
        ]
        self.output = execute_in_terminal(self.command)
        assert b"No data to download" in self.output.stdout

    def when_I_delete_one_file(self):
        self.command = [
            "rm",
            "tests/downloads/ARCTIC_MULTIYEAR_BGC_002_005"
            "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            "/2007/01/"
            "20070110_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc",
        ]
        self.output = execute_in_terminal(self.command)

    def then_same_command_with_sync_should_download_only_one_file(self):
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
            "tests/downloads",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"ARCTIC_MULTIYEAR_BGC_002_005"
            b"/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            b"/2007/01/"
            b"20070110_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
            in self.output.stdout
        )
        assert (
            b"ARCTIC_MULTIYEAR_BGC_002_005"
            b"/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            b"/2007/01/"
            b"20070111_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
            not in self.output.stdout
        )

    def when_I_add_a_file_locally(self):
        self.command = [
            "touch",
            "tests/downloads/ARCTIC_MULTIYEAR_BGC_002_005"
            "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            "/2007/01/"
            "20070120_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc",
        ]
        self.output = execute_in_terminal(self.command)

    def then_command_sync_delete_should_propose_to_delete_it_and_delete_it(
        self,
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
            "--force-download",
            "-o",
            "tests/downloads",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"Some files will be deleted due to sync delete:"
            in self.output.stdout
        )
        assert (
            b"tests/downloads/ARCTIC_MULTIYEAR_BGC_002_005"
            b"/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
            b"/2007/01/"
            b"20070120_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
            in self.output.stdout
        )
        assert (
            os.path.isfile(
                "tests/downloads/ARCTIC_MULTIYEAR_BGC_002_005"
                "/cmems_mod_arc_bgc_my_ecosmo_P1D-m_202105"
                "/2007/01/"
                "20070120_dm-25km-NERSC-MODEL-ECOSMO-ARC-RAN-fv2.0.nc"
            )
            is False
        )
