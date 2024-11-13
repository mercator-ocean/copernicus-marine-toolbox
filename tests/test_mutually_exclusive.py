from tests.test_utils import execute_in_terminal


class TestMutuallyExclusive:
    def test_all_mutually_exclusive_arguments(self):
        self.mutually_exclusive_arguments_returns_error(
            "get", "--skip-existing", "--overwrite"
        )
        self.mutually_exclusive_arguments_returns_error(
            "get", "--skip-existing", "--sync"
        )
        self.mutually_exclusive_arguments_returns_error(
            "get", "--skip-existing", "--sync-delete"
        )
        self.mutually_exclusive_arguments_returns_error(
            "get", "--sync", "--overwrite"
        )
        self.mutually_exclusive_arguments_returns_error(
            "get", "--sync-delete", "--overwrite"
        )
        self.mutually_exclusive_arguments_returns_error(
            "subset", "--skip-existing", "--no-directories"
        )
        self.mutually_exclusive_arguments_returns_error(
            "subset", "--no-directories", "--overwrite"
        )
        self.mutually_exclusive_arguments_returns_error(
            "subset", "--sync-delete", "--no-directories"
        )
        self.mutually_exclusive_arguments_returns_error(
            "subset", "--sync", "--no-directories"
        )

    def mutually_exclusive_arguments_returns_error(self, com, arg1, arg2):
        if com == "subset":
            command = [
                "copernicusmarine",
                "subset",
                "--dataset-id",
                "cmems_mod_med_bgc-bio_anfc_4.2km_P1D-m",
                "--variable",
                "nppv",
                f"{arg1}",
                f"{arg2}",
                "--dry-run",
            ]
        else:
            command = [
                "copernicusmarine",
                "get",
                "--dataset-id",
                "cmems_mod_med_bgc-bio_anfc_4.2km_P1D-m",
                f"{arg1}",
                f"{arg2}",
                "--dry-run",
            ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 2
