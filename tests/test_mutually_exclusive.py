from tests.test_utils import execute_in_terminal


class TestMutuallyExclusive:
    def test_all_mutually_exclusive_arguments(self):
        self.when_mutually_exclusive_arguments(
            "get", "--skip-existing", "--overwrite-output-data"
        )
        self.when_mutually_exclusive_arguments(
            "get", "--skip-existing", "--sync"
        )
        self.when_mutually_exclusive_arguments(
            "get", "--skip-existing", "--sync-delete"
        )
        self.when_mutually_exclusive_arguments(
            "get", "--sync", "--overwrite-output-data"
        )
        self.when_mutually_exclusive_arguments(
            "get", "--sync-delete", "--overwrite-output-data"
        )
        self.when_mutually_exclusive_arguments(
            "subset", "--skip-existing", "--no-directories"
        )
        self.when_mutually_exclusive_arguments(
            "subset", "--no-directories", "--overwrite-output-data"
        )
        self.when_mutually_exclusive_arguments(
            "subset", "--sync-delete", "--no-directories"
        )
        self.when_mutually_exclusive_arguments(
            "subset", "--sync", "--no-directories"
        )

    def when_mutually_exclusive_arguments(self, com, arg1, arg2):
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
            ]
        else:
            command = [
                "copernicusmarine",
                "get",
                "--dataset-id",
                "cmems_mod_med_bgc-bio_anfc_4.2km_P1D-m",
                f"{arg1}",
                f"{arg2}",
            ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 2
