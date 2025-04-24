from json import loads

from tests.test_utils import (
    execute_in_terminal,
    main_checks_when_file_is_downloaded,
)


class TestBasicCommandsBinaries:
    def test_big_subset(self, tmp_path):
        """
        This command can take several minutes or even end up in a memory issue
        because of the dask graph

        copernicusmarine subset -i cmems_mod_glo_phy_my_0.083deg_P1D-m -t "2013-08-01" -T "2013-08-01" -x 113.896034  -y -11.045679 -Y -6.366948 -z 0 -Z 5000
        """  # noqa
        output_filename = "subset_test.zarr"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
            "-t",
            "2019-08-01",
            "-T",
            "2020-08-01",
            "-x",
            "0",
            "-X",
            "30",
            "-y",
            "0",
            "-Y",
            "30",
            "-z",
            "0",
            "-Z",
            "50",
            "--output-directory",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--file-format",
            "zarr",
        ]
        # around 11-12 min on github machines
        self.output = execute_in_terminal(command, timeout_second=14 * 60)
        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )
