from tests.test_utils import execute_in_terminal

# TODO: maybe reduce the size of the request
BASIC_COMMAND = [
    "copernicusmarine",
    "subset",
    "--dataset-id",
    "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
    "--dataset-part",
    "history",
    "--variable",
    "PSAL",
    "--variable",
    "ATMP",
    "-y",
    "-63.90",
    "-Y",
    "90",
    "-x",
    "-146.99",
    "-X",
    "180",
    "-z",
    "0",
    "-Z",
    "10",
    "--start-datetime",
    "2023-11-25T00:00:00",
    "--end-datetime",
    "2023-12-02T03:00:00",
]


class TestSparseSubset:
    def test_I_can_subset_sparse_data(self, tmp_path):
        command = BASIC_COMMAND + [
            "--output-directory",
            tmp_path,
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (tmp_path / "sparse_data.parquet").exists()

    def test_I_can_subset_on_platform_ids(self, tmp_path):
        command = BASIC_COMMAND + [
            "--platform-id",
            "B-Sulafjorden___MO",
            "--platform-id",
            "F-Vartdalsfjorden___MO",
            "--output-directory",
            tmp_path,
        ]
        expected_files = [
            "B-Sulafjorden___MO_PSAL_6672.0.0.0.parquet",
            "B-Sulafjorden___MO_PSAL_6673.0.0.0.parquet",
            "F-Vartdalsfjorden___MO_PSAL_6672.0.0.0.parquet",
            "F-Vartdalsfjorden___MO_PSAL_6673.0.0.0.parquet",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (tmp_path / "sparse_data.parquet").exists()
        for file_ in expected_files:
            assert (tmp_path / "sparse_data.parquet" / file_).exists()
