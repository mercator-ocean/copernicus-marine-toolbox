from pandas import DataFrame
from xarray import Dataset

import copernicusmarine
from tests.test_utils import execute_in_terminal


class TestDatasetVersionSelection:
    def test_get_when_dataset_has_only_a_default_version(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "DMI-ARC-SEAICE_TEMP-L4-NRT-OBS",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert b'Selected dataset version: "default"' in self.output.stderr

    def test_get_when_dataset_version_is_specified(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "DMI-ARC-SEAICE_TEMP-L4-NRT-OBS",
            "--dataset-version",
            "default",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert b'Selected dataset version: "default"' in self.output.stderr

    def test_get_when_dataset_specified_version_does_not_exist(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_blk_wav_anfc_2.5km_PT1H-i",
            "--dataset-version",
            "default",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Dataset version not found: No version found "
            b"for dataset cmems_mod_blk_wav_anfc_2.5km_PT1H-i"
            in self.output.stderr
        )

    def test_subset_when_dataset_has_only_a_default_version(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "DMI-ARC-SEAICE_TEMP-L4-NRT-OBS",
            "--variable",
            "ice_concentration",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert b'Selected dataset version: "default"' in self.output.stderr

    def test_subset_when_dataset_version_is_specified(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "DMI-ARC-SEAICE_TEMP-L4-NRT-OBS",
            "--variable",
            "ice_concentration",
            "--dataset-version",
            "default",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert b'Selected dataset version: "default"' in self.output.stderr

    def test_subset_when_dataset_specified_version_does_not_exist(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_blk_wav_anfc_2.5km_PT1H-i",
            "--variable",
            "ice_concentration",
            "--dataset-version",
            "default",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Dataset version not found: No version found "
            b"for dataset cmems_mod_blk_wav_anfc_2.5km_PT1H-i"
            in self.output.stderr
        )

    def test_dataset_version_is_specifiable_in_python_with_get(self, caplog):

        copernicusmarine.get(
            dataset_id="DMI-ARC-SEAICE_TEMP-L4-NRT-OBS",
            dataset_version="default",
            dry_run=True,
        )
        assert 'Selected dataset version: "default"' in caplog.text

    def test_dataset_version_is_specifiable_in_python_with_subset(
        self, caplog
    ):
        copernicusmarine.subset(
            dataset_id="SST_MED_SST_L4_NRT_OBSERVATIONS_010_004_a_V2",
            variables=["analysed_sst"],
            minimum_longitude=0,
            maximum_longitude=0,
            minimum_latitude=40,
            maximum_latitude=40,
            dataset_version="202311",
            dry_run=True,
        )
        assert 'Selected dataset version: "202311"' in caplog.text

    def test_dataset_version_is_specifiable_in_python_with_open_dataset(self):
        assert isinstance(
            copernicusmarine.open_dataset(
                dataset_id="SST_MED_SST_L4_NRT_OBSERVATIONS_010_004_a_V2",
                dataset_version="202311",
                variables=["analysed_sst"],
                minimum_longitude=0,
                maximum_longitude=0,
                minimum_latitude=40,
                maximum_latitude=40,
            ),
            Dataset,
        )

    def test_dataset_version_is_specifiable_in_python_with_read_dataframe(
        self,
    ):
        assert isinstance(
            copernicusmarine.read_dataframe(
                dataset_id="SST_MED_SST_L4_NRT_OBSERVATIONS_010_004_a_V2",
                dataset_version="202311",
                variables=["analysed_sst"],
                minimum_longitude=0,
                maximum_longitude=0,
                minimum_latitude=40,
                maximum_latitude=40,
            ),
            DataFrame,
        )
