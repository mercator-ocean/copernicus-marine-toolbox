from pandas import DataFrame
from xarray import Dataset

import copernicusmarine
from tests.test_utils import execute_in_terminal


class TestDatasetVersionSelection:
    def test_get_when_no_version_is_specified_fetches_the_latest_one(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_blk_wav_anfc_2.5km_PT1H-i",
        ]

        output = execute_in_terminal(command)

        assert (
            b"Dataset version was not specified, the latest one was selected:"
            in output.stdout
        )
        assert (
            b'Dataset version was not specified, the latest one was selected: "default"'
            not in output.stdout
        )

    def test_get_when_dataset_has_only_a_default_version(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "METNO-ARC-SEAICE_CONC-L4-NRT-OBS",
        ]

        output = execute_in_terminal(command)

        assert (
            b'Dataset version was not specified, the latest one was selected: "default"'
            in output.stdout
        )

    def test_get_when_dataset_version_is_specified(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "METNO-ARC-SEAICE_CONC-L4-NRT-OBS",
            "--dataset-version",
            "default",
        ]

        output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset version "default"'
            in output.stdout
        )
        assert (
            b"Dataset version was not specified, the latest one was selected:"
            not in output.stdout
        )

    def test_get_when_dataset_specified_version_does_not_exist(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_blk_wav_anfc_2.5km_PT1H-i",
            "--dataset-version",
            "default",
        ]

        output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset version "default"'
            in output.stdout
        )
        assert b'No version "default" found' not in output.stdout

    def test_get_when_dataset_specified_version_does_not_exist_with_forced_service(
        self,
    ):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "CERSAT-GLO-SEAICE_6DAYS_DRIFT_QUICKSCAT_RAN-OBS_FULL_TIME_SERIE",
            "--dataset-version",
            "default",
            "--service",
            "files",
        ]

        output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset version "default"'
            in output.stdout
        )
        assert b'No version "default" found' not in output.stdout

    def test_subset_when_no_version_is_specified_fetches_the_latest_one(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_blk_wav_anfc_2.5km_PT1H-i",
            "--variable",
            "ice_concentration",
        ]

        output = execute_in_terminal(command)

        assert (
            b"Dataset version was not specified, the latest one was selected:"
            in output.stdout
        )
        assert (
            b'Dataset version was not specified, the latest one was selected: "default"'
            not in output.stdout
        )

    def test_subset_when_dataset_has_only_a_default_version(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "METNO-ARC-SEAICE_CONC-L4-NRT-OBS",
            "--variable",
            "ice_concentration",
        ]

        output = execute_in_terminal(command)

        assert (
            b'Dataset version was not specified, the latest one was selected: "default"'
            in output.stdout
        )

    def test_subset_when_dataset_version_is_specified(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "METNO-ARC-SEAICE_CONC-L4-NRT-OBS",
            "--variable",
            "ice_concentration",
            "--dataset-version",
            "default",
        ]

        output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset version "default"'
            in output.stdout
        )
        assert (
            b"Dataset version was not specified, the latest one was selected:"
            not in output.stdout
        )

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

        output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset version "default"'
            in output.stdout
        )
        assert b'No version "default" found' not in output.stdout

    def test_subset_when_dataset_specified_version_does_not_exist_with_forced_service(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "CERSAT-GLO-SEAICE_6DAYS_DRIFT_QUICKSCAT_RAN-OBS_FULL_TIME_SERIE",
            "--variable",
            "ice_concentration",
            "--dataset-version",
            "default",
            "--service",
            "geoseries",
        ]

        output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset version "default"'
            in output.stdout
        )
        assert b'No version "default" found' not in output.stdout

    def test_dataset_version_is_specifiable_in_python_with_get(self, caplog):
        try:
            copernicusmarine.get(
                dataset_id="METNO-ARC-SEAICE_CONC-L4-NRT-OBS",
                dataset_version="default",
            )
        except OSError:
            pass
        assert (
            'You forced selection of dataset version "default"' in caplog.text
        )
        assert (
            "Dataset version was not specified, the latest one was selected:"
            not in caplog.text
        )

    def test_dataset_version_is_specifiable_in_python_with_subset(
        self, caplog
    ):
        try:
            copernicusmarine.subset(
                dataset_id="SST_MED_SST_L4_NRT_OBSERVATIONS_010_004_a_V2",
                variables=["analysed_sst"],
                minimum_longitude=0,
                maximum_longitude=0,
                minimum_latitude=40,
                maximum_latitude=40,
                dataset_version="default",
            )
        except OSError:
            pass
        assert (
            'You forced selection of dataset version "default"' in caplog.text
        )
        assert (
            "Dataset version was not specified, the latest one was selected:"
            not in caplog.text
        )

    def test_dataset_version_is_specifiable_in_python_with_open_dataset(self):
        assert isinstance(
            copernicusmarine.open_dataset(
                dataset_id="SST_MED_SST_L4_NRT_OBSERVATIONS_010_004_a_V2",
                dataset_version="default",
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
                dataset_version="default",
                variables=["analysed_sst"],
                minimum_longitude=0,
                maximum_longitude=0,
                minimum_latitude=40,
                maximum_latitude=40,
            ),
            DataFrame,
        )
