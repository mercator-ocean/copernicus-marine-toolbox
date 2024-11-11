import copernicusmarine
from tests.test_utils import execute_in_terminal


class TestDatasetPartSelection:
    def test_get_when_dataset_part_is_specified(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
            "--dataset-part",
            "history",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset part "history"'
            in self.output.stderr
        )
        assert (
            b"Dataset part was not specified, the first one was selected:"
            not in self.output.stderr
        )

    def test_get_when_dataset_specified_part_does_not_exist(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
            "--dataset-part",
            "default",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset part "default"'
            in self.output.stderr
        )
        assert self.output.returncode == 1
        assert b'No part "default" found' not in self.output.stderr

    def test_dataset_part_is_specifiable_in_python_with_get(self, caplog):
        copernicusmarine.get(
            dataset_id="cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
            dataset_part="history",
            dry_run=True,
        )
        assert 'You forced selection of dataset part "history"' in caplog.text
        assert (
            "Dataset part was not specified, the first one was selected:"
            not in caplog.text
        )
