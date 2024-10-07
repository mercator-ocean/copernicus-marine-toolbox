import copernicusmarine
from tests.test_utils import execute_in_terminal


class TestDatasetPartSelection:
    def test_get_when_force_files_no_part_raises_error(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
        ]

        self.output = execute_in_terminal(command)

        assert (
            b"When dataset has multiple parts and using 'files' service"
            in self.output.stderr
        )

    def test_get_when_dataset_part_is_specified(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
            "--dataset-part",
            "history",
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
        ]

        self.output = execute_in_terminal(command)

        assert (
            b'You forced selection of dataset part "default"'
            in self.output.stderr
        )
        assert b'No part "default" found' not in self.output.stderr

    def test_dataset_part_is_specifiable_in_python_with_get(self, caplog):
        try:
            copernicusmarine.get(
                dataset_id="cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
                dataset_part="history",
            )
        except OSError:
            pass
        assert 'You forced selection of dataset part "history"' in caplog.text
        assert (
            "Dataset part was not specified, the first one was selected:"
            not in caplog.text
        )
