from copernicusmarine import subset
from tests.test_utils import execute_in_terminal


class TestDeprecatedOptions:
    def test_motu_api_request_deprecated_cli(self):
        command = [
            "copernicusmarine",
            "subset",
            "--motu-api-request",
            "some_request",
        ]
        self.output = execute_in_terminal(command)
        assert (
            b"'--motu-api-request' has been deprecated." in self.output.stderr
        )

    def test_motu_api_request_deprecated_api(self, caplog):
        try:
            subset(
                motu_api_request="some_request",
            )
        except Exception:
            pass
        assert "'motu_api_request' has been deprecated." in caplog.text
