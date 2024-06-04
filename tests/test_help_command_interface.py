from tests.test_utils import execute_in_terminal


class TestHelpCommandLineInterface:
    def test_help_from_describe_is_as_expected(self, snapshot):
        help_output = execute_in_terminal(
            ["copernicusmarine", "describe", "--help"]
        )

        assert str(help_output).split("\\n") == snapshot

    def test_help_from_get_is_as_expected(self, snapshot):
        help_output = execute_in_terminal(
            ["copernicusmarine", "get", "--help"]
        )

        assert str(help_output).split("\\n") == snapshot

    def test_help_from_subset_is_as_expected(self, snapshot):
        help_output = execute_in_terminal(
            ["copernicusmarine", "subset", "--help"]
        )

        assert str(help_output).split("\\n") == snapshot

    def test_help_from_login_is_as_expected(self, snapshot):
        help_output = execute_in_terminal(
            ["copernicusmarine", "login", "--help"]
        )

        assert str(help_output).split("\\n") == snapshot

    def test_help_from_copernicusmarin_is_as_expected(self, snapshot):
        help_output = execute_in_terminal(["copernicusmarine", "--help"])

        assert str(help_output).split("\\n") == snapshot
