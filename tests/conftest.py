import pytest

CREDENTIALS_CHECK_DISABLED_ENV_VAR = "CREDENTIALS_CHECK_DISABLED_ENV_VAR"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "credentials: mark a test as requiring valid Copernicus Marine "
        "credentials (real secrets). These tests exercise the authentication "
        "itself and are excluded from the default, secret-free test run. "
        "Select them with `-m credentials` and deselect them with "
        "`-m 'not credentials'`.",
    )


@pytest.fixture(autouse=True)
def _mock_credentials_check(request, monkeypatch):
    """
    Bypass the Copernicus Marine authentication check by default.

    Accessing the data stores does not actually require authentication
    (anonymous S3 access), only the credentials *validation* does. Mocking it
    here lets the bulk of the test suite run without real credentials/secrets,
    which also makes the tests runnable on forks.

    Tests marked with ``@pytest.mark.credentials`` test the authentication
    itself and therefore run against the real authentication system: for those,
    the bypass is disabled and valid credentials (secrets) are required.
    """
    if request.node.get_closest_marker("credentials"):
        monkeypatch.delenv(CREDENTIALS_CHECK_DISABLED_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(CREDENTIALS_CHECK_DISABLED_ENV_VAR, "True")
