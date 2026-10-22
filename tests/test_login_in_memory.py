from unittest.mock import Mock

import pytest

from copernicusmarine import login
from copernicusmarine.core_functions import credentials_utils
from copernicusmarine.core_functions.credentials_utils import (
    DEFAULT_CLIENT_CREDENTIALS_FILENAME,
    CouldNotConnectToAuthenticationSystem,
    get_username_password,
)
from copernicusmarine.core_functions.request_structure import (
    create_get_request,
    create_subset_request,
)


@pytest.fixture
def memory_login(monkeypatch):
    monkeypatch.setattr(credentials_utils, "_in_memory_credentials", None)
    monkeypatch.delenv("COPERNICUSMARINE_MYOCEAN_AUTH", raising=False)
    validator = Mock(return_value="test-user")
    monkeypatch.setattr(credentials_utils, "_validate_and_get_user", validator)
    return validator


def test_login_without_saving_creates_no_directory(tmp_path, memory_login):
    directory = tmp_path / "credentials"

    assert login(
        username="test-user",
        password="test-password",
        configuration_file_directory=directory,
        save=False,
    )

    memory_login.assert_called_once_with("test-user", "test-password")
    assert not directory.exists()
    assert get_username_password(None, None, None) == (
        "test-user",
        "test-password",
    )


def test_login_without_saving_preserves_existing_file(tmp_path, memory_login):
    credentials_file = tmp_path / DEFAULT_CLIENT_CREDENTIALS_FILENAME
    credentials_file.write_text("existing contents")

    assert login(
        username="test-user",
        password="test-password",
        configuration_file_directory=tmp_path,
        save=False,
    )

    assert credentials_file.read_text() == "existing contents"


@pytest.mark.parametrize("request_type", ["subset", "get"])
def test_request_reuses_in_memory_credentials(
    request_type, tmp_path, monkeypatch, memory_login
):
    assert login(username="test-user", password="test-password", save=False)
    memory_login.reset_mock()
    monkeypatch.setattr(
        credentials_utils, "COPERNICUSMARINE_SERVICE_USERNAME", "env-user"
    )
    monkeypatch.setattr(
        credentials_utils, "COPERNICUSMARINE_SERVICE_PASSWORD", "env-password"
    )
    load_file = Mock(side_effect=AssertionError("Unexpected file access"))
    prompt = Mock(side_effect=AssertionError("Unexpected prompt"))
    monkeypatch.setattr(
        credentials_utils,
        "_retrieve_credential_from_configuration_files",
        load_file,
    )
    monkeypatch.setattr(
        credentials_utils, "_retrieve_credential_from_prompt", prompt
    )

    if request_type == "subset":
        request = create_subset_request(dataset_id="dataset")
    else:
        request = create_get_request(
            dataset_id="dataset",
            dataset_version=None,
            dataset_part=None,
            username=None,
            password=None,
            credentials_file=None,
            no_directories=False,
            output_directory=tmp_path,
            overwrite=False,
            request_file=None,
            filter=None,
            regex=None,
            file_list=None,
            create_file_list=None,
            sync=False,
            sync_delete=False,
            skip_existing=False,
            index_parts=False,
            dry_run=True,
            max_concurrent_requests=1,
            disable_progress_bar=True,
        )

    assert request.username == "test-user"
    memory_login.assert_called_once_with("test-user", "test-password")
    load_file.assert_not_called()
    prompt.assert_not_called()


def test_explicit_credentials_override_memory(memory_login):
    assert login(username="test-user", password="test-password", save=False)

    assert get_username_password(
        "explicit-user", "explicit-password", None
    ) == (
        "explicit-user",
        "explicit-password",
    )
    assert get_username_password(None, None, None) == (
        "test-user",
        "test-password",
    )


def test_memory_overrides_explicit_credentials_file(memory_login, tmp_path):
    assert login(username="test-user", password="test-password", save=False)
    (
        credentials_file,
        _,
    ) = credentials_utils.create_copernicusmarine_configuration_file(
        "file-user", "file-password", tmp_path, True
    )

    assert get_username_password(None, None, credentials_file) == (
        "test-user",
        "test-password",
    )


@pytest.mark.parametrize("source", ["environment", "file"])
def test_credential_resolution_without_memory_is_unchanged(
    source, tmp_path, monkeypatch, memory_login
):
    monkeypatch.setattr(
        credentials_utils,
        "COPERNICUSMARINE_SERVICE_USERNAME",
        "env-user" if source == "environment" else None,
    )
    monkeypatch.setattr(
        credentials_utils,
        "COPERNICUSMARINE_SERVICE_PASSWORD",
        "env-password" if source == "environment" else None,
    )
    (
        credentials_file,
        _,
    ) = credentials_utils.create_copernicusmarine_configuration_file(
        "file-user", "file-password", tmp_path, True
    )

    assert get_username_password(None, None, credentials_file) == (
        f"{'env' if source == 'environment' else 'file'}-user",
        f"{'env' if source == 'environment' else 'file'}-password",
    )
    memory_login.assert_not_called()


@pytest.mark.parametrize("source", ["environment", "prompt"])
def test_in_memory_login_resolves_missing_credentials(
    source, monkeypatch, memory_login
):
    monkeypatch.setattr(
        credentials_utils,
        "COPERNICUSMARINE_SERVICE_USERNAME",
        "test-user" if source == "environment" else None,
    )
    monkeypatch.setattr(
        credentials_utils,
        "COPERNICUSMARINE_SERVICE_PASSWORD",
        "test-password" if source == "environment" else None,
    )
    prompt = Mock(side_effect=["test-user", "test-password"])
    monkeypatch.setattr(
        credentials_utils, "_retrieve_credential_from_prompt", prompt
    )

    assert login(save=False)

    memory_login.assert_called_once_with("test-user", "test-password")
    assert prompt.call_count == (2 if source == "prompt" else 0)
    assert get_username_password(None, None, None) == (
        "test-user",
        "test-password",
    )


def test_invalid_credentials_are_not_stored(tmp_path, memory_login, caplog):
    memory_login.return_value = None

    assert not login(
        username="invalid",
        password="invalid",
        configuration_file_directory=tmp_path / "credentials",
        save=False,
    )

    assert credentials_utils._in_memory_credentials is None
    assert not (tmp_path / "credentials").exists()
    assert "Invalid credentials." in caplog.text


def test_in_memory_login_replaces_only_with_valid_credentials(memory_login):
    assert login(username="first", password="first-password", save=False)
    assert login(username="second", password="second-password", save=False)
    memory_login.return_value = None

    assert not login(username="invalid", password="invalid", save=False)
    assert get_username_password(None, None, None) == (
        "second",
        "second-password",
    )


def test_authentication_failure_does_not_store_credentials(memory_login):
    memory_login.side_effect = CouldNotConnectToAuthenticationSystem()

    with pytest.raises(CouldNotConnectToAuthenticationSystem):
        login(username="test-user", password="test-password", save=False)

    assert credentials_utils._in_memory_credentials is None


def test_check_only_does_not_store_credentials(memory_login, tmp_path):
    assert login(
        username="test-user",
        password="test-password",
        configuration_file_directory=tmp_path / "credentials",
        check_credentials_valid=True,
        save=False,
    )

    assert credentials_utils._in_memory_credentials is None
    assert not (tmp_path / "credentials").exists()


def test_default_login_still_saves_credentials(tmp_path, memory_login):
    assert login(
        username="test-user",
        password="test-password",
        configuration_file_directory=tmp_path,
        force_overwrite=True,
    )

    assert (tmp_path / DEFAULT_CLIENT_CREDENTIALS_FILENAME).is_file()
    assert credentials_utils._in_memory_credentials is None
