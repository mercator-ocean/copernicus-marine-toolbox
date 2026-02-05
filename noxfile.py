import os

import nox

PYTHON_VERSIONS = ["3.14"]  # ["3.10", "3.11", "3.12", "3.13", "3.14"]
XARRAY_VERSIONS = ["2024.10.0", "latest"]
DASK_VERSIONS = ["2024.8.1", "latest"]
BOTO3_VERSIONS = ["1.26.0", "latest"]
NUMPY_VERSIONS = ["2.1.0", "latest"]
ZARR_VERSIONS = ["2.18.3", "latest"]
H5NETCDF_VERSIONS = ["1.4.0", "latest"]


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize("xarray_version", XARRAY_VERSIONS)
@nox.parametrize("dask_version", DASK_VERSIONS)
@nox.parametrize("boto3_version", BOTO3_VERSIONS)
@nox.parametrize("numpy_version", NUMPY_VERSIONS)
@nox.parametrize("zarr_version", ZARR_VERSIONS)
@nox.parametrize("h5netcdf_version", H5NETCDF_VERSIONS)
def tests(
    session,
    xarray_version,
    dask_version,
    boto3_version,
    numpy_version,
    zarr_version,
    h5netcdf_version,
):
    """
    Basic test of the toolbox against multiple versions.
    """
    if (
        session.python == "3.14"
        and os.getenv("RUNNER_OPERATING_SYSTEM") == "windows-latest"
        and numpy_version == "2.1.2"
    ):
        # see https://numpy.org/doc/stable/release/2.3.2-notes.html
        # for the first Windows 64 - Python 3.14 compatible version of numpy
        numpy_version = "2.3.2"
        session.log(
            "Python 3.14 on Windows requires numpy 2.3.2 or higher, "
            "using numpy 2.3.2 for this test"
        )

    if (
        session.python == "3.14"
        and os.getenv("RUNNER_OPERATING_SYSTEM") == "macos-latest"
        and numpy_version == "2.1.0"
    ):
        # see https://numpy.org/doc/stable/release/2.1.1-notes.html
        # where a bug for macOS wheel was fixed
        numpy_version = "2.1.1"
        session.log(
            "Python 3.14 on macOS requires numpy 2.1.1 or higher, "
            "using numpy 2.1.1 for this test"
        )

    session.install(
        format_to_correct_pip_command("xarray", xarray_version),
        format_to_correct_pip_command("dask", dask_version),
        format_to_correct_pip_command("boto3", boto3_version),
        format_to_correct_pip_command("numpy", numpy_version),
        format_to_correct_pip_command("zarr", zarr_version),
        format_to_correct_pip_command("h5netcdf", h5netcdf_version),
        "pytest==7.4.0",
    )

    session.run("pytest", "tests_dependencies_versions/test_basic_commands.py")


def format_to_correct_pip_command(package_name: str, version: str):
    if ">" in version or "<" in version:
        return f"{package_name}{version}"
    return (
        f"{package_name}=={version}" if version != "latest" else package_name
    )
