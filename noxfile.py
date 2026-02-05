import nox

PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]
XARRAY_VERSIONS = ["2024.10.0", "latest"]
DASK_VERSIONS = ["2024.8.1", "latest"]
BOTO3_VERSIONS = ["1.26.0", "latest"]
# 2.1.1 fix an issue with macOS and 2.1.2 an issue with Windows
NUMPY_VERSIONS = ["2.1.2", "latest"]
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
