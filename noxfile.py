import nox

# Define the supported versions of Python and xarray
PYTHON_VERSIONS = ["3.9", "3.10", "3.11", "3.12", "3.13"]
XARRAY_VERSIONS = ["2023.4.0", "latest"]
DASK_VERSIONS = ["2022.1.0", "latest"]
BOTO3_VERSIONS = ["1.26.0", "latest"]
NUMPY_VERSIONS = ["1.22.0", ">=2.0.0"]


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize("xarray_version", XARRAY_VERSIONS)
@nox.parametrize("dask_version", DASK_VERSIONS)
@nox.parametrize("boto3_version", BOTO3_VERSIONS)
@nox.parametrize("numpy_version", NUMPY_VERSIONS)
def tests(session, xarray_version, dask_version, boto3_version, numpy_version):
    """
    Basic test of the toolbox against multiple versions.
    """
    # Skip invalid combinations
    if dask_version == "2022.1.0" and numpy_version == ">=2.0.0":
        session.log(
            f"Skipping unsupported combination: dask={dask_version} and numpy={numpy_version}"  # noqa: E501
        )
        session.skip()

    # Install dependencies
    session.install(
        format_to_correct_pip_command("xarray", xarray_version),
        format_to_correct_pip_command("dask", dask_version),
        format_to_correct_pip_command("boto3", boto3_version),
        format_to_correct_pip_command("numpy", numpy_version),
        "pytest==7.4.0",
    )

    # Install the library itself (optional, if you're testing a local package)
    session.install(".")

    # Run tests
    session.run("pytest tests_dependencie_versions/test_basic_commands.py")


def format_to_correct_pip_command(package_name: str, version: str):
    if ">" in version or "<" in version:
        return f"{package_name}{version}"
    return (
        f"{package_name}=={version}" if version != "latest" else package_name
    )
