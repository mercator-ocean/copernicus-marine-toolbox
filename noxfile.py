import nox

PYTHON_VERSIONS = ["3.9", "3.10", "3.11", "3.12", "3.13"]
XARRAY_VERSIONS = ["2023.4.0", "latest"]
DASK_VERSIONS = ["2022.1.0", "latest"]
BOTO3_VERSIONS = ["1.26.0", "latest"]
NUMPY_VERSIONS = ["1.26", ">=2.0.0"]


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize("xarray_version", XARRAY_VERSIONS)
@nox.parametrize("dask_version", DASK_VERSIONS)
@nox.parametrize("boto3_version", BOTO3_VERSIONS)
@nox.parametrize("numpy_version", NUMPY_VERSIONS)
def tests(session, xarray_version, dask_version, boto3_version, numpy_version):
    """
    Basic test of the toolbox against multiple versions.
    """
    # Skip invalid combinations, dask 2022.1.0 is not supported with numpy >= 2.0.0
    if dask_version == "2022.1.0" and numpy_version == ">=2.0.0":
        session.log(
            f"Skipping unsupported combination: "
            f"dask={dask_version} and numpy={numpy_version}"
        )
        session.skip()

    session.install(
        format_to_correct_pip_command("xarray", xarray_version),
        format_to_correct_pip_command("dask", dask_version),
        format_to_correct_pip_command("boto3", boto3_version),
        format_to_correct_pip_command("numpy", numpy_version),
        "pytest==7.4.0",
    )

    session.run("pytest", "tests_dependencies_versions/test_basic_commands.py")


def format_to_correct_pip_command(package_name: str, version: str):
    if ">" in version or "<" in version:
        return f"{package_name}{version}"
    return (
        f"{package_name}=={version}" if version != "latest" else package_name
    )
