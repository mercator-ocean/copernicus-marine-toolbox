import logging

import semver

from copernicusmarine.core_functions.sessions import JsonParserConnection
from copernicusmarine.versioner import __version__ as toolbox_version

logger = logging.getLogger("copernicusmarine")


class VersionVerifier:
    function_marine_data_store_service_mapping: dict[str, list[str]] = {
        "describe": ["mds", "mds/serverlessArco/meta"],
        "get": ["mds", "mds/serverlessNative", "mds/serverlessArco/meta"],
        "subset": ["mds", "mds/serverlessArco", "mds/serverlessArco/meta"],
    }

    @staticmethod
    def check_version_describe(staging: bool):
        VersionVerifier._check_version("describe", staging)

    @staticmethod
    def check_version_get(staging: bool):
        VersionVerifier._check_version("get", staging)

    @staticmethod
    def check_version_subset(staging: bool):
        VersionVerifier._check_version("subset", staging)

    @staticmethod
    def _check_version(function_name: str, staging: bool):
        def create_error_message(required_version: str) -> str:
            return (
                f"Installed copernicusmarine version {toolbox_version} might "
                f"lead to unexpected results with the current backend services. "
                f"Minimum supported version is {required_version}. Please update. "
                f"You can find instructions to install the latest version here: "
                f"https://toolbox-docs.marine.copernicus.eu/"
            )

        marine_data_store_versions = (
            VersionVerifier._get_client_required_versions(staging)
        )
        for (
            service
        ) in VersionVerifier.function_marine_data_store_service_mapping[
            function_name
        ]:
            required_version = marine_data_store_versions[service]
            try:
                if not semver.Version.parse(toolbox_version).match(
                    required_version
                ):
                    logger.debug(
                        f"Client version {toolbox_version} is not compatible with "
                        f"{service}. Service needs version {required_version}."
                    )
                    logger.error(create_error_message(required_version))
                    return
            except ValueError:
                logger.warning(
                    f"Using a pre-release or a non-official version "
                    f"of the client. Client version: {toolbox_version}"
                )
                return

    @staticmethod
    def _get_client_required_versions(
        staging: bool,
    ) -> dict[str, str]:
        url_mds_versions = (
            "https://s3.waw3-1.cloudferro.com/mdl-metadata-dta/mdsVersions.json"
            if staging
            else "https://s3.waw3-1.cloudferro.com/mdl-metadata/mdsVersions.json"
        )
        logger.debug(f"Getting required versions from {url_mds_versions}")
        with JsonParserConnection() as connection:
            mds_versions: dict[str, str] = connection.get_json_file(
                url_mds_versions,
            )["clientVersions"]
        return mds_versions
