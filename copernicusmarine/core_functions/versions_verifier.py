import logging

import semver

import copernicusmarine
from copernicusmarine.core_functions.sessions import (
    get_configured_request_session,
)
from copernicusmarine.core_functions.utils import (
    construct_query_params_for_marine_data_store_monitoring,
)

logger = logging.getLogger("copernicus_marine_root_logger")


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
        marine_data_store_versions = (
            VersionVerifier._get_client_required_versions(staging)
        )
        client_version = copernicusmarine.__version__
        for (
            service
        ) in VersionVerifier.function_marine_data_store_service_mapping[
            function_name
        ]:
            required_version = marine_data_store_versions[service]
            if not semver.Version.parse(client_version).match(
                required_version
            ):
                logger.debug(
                    f"Client version {client_version} is not compatible with "
                    f"{service}. Service needs version {required_version}."
                )
                logger.error(
                    f"Client version {client_version} is not compatible with current "
                    "backend service. Please update to the latest client version."
                )

    @staticmethod
    def _get_client_required_versions(
        staging: bool,
    ) -> dict[str, str]:
        url_mds_versions = (
            "https://stac-dta.marine.copernicus.eu/mdsVersions.json"
            if staging
            else "https://stac.marine.copernicus.eu/mdsVersions.json"
        )
        logger.debug(f"Getting required versions from {url_mds_versions}")
        session = get_configured_request_session()
        mds_versions: dict[str, str] = session.get(
            url_mds_versions,
            params=construct_query_params_for_marine_data_store_monitoring(),
        ).json()["clientVersions"]
        return mds_versions
