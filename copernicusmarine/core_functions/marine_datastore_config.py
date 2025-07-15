import logging
from dataclasses import dataclass

import semver

from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_CONFIG_URL,
)
from copernicusmarine.core_functions.sessions import JsonParserConnection
from copernicusmarine.versioner import __version__ as toolbox_version

logger = logging.getLogger("copernicusmarine")

MARINE_DATASTORE_CONFIG_URL_CDN = (
    "https://stac.marine.copernicus.eu/clients-config-v1"
)
MARINE_DATASTORE_CONFIG_URL_DIRECT = (
    COPERNICUSMARINE_CONFIG_URL
    or "https://s3.waw3-1.cloudferro.com/mdl-metadata/clientsConfigV1.json"
)
MARINE_DATASTORE_CONFIG_URL_STAGING = (
    "https://stac-dta.marine.copernicus.eu/clients-config-v1"
)

MARINE_DATASTORE_SERVICES_MAPPING: dict[str, list[str]] = {
    "describe": ["mds", "mds/serverlessArco/meta"],
    "get": ["mds", "mds/serverlessNative", "mds/serverlessArco/meta"],
    "subset": ["mds", "mds/serverlessArco", "mds/serverlessArco/meta"],
}


@dataclass
class CatalogueConfig:
    stac_catalogue_url: str
    dataset_product_mapping_url: str
    root_metadata_url: str


@dataclass
class MarineDataStoreConfig:
    catalogues: list[CatalogueConfig]
    staging: bool


def get_config_and_check_version_describe(
    staging: bool,
) -> MarineDataStoreConfig:
    marine_datastore_config = _check_version("describe", staging)
    return marine_datastore_config


def get_config_and_check_version_get(staging: bool) -> MarineDataStoreConfig:
    marine_datastore_config = _check_version("get", staging)
    return marine_datastore_config


def get_config_and_check_version_subset(
    staging: bool,
) -> MarineDataStoreConfig:
    marine_datastore_config = _check_version("subset", staging)
    return marine_datastore_config


def _check_version(function_name: str, staging: bool) -> MarineDataStoreConfig:
    def create_error_message(required_version: str) -> str:
        return (
            f"Installed copernicusmarine version {toolbox_version} might "
            f"lead to unexpected results with the current backend services. "
            f"Minimum supported version is {required_version}. Please update. "
            f"You can find instructions to install the latest version here: "
            f"https://toolbox-docs.marine.copernicus.eu/"
        )

    (
        marine_datastore_versions,
        marine_datastore_config,
    ) = _get_required_versions_and_config(staging)
    for service in MARINE_DATASTORE_SERVICES_MAPPING[function_name]:
        required_version = marine_datastore_versions[service]
        try:
            if not semver.Version.parse(toolbox_version).match(
                required_version
            ):
                logger.debug(
                    f"Client version {toolbox_version} is not compatible with "
                    f"{service}. Service needs version {required_version}."
                )
                logger.error(create_error_message(required_version))
        except ValueError:
            logger.warning(
                f"Using a pre-release or a non-official version "
                f"of the client. Client version: {toolbox_version}"
            )
    return marine_datastore_config


def _get_required_versions_and_config(
    staging: bool,
) -> tuple[dict[str, str], MarineDataStoreConfig]:
    url_mds_versions = (
        MARINE_DATASTORE_CONFIG_URL_STAGING
        if staging
        else MARINE_DATASTORE_CONFIG_URL_DIRECT
    )
    logger.debug(f"Getting required versions from {url_mds_versions}")
    mds_config: dict = {}
    try:
        with JsonParserConnection(
            timeout=2, retries=1
        ) as connection_without_retries:
            mds_config = connection_without_retries.get_json_file(
                url_mds_versions,
            )
    except Exception as e:
        if staging:
            raise e
        else:
            logger.debug(
                f"Failed to get the configuration file from {url_mds_versions}. "
            )
            with JsonParserConnection() as connection:
                mds_config = connection.get_json_file(
                    MARINE_DATASTORE_CONFIG_URL_CDN,
                )
    if not mds_config:
        raise ValueError(
            "Please check your internet connection. "
            "Also, that you have whitelisted the domains indicated in "
            "https://toolbox-docs.marine.copernicus.eu/"
        )

    return (
        mds_config["clientVersions"],
        MarineDataStoreConfig(
            catalogues=[
                CatalogueConfig(
                    stac_catalogue_url=catalogue["stac"],
                    dataset_product_mapping_url=catalogue["idMapping"],
                    root_metadata_url=(
                        root_metadata_url[:-1]
                        if (
                            root_metadata_url := catalogue["stacRoot"]
                        ).endswith("/")
                        else root_metadata_url
                    ),
                )
                for catalogue in mds_config["catalogues"]
            ],
            staging=staging,
        ),
    )
