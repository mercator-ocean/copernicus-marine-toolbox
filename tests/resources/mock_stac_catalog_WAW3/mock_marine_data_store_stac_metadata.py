from typing import Optional

from copernicusmarine.core_functions.credentials_utils import (
    COPERNICUS_MARINE_AUTH_SYSTEM_USERINFO_ENDPOINT,
)
from copernicusmarine.core_functions.marine_datastore_config import (
    MARINE_DATASTORE_CONFIG_URL_DIRECT,
)
from tests.resources.mock_stac_catalog_WAW3.mock_catalog import (
    MOCK_STAC_CATALOG as MOCK_STAC_CATALOG_WAW3,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_GLO_glo_phy_cur import (
    MOCK_DATASET_GLO_PHY_CUR,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_GLO_glo_phy_cur_new_version import (
    MOCK_DATASET_GLO_PHY_CUR_NEW_VERSION,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_GLO_glo_phy_so import (
    MOCK_DATASET_GLO_PHY_SO,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_in_prep import (
    MOCK_DATASET_IN_PREP,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_NWSHELF_P1D_m_202012 import (
    MOCK_DATASET_NWSHELF_P1D_M_202012,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_NWSHELF_P1M_m_202012 import (
    MOCK_DATASET_NWSHELF_P1M_M_202012,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_oriol import (
    MOCK_DATASET_ORIOL,
)
from tests.resources.mock_stac_catalog_WAW3.mock_dataset_product_id_mapping import (
    MOCK_DATASET_PRODUCT_ID_MAPPING,
)
from tests.resources.mock_stac_catalog_WAW3.mock_marine_datastore_config import (
    MARINE_DATASTORE_CONFIG,
)
from tests.resources.mock_stac_catalog_WAW3.mock_product_GLO import (
    MOCK_PRODUCT_GLO,
)
from tests.resources.mock_stac_catalog_WAW3.mock_product_NWSHELF import (
    MOCK_PRODUCT_NWSHELF,
)
from tests.resources.mock_stac_catalog_WAW4.mock_catalog import (
    MOCK_CATALOG as MOCK_CATALOG_WAW4,
)

BASE_URLS = [
    catalogue["stacRoot"]  # type: ignore
    for catalogue in MARINE_DATASTORE_CONFIG["catalogues"]
]
BASE_URL_WAW3 = BASE_URLS[1]
DATASET_PRODUCT_MAPPING_URLS = [
    catalogue["idMapping"]  # type: ignore
    for catalogue in MARINE_DATASTORE_CONFIG["catalogues"]
]
CATALOG_URLS = [
    catalogue["stac"]  # type: ignore
    for catalogue in MARINE_DATASTORE_CONFIG["catalogues"]
]


def mocked_stac_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data: Optional[dict], status_code: int):
            self.json_data = json_data
            self.status_code = status_code

        def json(self) -> Optional[dict]:
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    print(args)

    if args[0] == CATALOG_URLS[1]:
        return MockResponse(MOCK_STAC_CATALOG_WAW3, 200)
    elif args[0] == CATALOG_URLS[0]:
        return MockResponse(MOCK_CATALOG_WAW4, 200)
    elif (
        args[0]
        == f"{BASE_URL_WAW3}GLOBAL_ANALYSISFORECAST_PHY_001_024/product.stac.json"
    ):
        return MockResponse(MOCK_PRODUCT_GLO, 200)
    elif (
        args[0]
        == f"{BASE_URL_WAW3}NWSHELF_MULTIYEAR_BGC_004_011/product.stac.json"
    ):
        return MockResponse(MOCK_PRODUCT_NWSHELF, 200)
    elif (
        args[0] == f"{BASE_URL_WAW3}GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_202211/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_GLO_PHY_CUR, 200)
    elif (
        args[0] == f"{BASE_URL_WAW3}GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_206011/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_GLO_PHY_CUR_NEW_VERSION, 200)
    elif (
        args[0] == f"{BASE_URL_WAW3}GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m_202211/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_GLO_PHY_SO, 200)
    elif (
        args[0] == f"{BASE_URL_WAW3}NWSHELF_MULTIYEAR_BGC_004_011/"
        f"cmems_mod_nws_bgc-chl_my_7km-3D_P1D-m_202012/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_NWSHELF_P1D_M_202012, 200)
    elif (
        args[0] == f"{BASE_URL_WAW3}NWSHELF_MULTIYEAR_BGC_004_011/"
        f"cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_202012/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_NWSHELF_P1M_M_202012, 200)
    elif (
        args[0] == f"{BASE_URL_WAW3}GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_obs-oc_glo_bgc-plankton_my_l3-olci-300m_P1D_202211/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_IN_PREP, 200)
    elif (
        args[0] == f"{BASE_URL_WAW3}SEALEVEL_GLO_PHY_L4_NRT_008_046/"
        f"cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.25deg_P1D_202311/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_ORIOL, 200)

    elif args[0] == f"{COPERNICUS_MARINE_AUTH_SYSTEM_USERINFO_ENDPOINT}":
        return MockResponse({"preferred_username": "copernicususer"}, 200)

    elif args[0] == DATASET_PRODUCT_MAPPING_URLS[1]:
        return MockResponse(MOCK_DATASET_PRODUCT_ID_MAPPING, 200)
    elif args[0] == DATASET_PRODUCT_MAPPING_URLS[0]:
        return MockResponse({}, 200)
    elif args[0] == MARINE_DATASTORE_CONFIG_URL_DIRECT:
        return MockResponse(MARINE_DATASTORE_CONFIG, 200)
    return MockResponse(None, 404)
