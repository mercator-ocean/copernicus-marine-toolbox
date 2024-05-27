from typing import Optional

from copernicusmarine.catalogue_parser.catalogue_parser import (
    MARINE_DATA_STORE_STAC_BASE_URL,
)
from tests.resources.mock_stac_catalog.mock_catalog import MOCK_STAC_CATALOG
from tests.resources.mock_stac_catalog.mock_dataset_GLO_glo_phy_cur import (
    MOCK_DATASET_GLO_PHY_CUR,
)
from tests.resources.mock_stac_catalog.mock_dataset_GLO_glo_phy_cur_new_version import (
    MOCK_DATASET_GLO_PHY_CUR_NEW_VERSION,
)
from tests.resources.mock_stac_catalog.mock_dataset_GLO_glo_phy_so import (
    MOCK_DATASET_GLO_PHY_SO,
)
from tests.resources.mock_stac_catalog.mock_dataset_in_prep import (
    MOCK_DATASET_IN_PREP,
)
from tests.resources.mock_stac_catalog.mock_dataset_NWSHELF_P1D_m_202012 import (
    MOCK_DATASET_NWSHELF_P1D_M_202012,
)
from tests.resources.mock_stac_catalog.mock_dataset_NWSHELF_P1M_m_202012 import (
    MOCK_DATASET_NWSHELF_P1M_M_202012,
)
from tests.resources.mock_stac_catalog.mock_product_GLO import MOCK_PRODUCT_GLO
from tests.resources.mock_stac_catalog.mock_product_NWSHELF import (
    MOCK_PRODUCT_NWSHELF,
)

BASE_URL = MARINE_DATA_STORE_STAC_BASE_URL


def mocked_stac_aiohttp_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data: Optional[dict], status_code: int):
            self.json_data = json_data
            self.status_code = status_code

        async def json(self) -> Optional[dict]:
            return self.json_data

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    if args[0] == f"{BASE_URL}/catalog.stac.json":
        return MockResponse(MOCK_STAC_CATALOG, 200)
    elif (
        args[0]
        == f"{BASE_URL}/GLOBAL_ANALYSISFORECAST_PHY_001_024/product.stac.json"
    ):
        return MockResponse(MOCK_PRODUCT_GLO, 200)
    elif (
        args[0]
        == f"{BASE_URL}/NWSHELF_MULTIYEAR_BGC_004_011/product.stac.json"
    ):
        return MockResponse(MOCK_PRODUCT_NWSHELF, 200)
    elif (
        args[0] == f"{BASE_URL}/GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_202211/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_GLO_PHY_CUR, 200)
    elif (
        args[0] == f"{BASE_URL}/GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_206011/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_GLO_PHY_CUR_NEW_VERSION, 200)
    elif (
        args[0] == f"{BASE_URL}/GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m_202211/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_GLO_PHY_SO, 200)
    elif (
        args[0] == f"{BASE_URL}/NWSHELF_MULTIYEAR_BGC_004_011/"
        f"cmems_mod_nws_bgc-chl_my_7km-3D_P1D-m_202012/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_NWSHELF_P1D_M_202012, 200)
    elif (
        args[0] == f"{BASE_URL}/NWSHELF_MULTIYEAR_BGC_004_011/"
        f"cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_202012/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_NWSHELF_P1M_M_202012, 200)
    elif (
        args[0] == f"{BASE_URL}/GLOBAL_ANALYSISFORECAST_PHY_001_024/"
        f"cmems_obs-oc_glo_bgc-plankton_my_l3-olci-300m_P1D_202211/"
        f"dataset.stac.json"
    ):
        return MockResponse(MOCK_DATASET_IN_PREP, 200)
    return MockResponse(None, 404)
