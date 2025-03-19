from typing import Optional

from copernicusmarine.catalogue_parser.catalogue_parser import (
    MARINE_DATA_STORE_STAC_URL,
)
from copernicusmarine.core_functions.credentials_utils import (
    COPERNICUS_MARINE_AUTH_SYSTEM_USERINFO_ENDPOINT,
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
from tests.resources.mock_stac_catalog.mock_dataset_product_id_mapping import (
    MOCK_DATASET_PRODUCT_ID_MAPPING,
)
from tests.resources.mock_stac_catalog.mock_mds_version import MOCK_MDS_VERSION
from tests.resources.mock_stac_catalog.mock_product_GLO import MOCK_PRODUCT_GLO
from tests.resources.mock_stac_catalog.mock_product_NWSHELF import (
    MOCK_PRODUCT_NWSHELF,
)

BASE_URL = MARINE_DATA_STORE_STAC_URL


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

    print(args[0])

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
    elif (
        args[0]
        == "https://s3.waw3-1.cloudferro.com/mdl-metadata/mdsVersions.json"
    ):
        return MockResponse(MOCK_MDS_VERSION, 200)

    elif args[0] == f"{COPERNICUS_MARINE_AUTH_SYSTEM_USERINFO_ENDPOINT}":
        return MockResponse(None, 200)

    elif (
        args[0]
        == "https://s3.waw3-1.cloudferro.com/mdl-metadata/dataset_product_id_mapping.json"
    ):
        return MockResponse(MOCK_DATASET_PRODUCT_ID_MAPPING, 200)
    return MockResponse(None, 404)
