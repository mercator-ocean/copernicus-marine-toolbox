MOCK_PRODUCT_GLO_OTHER = {
    "id": "GLOBAL_ANALYSISFORECAST_PHY_001_024_OTHER",
    "type": "Collection",
    "stac_version": "1.0.0",
    "stac_extensions": [
        "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"
    ],
    "title": "Global Ocean Physics Analysis and Forecast Other",
    "description": "A different product whose ID contains GLOBAL_ANALYSISFORECAST_PHY_001_024.",
    "license": "proprietary",
    "providers": [
        {"name": "Mercator Océan International", "roles": ["producer"]},
        {
            "name": "Copernicus Marine Service",
            "roles": ["host", "processor"],
            "url": "https://marine.copernicus.eu",
        },
    ],
    "keywords": ["numerical-model", "global-ocean"],
    "links": [
        {
            "rel": "root",
            "href": "../catalog.stac.json",
            "title": "Copernicus Marine Data Store",
            "type": "application/json",
        },
        {
            "rel": "parent",
            "href": "../catalog.stac.json",
            "title": "Copernicus Marine Data Store",
            "type": "application/json",
        },
        {
            "rel": "item",
            "href": "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_other_202211/dataset.stac.json",
            "title": "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_other_202211",
            "type": "application/json",
        },
    ],
    "extent": {
        "spatial": {"bbox": [[-180, -80, 179.9169921875, 90]]},
        "temporal": {
            "interval": [["2020-11-01T00:00:00Z", "2024-05-02T23:00:00Z"]]
        },
    },
    "assets": {
        "thumbnail": {
            "href": "https://catalogue.marine.copernicus.eu/documents/IMG/GLOBAL-ANALYSIS-FORECAST-PHYS-001-024-OTHER.gif",
            "type": "image/gif",
            "roles": ["thumbnail"],
            "title": "Global Ocean Physics Analysis and Forecast Other thumbnail",
        }
    },
    "properties": {
        "sources": ["Numerical models"],
        "processingLevel": "Level 4",
    },
    "sci:doi": "10.48670/moi-00016-other",
}
