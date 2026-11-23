MOCK_CATALOG = {
    "id": "MDS",
    "type": "Catalog",
    "stac_version": "1.0.0",
    "stac_extensions": [],
    "title": "Copernicus Marine Data Store",
    "description": "Data from the Copernicus Marine Data Store, in Analysis-Ready, Cloud-Optimised (ARCO) format.\n",
    "links": [
        {
            "rel": "self",
            "href": "https://stac.marine.copernicus.eu/metadata/catalog.stac.json",
            "title": "Copernicus Marine Data Store",
            "type": "application/json",
        },
        {
            "rel": "root",
            "href": ".",
            "title": "Copernicus Marine Data Store",
            "type": "application/json",
        },
    ],
}

# Used to test the behaviour with several catalogues:
# the product GLOBAL_ANALYSISFORECAST_PHY_001_024 is also in the WAW3 catalogue
MOCK_CATALOG_WITH_PRODUCTS = {
    "id": "MDS",
    "type": "Catalog",
    "stac_version": "1.0.0",
    "stac_extensions": [],
    "title": "Copernicus Marine Data Store",
    "description": "Data from the Copernicus Marine Data Store, in Analysis-Ready, Cloud-Optimised (ARCO) format.\n",
    "links": [
        {
            "rel": "self",
            "href": "https://stac.marine.copernicus.eu/metadata/catalog.stac.json",
            "title": "Copernicus Marine Data Store",
            "type": "application/json",
        },
        {
            "rel": "root",
            "href": ".",
            "title": "Copernicus Marine Data Store",
            "type": "application/json",
        },
        {
            "rel": "child",
            "href": "GLOBAL_ANALYSISFORECAST_PHY_001_024/product.stac.json",
            "title": "GLOBAL_ANALYSISFORECAST_PHY_001_024",
            "type": "application/json",
        },
    ],
}
