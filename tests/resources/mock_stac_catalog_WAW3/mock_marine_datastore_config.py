MARINE_DATASTORE_CONFIG = {
    "systemVersions": {
        "mds": "1.0.0",
        "mds/serverlessNative": "1.0.0",
        "mds/serverlessArco": "1.0.0",
        "mds/serverlessArco/meta": "1.0.0",
        "mds/serverlessArco/data": "1.0.0",
        "mds/serverlessArco/dense": "1.0.0",
        "mds/serverlessArco/sparse": "1.0.0",
        "mds/serverlessArco/static": "1.0.0",
        "mds/serverlessArco/omis": "1.0.0",
        "mds/wmts": "1.0.0",
        "mds/opendap": "1.0.0",
    },
    "clientVersions": {
        "mds": ">=1.2.2",
        "mds/serverlessNative": ">=1.2.2",
        "mds/serverlessArco": ">=1.2.2",
        "mds/serverlessArco/meta": ">=1.2.2",
    },
    "catalogues": [
        {
            "description": "Primary Copernicus Marine Service catalogue",
            "stac": "https://s3.waw4-1.cloudferro.com/mdl-metadata/metadata/catalog.stac.json",
            "stacRoot": "https://s3.waw4-1.cloudferro.com/mdl-metadata/metadata/",
            "idMapping": "https://s3.waw4-1.cloudferro.com/mdl-metadata/dataset_product_id_mapping.json",
        },
        {
            "description": "Secondary Copernicus Marine Service catalogue",
            "stac": "https://s3.waw3-1.cloudferro.com/mdl-metadata/metadata/catalog.stac.json",
            "stacRoot": "https://s3.waw3-1.cloudferro.com/mdl-metadata/metadata/",
            "idMapping": "https://s3.waw3-1.cloudferro.com/mdl-metadata/dataset_product_id_mapping.json",
        },
    ],
}
