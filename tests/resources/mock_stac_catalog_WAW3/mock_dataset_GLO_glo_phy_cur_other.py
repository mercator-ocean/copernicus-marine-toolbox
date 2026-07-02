MOCK_DATASET_GLO_PHY_CUR_OTHER = {
    "id": "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_other_202211",
    "type": "Feature",
    "stac_version": "1.0.0",
    "stac_extensions": [
        "https://stac-extensions.github.io/datacube/v2.1.0/schema.json"
    ],
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-180, -80],
                [-180, 90],
                [179.91668701171875, 90],
                [179.91668701171875, -80],
                [-180, -80],
            ]
        ],
    },
    "bbox": [-180, -80, 179.91668701171875, 90],
    "properties": {
        "title": "daily mean fields from Global Ocean Physics Other",
        "datetime": None,
        "start_datetime": "2020-11-01T00:00:00Z",
        "end_datetime": "2024-05-02T00:00:00Z",
        "Conventions": "CF-1.8",
        "area": "Global",
        "contact": "https://marine.copernicus.eu/contact",
        "credit": "E.U. Copernicus Marine Service Information (CMEMS)",
        "institution": "Mercator Ocean International",
        "licence": "http://marine.copernicus.eu/services-portfolio/service-commitments-and-licence/",
        "producer": "CMEMS - Global Monitoring and Forecasting Centre",
        "references": "http://marine.copernicus.eu",
        "source": "MOI GLO12",
        "cube:dimensions": {
            "latitude": {
                "type": "spatial",
                "axis": "y",
                "extent": [-80, 90],
                "step": 0.08333333333333333,
                "reference_system": 4326,
            },
            "longitude": {
                "type": "spatial",
                "axis": "x",
                "extent": [-180, 179.91668701171875],
                "step": 0.08333333804392655,
                "reference_system": 4326,
            },
            "time": {
                "type": "temporal",
                "extent": ["2020-11-01T00:00:00Z", "2024-05-02T00:00:00Z"],
                "step": "P1D",
            },
        },
        "cube:variables": {
            "uo": {
                "id": "uo",
                "dimensions": ["time", "latitude", "longitude"],
                "type": "data",
                "unit": "m/s",
                "standardName": "eastward_sea_water_velocity",
                "abbreviation": "uo",
                "name": {"en": "Eastward velocity"},
            }
        },
        "admp_in_preparation": False,
        "admp_updated": "2024-04-23T04:23:47.324874Z",
        "admp_released_date": "2022-11-01T00:00:00Z",
        "admp_retired_date": None,
    },
    "links": [
        {
            "rel": "root",
            "href": "../../catalog.stac.json",
            "title": "Copernicus Marine Data Store",
            "type": "application/json",
        },
        {
            "rel": "parent",
            "href": "../product.stac.json",
            "title": "GLOBAL_ANALYSISFORECAST_PHY_001_024_OTHER",
            "type": "application/json",
        },
        {
            "rel": "collection",
            "href": "../product.stac.json",
            "title": "GLOBAL_ANALYSISFORECAST_PHY_001_024_OTHER",
            "type": "application/json",
        },
    ],
    "assets": {
        "native": {
            "id": "native",
            "href": "https://s3.waw3-1.cloudferro.com/mdl-native-14/native/GLOBAL_ANALYSISFORECAST_PHY_001_024_OTHER/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_other_202211",
            "type": "application/x-netcdf",
            "roles": ["data"],
            "title": "Native dataset",
            "description": "The original, non-ARCO version of this dataset.",
        },
        "timeChunked": {
            "id": "timeChunked",
            "href": "https://s3.waw3-1.cloudferro.com/mdl-arco-time-010/arco/GLOBAL_ANALYSISFORECAST_PHY_001_024_OTHER/cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m_other_202211/timeChunked.zarr",
            "type": "application/vnd+zarr",
            "roles": ["data"],
            "title": "Time-chunked dataset in Zarr",
            "description": "An ARCO version of this dataset.",
            "xarray:open_kwargs": {"consolidated": True},
            "viewDims": {
                "latitude": {
                    "chunkLen": {"uo": 128},
                    "len": 2041,
                    "units": "degrees_north",
                },
                "longitude": {
                    "chunkLen": {"uo": 128},
                    "len": 4320,
                    "units": "degrees_east",
                },
                "time": {
                    "chunkLen": {"uo": 1},
                    "len": 1279,
                    "units": "milliseconds since 1970-01-01",
                },
            },
        },
    },
}
