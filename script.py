import copernicusmarine

copernicusmarine.subset(
    dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
    variables=["so"],
    minimum_longitude=-13,
    maximum_longitude=5,
    minimum_latitude=47,
    maximum_latitude=64,
    start_datetime="2025-09-24T00:00:00",
    end_datetime="2025-09-25T00:00:00",
    maximum_depth=0.49402499198913574,
    chunk_size_limit=0,
)
