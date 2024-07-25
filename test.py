import copernicusmarine

dataset = copernicusmarine.open_dataset(
    dataset_id="cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
    maximum_longitude=190,
    minimum_longitude=-150,
)

print(dataset)
