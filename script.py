import copernicusmarine

dataset_insitu = "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr"
# subset, save locally
response = copernicusmarine.subset(
    dataset_id=dataset_insitu,
    minimum_latitude=45,
    maximum_latitude=90,
    minimum_longitude=-45,
    maximum_longitude=1,
    minimum_depth=0,
    maximum_depth=10,
    variables=["TEMP"],
    start_datetime="2023-11-25T00:00:00",
    end_datetime="2023-11-26T00:00:00",
    dataset_part="history",
    output_directory="data",
    overwrite=True,
    vertical_axis="elevation",
    # dry_run=True,
)
print("output", response.file_path)

# dataset_insitu = "cmems_obs-ins_med_phybgcwav_mynrt_na_irr"
# # subset, save locally
# response = copernicusmarine.subset(
#     dataset_id=dataset_insitu,
#     minimum_latitude=31.83,
#     maximum_latitude=34.104,
#     minimum_longitude=27.55,
#     maximum_longitude=31.69,
#     minimum_depth=0,
#     maximum_depth=25,
#     variables=["TEMP"],
#     start_datetime="2025-04-12T06:00:00",
#     end_datetime="2025-04-13T06:00:00",
#     # dataset_part="history",
#     output_directory="data",
# )
