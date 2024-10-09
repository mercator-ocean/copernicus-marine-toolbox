import asyncio
import json
import logging
import os
import time
from pathlib import Path

# from random import shuffle
from typing import Optional, Union

import pendulum
import pytest
import xarray
from netCDF4 import Dataset
from syrupy.extensions.json import JSONSnapshotExtension

import copernicusmarine
from copernicusmarine.core_functions.utils import run_concurrently
from tests.test_utils import execute_in_terminal

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("copernicusmarine").setLevel(logging.CRITICAL + 1)


@pytest.fixture
def snapshot_json(snapshot):
    return snapshot.with_defaults(extension_class=JSONSnapshotExtension)


class TestDatasets:
    # def test_describe_output_open_datasets(self, snapshot, snapshot_json):
    #     self.describe = copernicusmarine.describe(
    #         include_versions=True, include_datasets=True
    #     )
    #     # test that all the datasets are described (compare with
    #     # stac maybe the mapping dataset-product)
    #     # one variable for each dataset and then compare the netcdf file

    #     snapshot_json.assert_match(self.describe)

    #     for product in self.describe["products"]:
    #         for dataset in product["datasets"]:
    #             dataset_id = dataset["dataset_id"]
    #             try:
    #                 dataset = copernicusmarine.open_dataset(dataset_id)
    #             except NoServiceAvailable:
    #                 continue
    #             snapshot.assert_match(dataset)
    #         for version in product["versions"]:
    #             version_id = version["version_id"]
    #             try:
    #                 dataset = copernicusmarine.open_dataset(version_id)
    #             except NoServiceAvailable:
    #                 continue
    #             snapshot.assert_match(dataset)

    # def test_download_variable_and_ncdump_og(self, snapshot, tmp_path):
    #     self.describe = copernicusmarine.describe(
    #         include_versions=True, include_datasets=True
    #     )
    #     count = 0
    #     for product in self.describe["products"]:
    #         for dataset in product["datasets"]:
    #             dataset_id = dataset["dataset_id"]
    #             for version in dataset["versions"]:
    #                 for part in version["parts"]:
    #                     for service in part["services"]:
    #                         if (
    #                             service["service_type"]["service_name"]
    #                             != "arco-geo-series"
    #                         ):
    #                             continue
    #                         for variable in service["variables"]:
    #                             variable_name = variable["short_name"]
    #                             output_filename = (
    #                                 f"{dataset_id}_{variable_name}.nc"
    #                             )
    #                             start_datetime = None
    #                             end_datetime = None
    #                             minimum_latitude = None
    #                             maximum_latitude = None
    #                             minimum_longitude = None
    #                             maximum_longitude = None
    #                             minimum_depth = None
    #                             maximum_depth = None
    #                             for coordinate in variable["coordinates"]:
    #                                 coordinate_id = coordinate[
    #                                     "coordinate_id"
    #                                 ]
    #                                 if coordinate_id == "time":
    #                                     start_datetime = extract_minimum_value(
    #                                         coordinate
    #                                     )
    #                                     if isinstance(start_datetime, int):
    #                                         start_datetime = (
    #                                             pendulum.from_timestamp(
    #                                                 start_datetime / 1000
    #                                             )
    #                                         ).naive()
    #                                     start_datetime =
    #                                     end_datetime = start_datetime
    #                                 if coordinate_id == "depth":
    #                                     minimum_depth = extract_minimum_value(
    #                                         coordinate
    #                                     )
    #                                     maximum_depth = minimum_depth
    #                                 if coordinate_id == "latitude":
    #                                     minimum_latitude = (
    #                                         extract_minimum_value(coordinate)
    #                                     )
    #                                     maximum_latitude = minimum_latitude
    #                                 if coordinate_id == "longitude":
    #                                     minimum_longitude = (
    #                                         extract_minimum_value(coordinate)
    #                                     )
    #                                     maximum_longitude = minimum_longitude
    #                             copernicusmarine.subset(
    #                                 dataset_id=dataset_id,
    #                                 variables=[variable_name],
    #                                 output_filename=output_filename,
    #                                 start_datetime=start_datetime,
    #                                 end_datetime=end_datetime,
    #                                 minimum_depth=minimum_depth,
    #                                 maximum_depth=maximum_depth,
    #                                 minimum_latitude=minimum_latitude,
    #                                 maximum_latitude=maximum_latitude,
    #                                 minimum_longitude=minimum_longitude,
    #                                 maximum_longitude=maximum_longitude,
    #                                 force_download=True,
    #                                 output_directory=tmp_path,
    #                             )
    #                             ncdump_output = ncdump_custom(
    #                                 tmp_path / output_filename
    #                             )
    #                             snapshot.assert_match(ncdump_output)

    def test_download_variable_and_ncdump(self, snapshot, tmp_path):
        print("Starting test")
        self.describe = copernicusmarine.describe(
            include_versions=False, include_datasets=True
        )
        all_tasks = []
        for product in self.describe["products"]:
            for dataset in product["datasets"]:
                dataset_id = dataset["dataset_id"]
                for version in dataset["versions"]:
                    for part in version["parts"]:
                        for service in part["services"]:
                            if (
                                service["service_type"]["service_name"]
                                != "arco-geo-series"
                                or "sql" in service["service_format"]
                            ):
                                continue
                            start_datetime = None
                            end_datetime = None
                            minimum_latitude = None
                            # maximum_latitude = None
                            minimum_longitude = None
                            # maximum_longitude = None
                            minimum_depth = None
                            maximum_depth = None
                            for variable in service["variables"]:
                                variable_name = variable["short_name"]
                                output_filename = (
                                    f"{dataset_id}_{variable_name}.nc"
                                )
                                for coordinate in variable["coordinates"]:
                                    coordinate_id = coordinate["coordinate_id"]
                                    if coordinate_id == "time":
                                        start_datetime = extract_minimum_value(
                                            coordinate
                                        )
                                        if isinstance(start_datetime, int):
                                            start_datetime = (
                                                (
                                                    pendulum.from_timestamp(
                                                        start_datetime / 1000
                                                    )
                                                )
                                                .naive()
                                                .isoformat()
                                            )
                                        start_datetime = start_datetime
                                        end_datetime = start_datetime
                                    if coordinate_id == "depth":
                                        minimum_depth = extract_minimum_value(
                                            coordinate
                                        )
                                        maximum_depth = minimum_depth
                                    if coordinate_id == "latitude":
                                        minimum_latitude = (
                                            extract_minimum_value(coordinate)
                                        )
                                        # maximum_latitude = (
                                        #     extract_maximum_value(coordinate)
                                        # )
                                    if coordinate_id == "longitude":
                                        minimum_longitude = (
                                            extract_minimum_value(coordinate)
                                        )
                                        # maximum_longitude = (
                                        #     extract_maximum_value(coordinate)
                                        # )
                            for arco_service in [
                                "arco-geo-series",
                                # "arco-time-series",
                            ]:
                                output_filename = (
                                    f"{dataset_id}_{arco_service}.nc"
                                )
                                # partial_open_dataset_and_snapshot_ncdump = (
                                #     partial(
                                #         open_dataset_and_snapshot_ncdump,
                                #         dataset_id=dataset_id,
                                #         variables=[
                                #             variable["short_name"]
                                #             for variable in service[
                                #                 "variables"
                                #             ]
                                #         ],
                                #         output_filename=output_filename,
                                #         start_datetime=start_datetime,
                                #         end_datetime=end_datetime,
                                #         minimum_depth=minimum_depth,
                                #         maximum_depth=maximum_depth,
                                #         minimum_latitude=minimum_latitude,
                                #         maximum_latitude=maximum_latitude,
                                #         minimum_longitude=minimum_longitude,
                                #         maximum_longitude=maximum_longitude,
                                #         snapshot=snapshot,
                                #         tmp_path=tmp_path,
                                #         service=arco_service,
                                #     )
                                # )
                                # all_tasks.append(
                                #     partial_open_dataset_and_snapshot_ncdump
                                # )
                                all_tasks.append(
                                    (
                                        dataset_id,
                                        [
                                            variable["short_name"]
                                            for variable in service[
                                                "variables"
                                            ]
                                        ],
                                        output_filename,
                                        start_datetime,
                                        end_datetime,
                                        minimum_depth,
                                        maximum_depth,
                                        minimum_latitude,
                                        minimum_latitude,
                                        minimum_longitude,
                                        minimum_longitude,
                                        arco_service,
                                        snapshot,
                                        tmp_path,
                                    )
                                )

        print(f"Found {len(all_tasks)} tasks")
        running_tasks = all_tasks  # [:200]
        # shuffle(running_tasks)
        print(f"Running tasks: {len(running_tasks)}")
        top = time.time()
        run_concurrently(
            open_dataset_and_snapshot_ncdump,
            running_tasks,
            max_concurrent_requests=8,
            tdqm_bar_configuration={"disable": False},
        )
        print(f"took: {time.time() - top} s")


async def run_in_executor(func):
    return await asyncio.get_event_loop().run_in_executor(None, func)


def extract_minimum_value(coordinate):
    if coordinate["values"]:
        return coordinate["values"][0]
    return coordinate["minimum_value"]


def extract_maximum_value(coordinate):
    if coordinate["values"]:
        return coordinate["values"][-1]
    return coordinate["maximum_value"]


def ncdump_custom(file_path: Path) -> str:
    message = ""
    dataset = Dataset(file_path, "r")
    message += str(dataset)

    for v in dataset.variables:
        message += str(dataset.variables[v])
    return message


def ncdump(nc_fid, verb=True):
    """
    ncdump outputs dimensions, variables and their attribute information.
    The information is similar to that of NCAR's ncdump utility.
    ncdump requires a valid instance of Dataset.

    Parameters
    ----------
    nc_fid : netCDF4.Dataset
        A netCDF4 dateset object
    verb : Boolean
        whether or not nc_attrs, nc_dims, and nc_vars are printed

    Returns
    -------
    nc_attrs : list
        A Python list of the NetCDF file global attributes
    nc_dims : list
        A Python list of the NetCDF file dimensions
    nc_vars : list
        A Python list of the NetCDF file variables
    """

    def print_ncattr(key):
        """
        Prints the NetCDF file attributes for a given key

        Parameters
        ----------
        key : unicode
            a valid netCDF4.Dataset.variables key
        """
        try:
            print("\t\ttype:", repr(nc_fid.variables[key].dtype))
            for ncattr in nc_fid.variables[key].ncattrs():
                print(
                    "\t\t%s:" % ncattr,
                    repr(nc_fid.variables[key].getncattr(ncattr)),
                )
        except KeyError:
            print("\t\tWARNING: %s does not contain variable attributes" % key)

    # NetCDF global attributes
    nc_attrs = nc_fid.ncattrs()
    if verb:
        print("NetCDF Global Attributes:")
        for nc_attr in nc_attrs:
            print("\t%s:" % nc_attr, repr(nc_fid.getncattr(nc_attr)))
    nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
    # Dimension shape information.
    if verb:
        print("NetCDF dimension information:")
        for dim in nc_dims:
            print("\tName:", dim)
            print("\t\tsize:", len(nc_fid.dimensions[dim]))
            print_ncattr(dim)
    # Variable information.
    nc_vars = [var for var in nc_fid.variables]  # list of nc variables
    if verb:
        print("NetCDF variable information:")
        for var in nc_vars:
            if var not in nc_dims:
                print("\tName:", var)
                print("\t\tdimensions:", nc_fid.variables[var].dimensions)
                print("\t\tsize:", nc_fid.variables[var].size)
                print_ncattr(var)
    return nc_attrs, nc_dims, nc_vars


def open_dataset_and_snapshot_ncdump(
    dataset_id: str,
    variables: list[str],
    output_filename: str,
    start_datetime: Optional[str],
    end_datetime: Optional[str],
    minimum_depth: Optional[float],
    maximum_depth: Optional[float],
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    service: str,
    snapshot,
    tmp_path,
):
    print(f"Doing {dataset_id}")
    message: Union[dict, str] = ""
    try:
        copernicusmarine.subset(
            dataset_id=dataset_id,
            variables=variables,
            output_filename=output_filename,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            minimum_depth=minimum_depth,
            maximum_depth=maximum_depth,
            minimum_latitude=minimum_latitude,
            maximum_latitude=maximum_latitude,
            minimum_longitude=minimum_longitude,
            maximum_longitude=maximum_longitude,
            service=service,
            force_download=True,
            output_directory=tmp_path,
            disable_progress_bar=True,
        )
    except Exception as e:
        arguments = [
            str(start_datetime),
            str(end_datetime),
            str(minimum_depth),
            str(maximum_depth),
            str(minimum_latitude),
            str(maximum_latitude),
            str(minimum_longitude),
            str(maximum_longitude),
            str(service),
        ]
        message = (
            f"*** Error: {e} FROM the call {dataset_id}, {','.join(arguments)}"
        )
        # raise Exception(f"{dataset_id}, {','.join(arguments)}")
        # raise Exception(f"Error: {e} with {dataset_id}, {','.join(arguments)}")
    if message:
        assert message == snapshot(name=f"{dataset_id}_{service}")
    else:
        # command = [
        #     "ncdump",
        #     "-h",
        #     str(tmp_path / output_filename),
        # ]
        # ncdump_output = execute_in_terminal(command)
        # # ncdump_output = ncdump_custom(tmp_path / output_filename)
        # assert ncdump_output.stdout.decode("utf-8") == snapshot(
        #     name=f"{dataset_id}_{service}"
        # )
        try:
            message = then_it_is_cf_compliant(
                dataset_id, tmp_path, output_filename
            )
        except Exception as e:
            message = (
                f"*** Error: {e} FROM CF compliance "
                f"for the call {dataset_id}, {','.join(arguments)}"
            )
        os.remove(tmp_path / output_filename)
        assert message == snapshot(name=f"{dataset_id}_{service}")


def then_it_is_cf_compliant(dataset_id, tmp_path, output_filename) -> dict:
    dataset_id = dataset_id
    dataset = xarray.open_dataset(f"{tmp_path}/{output_filename}")
    cf_convention = dataset.attrs.get("Conventions")
    if cf_convention:
        cf_convention = cf_convention[-3:]
        if cf_convention < "1.6":
            cf_convention = "1.6"
    else:
        cf_convention = "1.6"
    command = [
        "compliance-checker",
        f"--test=cf:{cf_convention}",
        f"{tmp_path}/{output_filename}",
        "-f",
        "json",
    ]
    output = execute_in_terminal(command)

    data = json.loads(output.stdout)

    dict_result = {}
    dict_result["dataset_id"] = dataset_id
    dict_result["scored_points"] = data[f"cf:{cf_convention}"]["scored_points"]
    dict_result["possible_points"] = data[f"cf:{cf_convention}"][
        "possible_points"
    ]
    dict_result["messages"] = []
    for dictionary in sorted(
        data[f"cf:{cf_convention}"]["all_priorities"], key=lambda x: x["name"]
    ):
        if len(dictionary["msgs"]) > 0:
            dict_result["messages"].append(dictionary["name"])
            dict_result["messages"].append(sorted(dictionary["msgs"]))
    return dict_result
