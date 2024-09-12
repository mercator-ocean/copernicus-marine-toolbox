import asyncio
import os
import time
from functools import partial
from pathlib import Path
from typing import Optional

import pendulum
import pytest
from netCDF4 import Dataset
from syrupy.extensions.json import JSONSnapshotExtension

import copernicusmarine
from copernicusmarine.core_functions.utils import rolling_batch_gather
from tests.test_utils import execute_in_terminal


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
                            maximum_latitude = None
                            minimum_longitude = None
                            maximum_longitude = None
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
                                        maximum_latitude = minimum_latitude
                                    if coordinate_id == "longitude":
                                        minimum_longitude = (
                                            extract_minimum_value(coordinate)
                                        )
                                        maximum_longitude = minimum_longitude
                            for arco_service in [
                                "arco-geo-series",
                                # "arco-time-series",
                            ]:
                                partial_open_dataset_and_snapshot_ncdump = (
                                    partial(
                                        open_dataset_and_snapshot_ncdump,
                                        dataset_id=dataset_id,
                                        variables=[
                                            variable["short_name"]
                                            for variable in service[
                                                "variables"
                                            ]
                                        ],
                                        output_filename=output_filename,
                                        start_datetime=start_datetime,
                                        end_datetime=end_datetime,
                                        minimum_depth=minimum_depth,
                                        maximum_depth=maximum_depth,
                                        minimum_latitude=minimum_latitude,
                                        maximum_latitude=maximum_latitude,
                                        minimum_longitude=minimum_longitude,
                                        maximum_longitude=maximum_longitude,
                                        snapshot=snapshot,
                                        tmp_path=tmp_path,
                                        service=arco_service,
                                    )
                                )
                                all_tasks.append(
                                    partial_open_dataset_and_snapshot_ncdump
                                )
        print(len(all_tasks))
        top = time.time()
        # for task in all_tasks[:4]:
        #     task()
        # with ThreadPoolExecutor(max_workers=20) as executor:
        #     executor.map(lambda x: x(), all_tasks)
        all_futures = [run_in_executor(task) for task in all_tasks]
        asyncio.run(rolling_batch_gather(all_futures, per_batch=10))
        print(f"took: {time.time() - top} s")


async def run_in_executor(func):
    return await asyncio.get_event_loop().run_in_executor(None, func)


def extract_minimum_value(coordinate):
    if coordinate["values"]:
        return coordinate["values"][0]
    return coordinate["minimum_value"]


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
    message = ""
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
        command = [
            "ncdump",
            "-h",
            str(tmp_path / output_filename),
        ]
        ncdump_output = execute_in_terminal(command)
        # ncdump_output = ncdump_custom(tmp_path / output_filename)
        assert ncdump_output.stdout.decode("utf-8") == snapshot(
            name=f"{dataset_id}_{service}"
        )
        os.remove(tmp_path / output_filename)
