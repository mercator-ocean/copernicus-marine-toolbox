import json
import logging
import os
import re
import time
from pathlib import Path
from random import shuffle
from typing import Literal, Optional

import pendulum
import xarray
from pydantic import BaseModel

import copernicusmarine
from copernicusmarine import (
    CopernicusMarineCoordinate,
    CopernicusMarineService,
)
from copernicusmarine.core_functions.utils import run_concurrently
from tests.test_utils import execute_in_terminal


class SubsetArguments(BaseModel):
    dataset_id: str
    version: str
    part: str
    variables: list[str]
    output_filename: str
    start_datetime: Optional[str]
    end_datetime: Optional[str]
    minimum_depth: Optional[float]
    maximum_depth: Optional[float]
    minimum_latitude: Optional[float]
    maximum_latitude: Optional[float]
    minimum_longitude: Optional[float]
    maximum_longitude: Optional[float]
    service: str
    keep_going: bool


def test_download_variable_and_test_compliance(
    number_of_datasets: Optional[int] = None,
    dataset_id: Optional[str] = None,
    product_id: Optional[str] = None,
    service_to_test: Literal[
        "arco-time-series", "arco-geo-series"
    ] = "arco-time-series",
    keep_going: bool = True,
    concurrent_requests: int = 8,
    log_file: str = "data_to_delete/some_file.txt",
):
    """
    Script to download the minimum on each dimension of the dataset and then
    run cf compliance on the file.

    input:
    - number_of_datasets: int, number of datasets to run the test on
    - dataset_id: str, dataset_id to run the test on
    - product_id: str, product_id to run the test on
    - keep_going: bool, if True, the script will continue even if there is an error. Usually used for debugging
    - concurrent_requests: int, number of concurrent requests to run

    Improvements:
    - Operationalise
    - Automate
    - Choose a more representative zone
    """  # noqa
    print("Starting test")
    describe = copernicusmarine.describe(
        show_all_versions=False,
        dataset_id=dataset_id,
        product_id=product_id,
    )
    all_tasks = []
    for product in describe.products:
        for dataset in product.datasets:
            dataset_id = dataset.dataset_id
            for version in dataset.versions:
                for part in version.parts:
                    for service in part.services:
                        # TODO: test on more services
                        if service.service_name != service_to_test or (
                            service.service_format
                            and "sql" in service.service_format
                        ):
                            continue
                        subset_arguments = SubsetArguments(
                            dataset_id=dataset_id,
                            version=version.label,
                            part=part.name,
                            variables=[
                                variable.short_name
                                for variable in service.variables
                            ],
                            output_filename=f"{dataset_id}___{part.name}___{service.service_name}.nc",  # noqa
                            start_datetime=None,
                            end_datetime=None,
                            minimum_depth=None,
                            maximum_depth=None,
                            minimum_latitude=None,
                            maximum_latitude=None,
                            minimum_longitude=None,
                            maximum_longitude=None,
                            service=service.service_name,
                            keep_going=keep_going,
                        )
                        subset_arguments_updated = _get_subset_arguments(
                            subset_arguments=subset_arguments,
                            service=service,
                        )
                        if subset_arguments_updated:
                            all_tasks.append(
                                tuple(subset_arguments.model_dump().values())
                            )

    print(f"Found {len(all_tasks)} tasks")
    if number_of_datasets:
        running_tasks = all_tasks[:number_of_datasets]
    else:
        running_tasks = all_tasks  # [:200]
    shuffle(running_tasks)
    print(f"Running tasks: {len(running_tasks)}")
    top = time.time()
    results = run_concurrently(
        open_dataset_and_snapshot_ncdump,
        running_tasks,
        max_concurrent_requests=concurrent_requests,
        tdqm_bar_configuration={"disable": False},
    )
    total_transfered_data = 0.0
    total_size_file = 0.0
    message_all = ""
    for message, transfered_data, size_file in results:
        total_transfered_data += transfered_data or 0
        total_size_file += size_file or 0
        message_all += f"{message} \n"
    with open(log_file, "wb") as f:
        f.write(message_all.encode("utf-8"))
    print(f"Took: {time.time() - top} s")
    print(f"Total transfered data: {total_transfered_data:.2f} MB")
    print(f"Total size files: {total_size_file:.2f} MB")


def _get_subset_arguments(
    subset_arguments: SubsetArguments,
    service: CopernicusMarineService,
) -> Optional[SubsetArguments]:
    for variable in service.variables:
        for coordinate in variable.coordinates:
            coordinate_id = coordinate.coordinate_id
            if coordinate_id == "x" or coordinate_id == "y":
                return None
            if coordinate_id == "time":
                start_datetime = extract_minimum_value(coordinate)
                if isinstance(start_datetime, int) or isinstance(
                    start_datetime, float
                ):
                    start_datetime = (
                        (pendulum.from_timestamp(start_datetime / 1000))
                        .naive()
                        .isoformat()
                    )
                subset_arguments.start_datetime = start_datetime
                subset_arguments.end_datetime = start_datetime
            if coordinate_id == "depth":
                minimum_depth = extract_minimum_value(coordinate)
                subset_arguments.minimum_depth = minimum_depth  # type: ignore
                subset_arguments.maximum_depth = minimum_depth  # type: ignore
            if coordinate_id == "latitude":
                minimum_latitude = extract_minimum_value(coordinate)
                subset_arguments.minimum_latitude = minimum_latitude  # type: ignore
                subset_arguments.maximum_latitude = minimum_latitude  # type: ignore
            if coordinate_id == "longitude":
                minimum_longitude = extract_minimum_value(coordinate)
                subset_arguments.minimum_longitude = minimum_longitude  # type: ignore
                subset_arguments.maximum_longitude = minimum_longitude  # type: ignore
            if (
                subset_arguments.start_datetime is not None
                and subset_arguments.minimum_depth is not None
                and subset_arguments.minimum_latitude is not None
                and subset_arguments.minimum_longitude is not None
            ):
                return subset_arguments
    return subset_arguments


def extract_minimum_value(coordinate: CopernicusMarineCoordinate):
    if coordinate.values:
        return coordinate.values[0]
    return coordinate.minimum_value


def extract_maximum_value(coordinate: CopernicusMarineCoordinate):
    if coordinate.values:
        return coordinate.values[-1]
    return coordinate.maximum_value


def open_dataset_and_snapshot_ncdump(
    dataset_id: str,
    version: str,
    part: str,
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
    keep_going: bool,
    tmp_path: Path = Path("data_to_delete"),
) -> tuple[str, Optional[float], Optional[float]]:
    message: str = "------------------------------------------------------------------- \n"  # noqa
    message += f"{dataset_id} \n"
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
    cli_command = [
        "subset",
        "--dataset-id",
        dataset_id,
        "--service",
        service,
    ]
    if start_datetime:
        cli_command += ["-t", start_datetime]
    if end_datetime:
        cli_command += ["-T", end_datetime]
    if minimum_latitude:
        cli_command += ["-y", str(minimum_latitude)]
    if maximum_latitude:
        cli_command += ["-Y", str(maximum_latitude)]
    if minimum_longitude:
        cli_command += ["-x", str(minimum_longitude)]
    if maximum_longitude:
        cli_command += ["-X", str(maximum_longitude)]
    if minimum_depth:
        cli_command += ["-z", str(minimum_depth)]
    if maximum_depth:
        cli_command += ["-Z", str(maximum_depth)]
    if part:
        cli_command += ["--dataset-part", part]
    if version:
        cli_command += ["--dataset-version", version]

    try:
        response = copernicusmarine.subset(
            dataset_id=dataset_id,
            dataset_version=version,
            dataset_part=part,
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
            output_directory=tmp_path,
            disable_progress_bar=True,
            overwrite=True,
        )
    except Exception as e:
        if not keep_going:
            raise e
        message += f"*** Error: {e} FROM the call {dataset_id}, {','.join(arguments)} \n"  # noqa
        command_to_reproduce = f"copernicusmarine {' '.join(cli_command)}"
        message += f"Command to reproduce: {command_to_reproduce}"
        return message + "\n", 0, 0
    try:
        message += json.dumps(
            then_it_is_cf_compliant(dataset_id, tmp_path, output_filename)
        )
    except Exception as e:
        if not keep_going:
            raise e
        message += f"*** Error: {e} FROM CF compliance "
        command_to_reproduce = f"copernicusmarine {' '.join(cli_command)}"
        message += f"Command to reproduce: {command_to_reproduce}"
        os.remove(tmp_path / output_filename)
        return message + "\n", 0, 0
    os.remove(tmp_path / output_filename)
    return message + "\n", response.data_transfer_size, response.file_size


def then_it_is_cf_compliant(dataset_id, tmp_path, output_filename) -> dict:
    dataset_id = dataset_id
    dataset = xarray.open_dataset(f"{tmp_path}/{output_filename}")
    conventions = dataset.attrs.get("Conventions")
    if conventions:
        pattern = r"CF-(\d+\.\d+)"
        match = re.search(pattern, conventions)
        if match:
            cf_convention = match.group(1)
        else:
            cf_convention = "1.6"
    else:
        cf_convention = "1.6"
    if cf_convention < "1.6":
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


def check_compliance_checker():
    command = ["compliance-checker", "--help"]
    output = execute_in_terminal(command)
    try:
        assert output.returncode == 0
    except AssertionError as e:
        print("Compliance checker is not installed")
        raise e


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("copernicusmarine").setLevel(logging.CRITICAL + 1)

    # TODO: check why, if the compliance checker is not installed,
    # the script does not stop and keep downloading
    # in the meantime checking if installed
    check_compliance_checker()
    # test_download_variable_and_ncdump(
    #     number_of_datasets=10,
    #     dataset_id="cmems_obs-wave_glo_phy-swh_nrt_multi-l4-2deg_P1D",
    #     # product_id="ARCTIC_ANALYSISFORECAST_PHY_002_001",
    #     keep_going=False,
    # )
    test_download_variable_and_test_compliance(keep_going=True)
