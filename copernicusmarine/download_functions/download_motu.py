import pathlib
import subprocess

import click

from copernicusmarine.catalogue_parser.catalogue_parser import (
    CopernicusMarineCatalogue,
    get_product_from_url,
)
from copernicusmarine.catalogue_parser.request_structure import SubsetRequest
from copernicusmarine.core_functions.utils import (
    FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE,
    get_unique_filename,
)


def parse_motu_dataset_url(data_path: str) -> str:
    host = data_path.split("/motu-web/Motu")[0] + "/motu-web/Motu"
    return host


def download_motu(
    username: str,
    password: str,
    subset_request: SubsetRequest,
    catalogue: CopernicusMarineCatalogue,
):
    dataset_url = subset_request.dataset_url
    if not dataset_url:
        raise TypeError(
            "Variable 'dataset_url' should not be empty in function 'download_motu()'"
        )
    product = get_product_from_url(catalogue, dataset_url)
    product_id = product.product_id
    if not subset_request.dataset_id:
        dataset_id = product.datasets[0].dataset_id
    else:
        dataset_id = subset_request.dataset_id
    output_filename = subset_request.output_filename or "output-motu.nc"
    if not subset_request.output_directory:
        output_directory = pathlib.Path(".")
    else:
        output_directory = subset_request.output_directory

    if not subset_request.force_download:
        click.confirm(
            FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE, default=True, abort=True
        )

    output_filepath = output_directory / output_filename
    output_filepath = get_unique_filename(
        filepath=output_filepath,
        overwrite_option=subset_request.overwrite_output_data,
    )

    if not output_filepath.parent.is_dir():
        pathlib.Path.mkdir(output_directory, parents=True)

    options_list = [
        "--motu",
        parse_motu_dataset_url(str(subset_request.dataset_url)),
        "--service-id",
        product_id + "-TDS",
        "--product-id",
        dataset_id,
        "--out-dir",
        str(output_filepath.parent),
        "--out-name",
        output_filepath.name,
        "--user",
        username,
        "--pwd",
        password,
    ]

    if subset_request.minimum_longitude is not None:
        options_list.extend(
            [
                "--longitude-min",
                str(subset_request.minimum_longitude),
            ]
        )
    if subset_request.maximum_longitude is not None:
        options_list.extend(
            [
                "--longitude-max",
                str(subset_request.maximum_longitude),
            ]
        )
    if subset_request.minimum_latitude is not None:
        options_list.extend(
            [
                "--latitude-min",
                str(subset_request.minimum_latitude),
            ]
        )
    if subset_request.maximum_latitude is not None:
        options_list.extend(
            [
                "--latitude-max",
                str(subset_request.maximum_latitude),
            ]
        )
    if subset_request.minimum_depth is not None:
        options_list.extend(
            [
                "--depth-min",
                str(subset_request.minimum_depth),
            ]
        )
    if subset_request.maximum_depth is not None:
        options_list.extend(
            [
                "--depth-max",
                str(subset_request.maximum_depth),
            ]
        )
    if subset_request.start_datetime:
        options_list.extend(
            [
                "--date-min",
                str(subset_request.start_datetime),
            ]
        )
    if subset_request.end_datetime:
        options_list.extend(
            [
                "--date-max",
                str(subset_request.end_datetime),
            ]
        )

    if subset_request.variables:
        options_list.extend(
            [
                flat
                for var in subset_request.variables
                for flat in ["--variable", var]
            ]
        )

    subprocess.run(
        [
            "motuclient",
        ]
        + options_list
    )
    return output_filepath
