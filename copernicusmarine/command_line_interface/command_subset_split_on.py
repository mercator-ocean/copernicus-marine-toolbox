import json
import logging
from typing import Optional, Union

import click
from click import Context

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
)
from copernicusmarine.core_functions.documentation_utils import SUBSET_SPLIT_ON
from copernicusmarine.core_functions.fields_query_builder import (
    build_query,
    get_queryable_requested_fields,
)
from copernicusmarine.core_functions.models import (
    ResponseSubset,
    SplitOnTimeOption,
)
from copernicusmarine.core_functions.subset_split_on import (
    subset_split_on_function,
)

logger = logging.getLogger("copernicusmarine")
blank_logger = logging.getLogger("copernicusmarine_blank_logger")

DEFAULT_FIELDS_TO_INCLUDE = {
    "status",
    "message",
    "file_size",
    "data_transfer_size",
    "filename",
}


@click.command(
    cls=CustomClickOptionsCommand,
    help=SUBSET_SPLIT_ON["SPLIT_ON_DESCRIPTION_HELP"],
    short_help=SUBSET_SPLIT_ON["SPLIT_ON_SHORT_HELP"],
)
@click.option(
    "--on-variables",
    is_flag=True,
    default=False,
    help=SUBSET_SPLIT_ON["ON_VARIABLES_HELP"],
)
@click.option(
    "--on-time",
    type=click.Choice(["hour", "day", "month", "year"]),
    default=None,
    help=SUBSET_SPLIT_ON["ON_TIME_HELP"],
)
@click.option(
    "--concurrent-processes",
    type=click.IntRange(1, None),
    default=None,
    help=SUBSET_SPLIT_ON["CONCURRENT_PROCESSES_HELP"],
)
@click.pass_context
@log_exception_and_exit
def split_on(
    context: Context,
    on_variables: bool,
    on_time: Optional[SplitOnTimeOption],
    concurrent_processes: Optional[int],
):
    subset_request = context.obj.get("subset_request")
    responses = subset_split_on_function(
        on_variables=on_variables,
        on_time=on_time,
        subset_request=subset_request,
        concurrent_processes=concurrent_processes,
    )

    response_fields: Optional[str] = context.obj.get("response_fields")
    dry_run: bool = subset_request.dry_run
    if response_fields:
        fields_to_include = set(response_fields.replace(" ", "").split(","))
    elif dry_run:
        fields_to_include = {"all"}
    else:
        fields_to_include = DEFAULT_FIELDS_TO_INCLUDE

    included_fields: Optional[Union[dict, set]]
    if "all" in fields_to_include:
        included_fields = None
    elif "none" in fields_to_include:
        included_fields = set()
    else:
        queryable_fields = get_queryable_requested_fields(
            fields_to_include, ResponseSubset, "subset --response-fields"
        )
        included_fields = build_query(set(queryable_fields), ResponseSubset)

    blank_logger.info(
        json.dumps(
            [
                json.loads(
                    response.model_dump_json(
                        include=included_fields,
                        exclude_none=True,
                        exclude_unset=True,
                    )
                )
                for response in responses
            ],
            indent=2,
        )
    )
