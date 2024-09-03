from datetime import datetime
from typing import Optional, Union

import pendulum
from pendulum import DateTime

from copernicusmarine.core_functions.utils import datetime_parser


def homogenize_datetime(
    input_datetime: Optional[Union[datetime, str]]
) -> Optional[DateTime]:
    if input_datetime is None:
        return None
    if isinstance(input_datetime, str):
        return datetime_parser(input_datetime)
    return pendulum.instance(input_datetime)
