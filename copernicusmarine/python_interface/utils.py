from datetime import datetime
from typing import Optional, Union

import pandas as pd
from dateutil.tz import UTC

from copernicusmarine.core_functions.utils import datetime_parser


def homogenize_datetime(
    input_datetime: Optional[Union[datetime, pd.Timestamp, str]]
) -> Optional[datetime]:
    if input_datetime is None:
        return None
    if isinstance(input_datetime, str):
        return datetime_parser(input_datetime)
    if isinstance(input_datetime, pd.Timestamp):
        return input_datetime.to_pydatetime().astimezone(UTC)
    return input_datetime.astimezone(UTC)
