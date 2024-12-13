from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from copernicusmarine.core_functions.models import (
    DEFAULT_VERTICAL_AXIS,
    VerticalAxis,
)


@dataclass
class LatitudeParameters:
    minimum_latitude: Optional[float] = None
    maximum_latitude: Optional[float] = None


@dataclass
class LongitudeParameters:
    minimum_longitude: Optional[float] = None
    maximum_longitude: Optional[float] = None


@dataclass
class GeographicalParameters:
    latitude_parameters: LatitudeParameters = field(
        default_factory=LatitudeParameters
    )
    longitude_parameters: LongitudeParameters = field(
        default_factory=LongitudeParameters
    )


@dataclass
class TemporalParameters:
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None


@dataclass
class DepthParameters:
    minimum_depth: Optional[float] = None
    maximum_depth: Optional[float] = None
    vertical_axis: VerticalAxis = DEFAULT_VERTICAL_AXIS
