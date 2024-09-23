from dataclasses import dataclass, field
from typing import Optional

from pendulum import DateTime

from copernicusmarine.core_functions.models import (
    DEFAULT_VERTICAL_DIMENSION_OUTPUT,
    VerticalDimensionOutput,
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
    start_datetime: Optional[DateTime] = None
    end_datetime: Optional[DateTime] = None


@dataclass
class DepthParameters:
    minimum_depth: Optional[float] = None
    maximum_depth: Optional[float] = None
    vertical_dimension_output: VerticalDimensionOutput = (
        DEFAULT_VERTICAL_DIMENSION_OUTPUT
    )
