from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from copernicusmarine.core_functions.models import (
    DEFAULT_GEOSPATIAL_PROJECTION,
    DEFAULT_VERTICAL_AXIS,
    GeoSpatialProjection,
    VerticalAxis,
)


@dataclass
class LatitudeParameters:
    minimum_latitude: Optional[float] = None
    maximum_latitude: Optional[float] = None
    name: str = "latitude"
    axis: int = 0


@dataclass
class LongitudeParameters:
    minimum_longitude: Optional[float] = None
    maximum_longitude: Optional[float] = None
    name: str = "longitude"
    axis: int = 1


@dataclass
class GeographicalParameters:
    latitude_parameters: LatitudeParameters = field(
        default_factory=LatitudeParameters
    )
    longitude_parameters: LongitudeParameters = field(
        default_factory=LongitudeParameters
    )
    projection: GeoSpatialProjection = DEFAULT_GEOSPATIAL_PROJECTION


@dataclass
class TemporalParameters:
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None


@dataclass
class DepthParameters:
    minimum_depth: Optional[float] = None
    maximum_depth: Optional[float] = None
    vertical_axis: VerticalAxis = DEFAULT_VERTICAL_AXIS
