from dataclasses import dataclass, field
from datetime import datetime

from copernicusmarine.core_functions.models import (
    DEFAULT_GEOSPATIAL_PROJECTION,
    DEFAULT_VERTICAL_AXIS,
    GeoSpatialProjection,
    VerticalAxis,
)


@dataclass
class YParameters:
    minimum_y: float | None = None
    maximum_y: float | None = None
    coordinate_id: str = "latitude"


@dataclass
class XParameters:
    minimum_x: float | None = None
    maximum_x: float | None = None
    coordinate_id: str = "longitude"


@dataclass
class GeographicalParameters:
    y_axis_parameters: YParameters = field(default_factory=YParameters)
    x_axis_parameters: XParameters = field(default_factory=XParameters)
    projection: GeoSpatialProjection = DEFAULT_GEOSPATIAL_PROJECTION


@dataclass
class TemporalParameters:
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    coordinate_id: str = "time"


@dataclass
class DepthParameters:
    minimum_depth: float | None = None
    maximum_depth: float | None = None
    vertical_axis: VerticalAxis = DEFAULT_VERTICAL_AXIS
    coordinate_id: str = "depth"
