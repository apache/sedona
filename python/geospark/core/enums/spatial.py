from enum import Enum


class SpatialType(Enum):
    LINESTRING = "LINESTRING"
    POLYGON = "POLYGON"
    RECTANGLE = "RECTANGLE"
    POINT = "POINT"
    SPATIAL = "SPATIAL"
    CIRCLE = "CIRCLE"

    @classmethod
    def from_str(cls, spatial: str) -> 'SpatialType':
        try:
            spatial = getattr(cls, spatial.upper())
        except AttributeError:
            raise AttributeError(f"{cls.__class__.__name__} has no {spatial} attribute")
        return spatial
