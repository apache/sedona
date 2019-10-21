from typing import List

from shapely.geometry import Point, MultiPoint, Polygon, MultiPolygon, LineString, MultiLineString
from shapely.geometry.base import BaseGeometry


def assign_all() -> bool:
    assign_udt_shapely_objects(
        geoms=[Point, MultiPoint, Polygon, MultiPolygon, LineString, MultiLineString]
    )
    return True


def assign_udt_shapely_objects(geoms: List[type(BaseGeometry)]) -> bool:
    from geo_pyspark.sql.types import GeometryType
    for geom in geoms:
        geom.__UDT__ = GeometryType()
    return True
