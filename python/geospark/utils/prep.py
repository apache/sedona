from typing import List

from shapely.geometry import Point, MultiPoint, Polygon, MultiPolygon, LineString, MultiLineString
from shapely.geometry.base import BaseGeometry


def assign_all() -> bool:
    geoms = [Point, MultiPoint, Polygon, MultiPolygon, LineString, MultiLineString]
    assign_udt_shapely_objects(geoms=geoms)
    assign_user_data_to_shapely_objects(geoms=geoms)
    return True


def assign_udt_shapely_objects(geoms: List[type(BaseGeometry)]) -> bool:
    from geospark.sql.types import GeometryType
    for geom in geoms:
        geom.__UDT__ = GeometryType()
    return True


def assign_user_data_to_shapely_objects(geoms: List[type(BaseGeometry)]) -> bool:
    for geom in geoms:
        geom.getUserData = lambda geom_instance: geom_instance.userData
