from functools import partial
from typing import Optional, Union

from pyspark.sql import Column

from sedona.sql.dataframe_api import ColumnOrName, ColumnOrNameOrNumber, call_sedona_function, validate_argument_types


__all__ = [
    "ST_GeomFromGeoHash",
    "ST_GeomFromGeoJSON",
    "ST_GeomFromGML",
    "ST_GeomFromKML",
    "ST_GeomFromText",
    "ST_GeomFromWKB",
    "ST_GeomFromWKT",
    "ST_LineFromText",
    "ST_LineStringFromText",
    "ST_Point",
    "ST_PointFromText",
    "ST_PolygonFromEnvelope",
    "ST_PolygonFromText",
]


_call_constructor_function = partial(call_sedona_function, "st_constructors")


@validate_argument_types
def ST_GeomFromGeoHash(geohash: ColumnOrName, precision: Union[ColumnOrName, int]) -> Column:
    return _call_constructor_function("ST_GeomFromGeoHash", (geohash, precision))


@validate_argument_types
def ST_GeomFromGeoJSON(geojson_string: ColumnOrName) -> Column: 
    return _call_constructor_function("ST_GeomFromGeoJSON", geojson_string)


@validate_argument_types
def ST_GeomFromGML(gml_string: ColumnOrName) -> Column: 
    return _call_constructor_function("ST_GeomFromGML", gml_string)


@validate_argument_types
def ST_GeomFromKML(kml_string: ColumnOrName) -> Column:
    return _call_constructor_function("ST_GeomFromKML", kml_string)


@validate_argument_types
def ST_GeomFromText(wkt: ColumnOrName) -> Column:
    return _call_constructor_function("ST_GeomFromText", wkt)


@validate_argument_types
def ST_GeomFromWKB(wkb: ColumnOrName) -> Column:
    return _call_constructor_function("ST_GeomFromWKB", wkb)


@validate_argument_types
def ST_GeomFromWKT(wkt: ColumnOrName) -> Column:
    return _call_constructor_function("ST_GeomFromWKT", wkt)


@validate_argument_types
def ST_LineFromText(wkt: ColumnOrName) -> Column:
    return _call_constructor_function("ST_LineFromText", wkt)


@validate_argument_types
def ST_LineStringFromText(coords: ColumnOrName, delimiter: ColumnOrName) -> Column:
    return _call_constructor_function("ST_LineStringFromText", (coords, delimiter))


@validate_argument_types
def ST_Point(x: ColumnOrNameOrNumber, y: ColumnOrNameOrNumber, z: Optional[ColumnOrNameOrNumber] = None) -> Column:
    args = (x, y) if z is None else (x, y, z)
    return _call_constructor_function("ST_Point", args)


@validate_argument_types
def ST_PointFromText(coords: ColumnOrName, delimiter: ColumnOrName) -> Column:
    return _call_constructor_function("ST_PointFromText", (coords, delimiter))


@validate_argument_types
def ST_PolygonFromEnvelope(min_x: ColumnOrNameOrNumber, min_y: ColumnOrNameOrNumber, max_x: ColumnOrNameOrNumber, max_y: ColumnOrNameOrNumber) -> Column:
    return _call_constructor_function("ST_PolygonFromEnvelope", (min_x, min_y, max_x, max_y))


@validate_argument_types
def ST_PolygonFromText(coords: ColumnOrName, delimiter: ColumnOrName) -> Column: 
    return _call_constructor_function("ST_PolygonFromText", (coords, delimiter))
