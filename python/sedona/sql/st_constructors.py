#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import inspect
import sys
from functools import partial
from typing import Optional, Union

from pyspark.sql import Column

from sedona.sql.dataframe_api import (
    ColumnOrName,
    ColumnOrNameOrNumber,
    call_sedona_function,
    validate_argument_types,
)

_call_constructor_function = partial(call_sedona_function, "st_constructors")


@validate_argument_types
def ST_GeomFromGeoHash(
    geohash: ColumnOrName, precision: Union[ColumnOrName, int]
) -> Column:
    """Generate a geometry column from a geohash column at a specified precision.

    :param geohash: Geohash string column to generate from.
    :type geohash: ColumnOrName
    :param precision: Geohash precision to use, either an integer or an integer column.
    :type precision: Union[ColumnOrName, int]
    :return: Geometry column representing the supplied geohash and precision level.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromGeoHash", (geohash, precision))


@validate_argument_types
def ST_PointFromGeoHash(
    geohash: ColumnOrName, precision: Optional[Union[ColumnOrName, int]] = None
) -> Column:
    """Generate a point column from a geohash column at a specified precision.

    :param geohash: Geohash string column to generate from.
    :type geohash: ColumnOrName
    :param precision: Geohash precision to use, either an integer or an integer column.
    :type precision: Union[ColumnOrName, int]
    :return: Point column representing the supplied geohash and precision level.
    :rtype: Column
    """
    args = (geohash) if precision is None else (geohash, precision)

    return _call_constructor_function("ST_PointFromGeoHash", args)


@validate_argument_types
def ST_GeomFromGeoJSON(geojson_string: ColumnOrName) -> Column:
    """Generate a geometry column from a GeoJSON string column.

    :param geojson_string: GeoJSON string column to generate from.
    :type geojson_string: ColumnOrName
    :return: Geometry column representing the GeoJSON string.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromGeoJSON", geojson_string)


@validate_argument_types
def ST_GeomFromGML(gml_string: ColumnOrName) -> Column:
    """Generate a geometry column from a Geography Markup Language (GML) string column.

    :param gml_string: GML string column to generate from.
    :type gml_string: ColumnOrName
    :return: Geometry column representing the GML string.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromGML", gml_string)


@validate_argument_types
def ST_GeomFromKML(kml_string: ColumnOrName) -> Column:
    """Generate a geometry column from a KML string column.

    :param kml_string: KML string column to generate from.
    :type kml_string: ColumnOrName
    :return: Geometry column representing the KML string.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromKML", kml_string)


@validate_argument_types
def ST_GeomFromText(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate a geometry column from a Well-Known Text (WKT) string column.
    This is an alias of ST_GeomFromWKT.

    :param wkt: WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: Geometry column representing the WKT string.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_GeomFromText", args)


@validate_argument_types
def ST_GeometryFromText(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate a geometry column from a Well-Known Text (WKT) string column.
    This is an alias of ST_GeomFromWKT.

    :param wkt: WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: Geometry column representing the WKT string.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_GeometryFromText", args)


@validate_argument_types
def ST_GeomFromWKB(wkb: ColumnOrName) -> Column:
    """Generate a geometry column from a Well-Known Binary (WKB) binary column.

    :param wkb: WKB binary column to generate from.
    :type wkb: ColumnOrName
    :return: Geometry column representing the WKB binary.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromWKB", wkb)


@validate_argument_types
def ST_GeomFromEWKB(wkb: ColumnOrName) -> Column:
    """Generate a geometry column from a Well-Known Binary (WKB) binary column.

    :param wkb: WKB binary column to generate from.
    :type wkb: ColumnOrName
    :return: Geometry column representing the WKB binary.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromEWKB", wkb)


@validate_argument_types
def ST_GeomFromWKT(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate a geometry column from a Well-Known Text (WKT) string column.
    This is an alias of ST_GeomFromText.

    :param wkt: WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: Geometry column representing the WKT string.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_GeomFromWKT", args)


@validate_argument_types
def ST_GeogFromWKT(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate a geography column from a Well-Known Text (WKT) string column.

    :param wkt: WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: Geography column representing the WKT string.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_GeogFromWKT", args)


@validate_argument_types
def ST_GeomFromEWKT(ewkt: ColumnOrName) -> Column:
    """Generate a geometry column from a OGC Extended Well-Known Text (WKT) string column.

    :param ewkt: OGC Extended WKT string column to generate from.
    :type ewkt: ColumnOrName
    :return: Geometry column representing the EWKT string.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromEWKT", ewkt)


@validate_argument_types
def ST_LineFromText(wkt: ColumnOrName) -> Column:
    """Generate linestring geometry from a linestring WKT representation.

    :param wkt: Linestring WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: Linestring geometry generated from the wkt column.
    :rtype: Column
    """
    return _call_constructor_function("ST_LineFromText", wkt)


@validate_argument_types
def ST_LineStringFromText(coords: ColumnOrName, delimiter: ColumnOrName) -> Column:
    """Generate a linestring geometry column from a list of coords separated by a delimiter
    in a string column.

    :param coords: String column containing a list of coords.
    :type coords: ColumnOrName
    :param delimiter: Delimiter that separates each coordinate in the coords column, a string constant must be wrapped as a string literal (using pyspark.sql.functions.lit).
    :type delimiter: ColumnOrName
    :return: Linestring geometry column generated from the list of coordinates.
    :rtype: Column
    """
    return _call_constructor_function("ST_LineStringFromText", (coords, delimiter))


@validate_argument_types
def ST_Point(x: ColumnOrNameOrNumber, y: ColumnOrNameOrNumber) -> Column:
    """Generates a 2D point geometry column from numeric values.

    :param x: Either a number or numeric column representing the X coordinate of a point.
    :type x: ColumnOrNameOrNumber
    :param y: Either a number or numeric column representing the Y coordinate of a point.
    :type y: ColumnOrNameOrNumber
    :return: Point geometry column generated from the coordinate values.
    :rtype: Column
    """
    return _call_constructor_function("ST_Point", (x, y))


@validate_argument_types
def ST_PointZ(
    x: ColumnOrNameOrNumber,
    y: ColumnOrNameOrNumber,
    z: ColumnOrNameOrNumber,
    srid: Optional[ColumnOrNameOrNumber] = None,
) -> Column:
    """Generates a 3D point geometry column from numeric values.

    :param x: Either a number or numeric column representing the X coordinate of a point.
    :type x: ColumnOrNameOrNumber
    :param y: Either a number or numeric column representing the Y coordinate of a point.
    :type y: ColumnOrNameOrNumber
    :param z: Either a number or numeric column representing the Z coordinate of a point, if None then a 2D point is generated, defaults to None
    :type z: ColumnOrNameOrNumber
    :param srid: The srid of the point. Defaults to 0 (unknown).
    :type srid: Optional[ColumnOrNameOrNumber], optional
    :return: Point geometry column generated from the coordinate values.
    :rtype: Column
    """
    args = (x, y, z) if srid is None else (x, y, z, srid)
    return _call_constructor_function("ST_PointZ", args)


@validate_argument_types
def ST_PointM(
    x: ColumnOrNameOrNumber,
    y: ColumnOrNameOrNumber,
    m: ColumnOrNameOrNumber,
    srid: Optional[ColumnOrNameOrNumber] = None,
) -> Column:
    """Generates a 3D point geometry column from numeric values.

    :param x: Either a number or numeric column representing the X coordinate of a point.
    :type x: ColumnOrNameOrNumber
    :param y: Either a number or numeric column representing the Y coordinate of a point.
    :type y: ColumnOrNameOrNumber
    :param z: Either a number or numeric column representing the Z coordinate of a point, if None then a 2D point is generated, defaults to None
    :type z: ColumnOrNameOrNumber
    :param srid: The srid of the point. Defaults to 0 (unknown).
    :type srid: Optional[ColumnOrNameOrNumber], optional
    :return: Point geometry column generated from the coordinate values.
    :rtype: Column
    """
    args = (x, y, m) if srid is None else (x, y, m, srid)
    return _call_constructor_function("ST_PointM", args)


@validate_argument_types
def ST_PointZM(
    x: ColumnOrNameOrNumber,
    y: ColumnOrNameOrNumber,
    z: ColumnOrNameOrNumber,
    m: ColumnOrNameOrNumber,
    srid: Optional[ColumnOrNameOrNumber] = None,
) -> Column:
    """Generates a 3D point geometry column from numeric values.

    :param x: Either a number or numeric column representing the X coordinate of a point.
    :type x: ColumnOrNameOrNumber
    :param y: Either a number or numeric column representing the Y coordinate of a point.
    :type y: ColumnOrNameOrNumber
    :param z: Either a number or numeric column representing the Z coordinate of a point, if None then a 2D point is generated, defaults to None
    :type z: ColumnOrNameOrNumber
    :param m: Either a number or numeric column representing the M value of a point.
    :type m: ColumnOrNameOrNumber
    :param srid: The srid of the point. Defaults to 0 (unknown).
    :type srid: Optional[ColumnOrNameOrNumber], optional
    :return: Point geometry column generated from the coordinate values.
    :rtype: Column
    """
    args = (x, y, z, m) if srid is None else (x, y, z, m, srid)
    return _call_constructor_function("ST_PointZM", args)


@validate_argument_types
def ST_PointFromText(coords: ColumnOrName, delimiter: ColumnOrName) -> Column:
    """Generate a point geometry column from coordinates separated by a delimiter and stored
    in a string column.

    :param coords: String column with the stored coordinates.
    :type coords: ColumnOrName
    :param delimiter: Delimiter separating the coordinates, a string constant must be wrapped as a string literal (using pyspark.sql.functions.lit).
    :type delimiter: ColumnOrName
    :return: Point geometry column generated from the coordinates.
    :rtype: Column
    """
    return _call_constructor_function("ST_PointFromText", (coords, delimiter))


@validate_argument_types
def ST_PointFromWKB(
    wkb: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate a Point geometry column from a Well-Known Binary (WKB) binary column.

    :param wkb: WKB binary column to generate from.
    :type wkb: ColumnOrName
    :param srid: SRID to be set for geometry
    :type srid: ColumnOrNameOrNumber
    :return: Point Geometry column representing the WKB binary.
    :rtype: Column
    """
    args = (wkb) if srid is None else (wkb, srid)
    return _call_constructor_function("ST_PointFromWKB", args)


@validate_argument_types
def ST_LineFromWKB(
    wkb: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate a Line geometry column from a Well-Known Binary (WKB) binary column.

    :param wkb: WKB binary column to generate from.
    :type wkb: ColumnOrName
    :param srid: SRID to be set for the geometry.
    :type srid: ColumnOrNameOrNumber
    :return: Geometry column representing the WKB binary.
    :rtype: Column
    """
    args = (wkb) if srid is None else (wkb, srid)
    return _call_constructor_function("ST_LineFromWKB", args)


@validate_argument_types
def ST_LinestringFromWKB(
    wkb: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate a Line geometry column from a Well-Known Binary (WKB) binary column.

    :param wkb: WKB binary column to generate from.
    :type wkb: ColumnOrName
    :param srid: SRID to be set for the geometry.
    :type srid: ColumnOrNameOrNumber
    :return: Geometry column representing the WKB binary.
    :rtype: Column
    """
    args = (wkb) if srid is None else (wkb, srid)
    return _call_constructor_function("ST_LinestringFromWKB", args)


@validate_argument_types
def ST_MakePointM(
    x: ColumnOrNameOrNumber, y: ColumnOrNameOrNumber, m: ColumnOrNameOrNumber
) -> Column:
    """Generate 3D M Point geometry.

    :param x: Either a number or numeric column representing the X coordinate of a point.
    :type x: ColumnOrNameOrNumber
    :param y: Either a number or numeric column representing the Y coordinate of a point.
    :type y: ColumnOrNameOrNumber
    :param m: Either a number or numeric column representing the M coordinate of a point
    :type m: ColumnOrNameOrNumber
    :return: Point geometry column generated from the coordinate values.
    :rtype: Column
    """
    return _call_constructor_function("ST_MakePointM", (x, y, m))


@validate_argument_types
def ST_MakePoint(
    x: ColumnOrNameOrNumber,
    y: ColumnOrNameOrNumber,
    z: Optional[ColumnOrNameOrNumber] = None,
    m: Optional[ColumnOrNameOrNumber] = None,
) -> Column:
    """Generate a 2D, 3D Z or 4D ZM Point geometry. If z is None then a 2D point is generated.
    This function doesn't support M coordinates for creating a 4D ZM Point in Dataframe API.

    :param x: Either a number or numeric column representing the X coordinate of a point.
    :type x: ColumnOrNameOrNumber
    :param y: Either a number or numeric column representing the Y coordinate of a point.
    :type y: ColumnOrNameOrNumber
    :param z: Either a number or numeric column representing the Z coordinate of a point, if None then a 2D point is generated, defaults to None
    :type z: ColumnOrNameOrNumber
    :param m: Either a number or numeric column representing the M coordinate of a point, if None then a point without M coordinate is generated, defaults to None
    :type m: ColumnOrNameOrNumber
    :return: Point geometry column generated from the coordinate values.
    :rtype: Column
    """
    args = (x, y)
    if z is not None:
        args = args + (z,)
    if m is not None:
        args = args + (m,)
    return _call_constructor_function("ST_MakePoint", (args))


@validate_argument_types
def ST_MakeEnvelope(
    min_x: ColumnOrNameOrNumber,
    min_y: ColumnOrNameOrNumber,
    max_x: ColumnOrNameOrNumber,
    max_y: ColumnOrNameOrNumber,
    srid: Optional[ColumnOrNameOrNumber] = None,
) -> Column:
    """Generate a polygon geometry column from the minimum and maximum coordinates of an envelope with an option to add SRID

    :param min_x: Minimum X coordinate for the envelope.
    :type min_x: ColumnOrNameOrNumber
    :param min_y: Minimum Y coordinate for the envelope.
    :type min_y: ColumnOrNameOrNumber
    :param max_x: Maximum X coordinate for the envelope.
    :type max_x: ColumnOrNameOrNumber
    :param max_y: Maximum Y coordinate for the envelope.
    :type max_y: ColumnOrNameOrNumber
    :param srid: SRID to be set for the envelope.
    :type srid: ColumnOrNameOrNumber
    :return: Polygon geometry column representing the envelope described by the coordinate bounds.
    :rtype: Column
    """
    args = (min_x, min_y, max_x, max_y, srid)
    if srid is None:
        args = (min_x, min_y, max_x, max_y)

    return _call_constructor_function("ST_MakeEnvelope", args)


@validate_argument_types
def ST_PolygonFromEnvelope(
    min_x: ColumnOrNameOrNumber,
    min_y: ColumnOrNameOrNumber,
    max_x: ColumnOrNameOrNumber,
    max_y: ColumnOrNameOrNumber,
) -> Column:
    """Generate a polygon geometry column from the minimum and maximum coordinates of an envelope.

    :param min_x: Minimum X coordinate for the envelope.
    :type min_x: ColumnOrNameOrNumber
    :param min_y: Minimum Y coordinate for the envelope.
    :type min_y: ColumnOrNameOrNumber
    :param max_x: Maximum X coordinate for the envelope.
    :type max_x: ColumnOrNameOrNumber
    :param max_y: Maximum Y coordinate for the envelope.
    :type max_y: ColumnOrNameOrNumber
    :return: Polygon geometry column representing the envelope described by the coordinate bounds.
    :rtype: Column
    """
    return _call_constructor_function(
        "ST_PolygonFromEnvelope", (min_x, min_y, max_x, max_y)
    )


@validate_argument_types
def ST_PolygonFromText(coords: ColumnOrName, delimiter: ColumnOrName) -> Column:
    """Generate a polygon from a list of coordinates separated by a delimiter stored
    in a string column.

    :param coords: String column containing the coordinates.
    :type coords: ColumnOrName
    :param delimiter: Delimiter separating the coordinates, a string constant must be wrapped as a string literal (using pyspark.sql.functions.lit).
    :type delimiter: ColumnOrName
    :return: Polygon geometry column generated from the list of coordinates.
    :rtype: Column
    """
    return _call_constructor_function("ST_PolygonFromText", (coords, delimiter))


@validate_argument_types
def ST_MPolyFromText(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate multiPolygon geometry from a multiPolygon WKT representation.

    :param wkt: multiPolygon WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: multiPolygon geometry generated from the wkt column.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_MPolyFromText", args)


@validate_argument_types
def ST_MLineFromText(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate multiLineString geometry from a multiLineString WKT representation.

    :param wkt: multiLineString WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: multiLineString geometry generated from the wkt column.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_MLineFromText", args)


@validate_argument_types
def ST_MPointFromText(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate MultiPoint geometry from a MultiPoint WKT representation.

    :param wkt: MultiPoint WKT string column to generate from.
    :type wkt: ColumnOrName
    :param srid: SRID for the geometry
    :type srid: ColumnOrNameOrNumber
    :return: MultiPoint geometry generated from the wkt column.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_MPointFromText", args)


@validate_argument_types
def ST_GeomCollFromText(
    wkt: ColumnOrName, srid: Optional[ColumnOrNameOrNumber] = None
) -> Column:
    """Generate GeometryCollection geometry from a GeometryCollection WKT representation.

    :param wkt: GeometryCollection WKT string column to generate from.
    :type wkt: ColumnOrName
    :param srid: SRID for the geometry
    :type srid: ColumnOrNameOrNumber
    :return: GeometryCollection geometry generated from the wkt column.
    :rtype: Column
    """
    args = (wkt) if srid is None else (wkt, srid)

    return _call_constructor_function("ST_GeomCollFromText", args)


# Automatically populate __all__
__all__ = [
    name
    for name, obj in inspect.getmembers(sys.modules[__name__])
    if inspect.isfunction(obj)
]
