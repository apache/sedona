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
    "ST_MLineFromText",
    "ST_MPolyFromText"
]


_call_constructor_function = partial(call_sedona_function, "st_constructors")


@validate_argument_types
def ST_GeomFromGeoHash(geohash: ColumnOrName, precision: Union[ColumnOrName, int]) -> Column:
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
def ST_GeomFromText(wkt: ColumnOrName) -> Column:
    """Generate a geometry column from a Well-Known Text (WKT) string column.
    This is an alias of ST_GeomFromWKT.

    :param wkt: WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: Geometry column representing the WKT string.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromText", wkt)


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
def ST_GeomFromWKT(wkt: ColumnOrName) -> Column:
    """Generate a geometry column from a Well-Known Text (WKT) string column.
    This is an alias of ST_GeomFromText.

    :param wkt: WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: Geometry column representing the WKT string.
    :rtype: Column
    """
    return _call_constructor_function("ST_GeomFromWKT", wkt)


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
    """Generate a linestring geometry column from a list of coords seperated by a delimiter
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
def ST_Point(x: ColumnOrNameOrNumber, y: ColumnOrNameOrNumber, z: Optional[ColumnOrNameOrNumber] = None) -> Column:
    """Generate either a 2D or 3D point geometry column from numeric values.

    :param x: Either a number or numeric column representing the X coordinate of a point.
    :type x: ColumnOrNameOrNumber
    :param y: Either a number or numeric column representing the Y coordinate of a point.
    :type y: ColumnOrNameOrNumber
    :param z: Either a number or numeric column representing the Z coordinate of a point, if None then a 2D point is generated, defaults to None
    :type z: Optional[ColumnOrNameOrNumber], optional
    :return: Point geometry column generated from the coordinate values.
    :rtype: Column
    """
    args = (x, y) if z is None else (x, y, z)
    return _call_constructor_function("ST_Point", args)


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
def ST_PolygonFromEnvelope(min_x: ColumnOrNameOrNumber, min_y: ColumnOrNameOrNumber, max_x: ColumnOrNameOrNumber, max_y: ColumnOrNameOrNumber) -> Column:
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
    return _call_constructor_function("ST_PolygonFromEnvelope", (min_x, min_y, max_x, max_y))


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
def ST_MPolyFromText(wkt: ColumnOrName) -> Column:
    """Generate multiPolygon geometry from a multiPolygon WKT representation.

    :param wkt: multiPolygon WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: multiPolygon geometry generated from the wkt column.
    :rtype: Column
    """
    return _call_constructor_function("ST_MPolyFromText", wkt)

@validate_argument_types
def ST_MLineFromText(wkt: ColumnOrName) -> Column:
    """Generate multiLineString geometry from a multiLineString WKT representation.

    :param wkt: multiLineString WKT string column to generate from.
    :type wkt: ColumnOrName
    :return: multiLineString geometry generated from the wkt column.
    :rtype: Column
    """
    return _call_constructor_function("ST_MLineFromText", wkt)
