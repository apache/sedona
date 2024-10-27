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
    call_sedona_function,
    validate_argument_types,
)

_call_predicate_function = partial(call_sedona_function, "st_predicates")


@validate_argument_types
def ST_Contains(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether geometry a contains geometry b.

    :param a: Geometry column to check containment for.
    :type a: ColumnOrName
    :param b: Geometry column to check containment of.
    :type b: ColumnOrName
    :return: True if geometry a contains geometry b and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Contains", (a, b))


@validate_argument_types
def ST_Crosses(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether geometry a crosses geometry b.

    :param a: Geometry to check crossing with.
    :type a: ColumnOrName
    :param b: Geometry to check crossing of.
    :type b: ColumnOrName
    :return: True if geometry a cross geometry b and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Crosses", (a, b))


@validate_argument_types
def ST_Disjoint(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether two geometries are disjoint.

    :param a: One geometry column to check.
    :type a: ColumnOrName
    :param b: Other geometry column to check.
    :type b: ColumnOrName
    :return: True if a and b are disjoint and False otherwise as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Disjoint", (a, b))


@validate_argument_types
def ST_Equals(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether two geometries are equal regardless of vertex ordering.

    :param a: One geometry column to check.
    :type a: ColumnOrName
    :param b: Other geometry column to check.
    :type b: ColumnOrName
    :return: True if a and b are equal and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Equals", (a, b))


@validate_argument_types
def ST_Intersects(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether two geometries intersect.

    :param a: One geometry column to check.
    :type a: ColumnOrName
    :param b: Other geometry column to check.
    :type b: ColumnOrName
    :return: True if a and b intersect and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Intersects", (a, b))


@validate_argument_types
def ST_OrderingEquals(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether two geometries have identical vertices that are in the same order.

    :param a: One geometry column to check.
    :type a: ColumnOrName
    :param b: Other geometry column to check.
    :type b: ColumnOrName
    :return: True if a and b are equal and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_OrderingEquals", (a, b))


@validate_argument_types
def ST_Overlaps(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether geometry a overlaps geometry b.

    :param a: Geometry column to check overlapping for.
    :type a: ColumnOrName
    :param b: Geometry column to check overlapping of.
    :type b: ColumnOrName
    :return: True if a overlaps b and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Overlaps", (a, b))


@validate_argument_types
def ST_Touches(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether two geometries touch.

    :param a: One geometry column to check.
    :type a: ColumnOrName
    :param b: Other geometry column to check.
    :type b: ColumnOrName
    :return: True if a and b touch and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Touches", (a, b))


@validate_argument_types
def ST_Relate(
    a: ColumnOrName, b: ColumnOrName, intersectionMatrix: Optional[ColumnOrName] = None
) -> Column:
    """Check whether two geometries are related to each other.

    :param a: One geometry column to check.
    :type a: ColumnOrName
    :param b: Other geometry column to check.
    :type b: ColumnOrName
    :param intersectionMatrix: intersectionMatrix column to check
    :type intersectionMatrix: ColumnOrName
    :return: True if a and b touch and False otherwise, as a boolean column.
    :rtype: Column
    """
    args = (a, b) if intersectionMatrix is None else (a, b, intersectionMatrix)

    return _call_predicate_function("ST_Relate", args)


@validate_argument_types
def ST_RelateMatch(matrix1: ColumnOrName, matrix2: ColumnOrName) -> Column:
    """Check whether two DE-9IM are related to each other.

    :param matrix1: One geometry column to check.
    :type matrix1: ColumnOrName
    :param matrix2: Other geometry column to check.
    :type matrix2: ColumnOrName
    :return: True if a and b touch and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_RelateMatch", (matrix1, matrix2))


@validate_argument_types
def ST_Within(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check if geometry a is within geometry b.

    :param a: Geometry column to check.
    :type a: ColumnOrName
    :param b: Geometry column to check.
    :type b: ColumnOrName
    :return: True if a is within b and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Within", (a, b))


@validate_argument_types
def ST_Covers(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check whether geometry a covers geometry b.

    :param a: Geometry column to check.
    :type a: ColumnOrName
    :param b: Geometry column to check.
    :type b: ColumnOrName
    :return: True if a covers b and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_Covers", (a, b))


@validate_argument_types
def ST_CoveredBy(a: ColumnOrName, b: ColumnOrName) -> Column:
    """Check if geometry a is covered by geometry b.

    :param a: Geometry column to check.
    :type a: ColumnOrName
    :param b: Geometry column to check.
    :type b: ColumnOrName
    :return: True if a is covered by b and False otherwise, as a boolean column.
    :rtype: Column
    """
    return _call_predicate_function("ST_CoveredBy", (a, b))


@validate_argument_types
def ST_DWithin(
    a: ColumnOrName,
    b: ColumnOrName,
    distance: Union[ColumnOrName, float],
    use_sphere: Optional[Union[ColumnOrName, bool]] = None,
):
    """
    Check if geometry a is within 'distance' units of geometry b
    :param a: Geometry column to check
    :param b: Geometry column to check
    :param distance: distance units to check the within predicate
    :param use_sphere: whether to use spheroid distance or euclidean distance
    :return: True if a is within distance units of Geometry b
    """
    args = (
        (a, b, distance, use_sphere)
        if use_sphere is not None
        else (
            a,
            b,
            distance,
        )
    )
    return _call_predicate_function("ST_DWithin", args)


# Automatically populate __all__
__all__ = [
    name
    for name, obj in inspect.getmembers(sys.modules[__name__])
    if inspect.isfunction(obj)
]
