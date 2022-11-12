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

from pyspark.sql import Column

from sedona.sql.dataframe_api import ColumnOrName, call_sedona_function, validate_argument_types


__all__ = [
    "ST_Contains",
    "ST_Crosses",
    "ST_Disjoint",
    "ST_Equals",
    "ST_Intersects",
    "ST_OrderingEquals",
    "ST_Overlaps",
    "ST_Touches",
    "ST_Within",
]


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
    """Check whether two geometries are equal regardles of vertex ordering.

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
