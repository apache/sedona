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
    return _call_predicate_function("ST_Contains", (a, b))


@validate_argument_types
def ST_Crosses(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_Crosses", (a, b))


@validate_argument_types
def ST_Disjoint(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_Disjoint", (a, b))


@validate_argument_types
def ST_Equals(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_Equals", (a, b))


@validate_argument_types
def ST_Intersects(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_Intersects", (a, b))


@validate_argument_types
def ST_OrderingEquals(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_OrderingEquals", (a, b))


@validate_argument_types
def ST_Overlaps(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_Overlaps", (a, b))


@validate_argument_types
def ST_Touches(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_Touches", (a, b))


@validate_argument_types
def ST_Within(a: ColumnOrName, b: ColumnOrName) -> Column:
    return _call_predicate_function("ST_Within", (a, b))
