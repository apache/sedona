from functools import partial

from pyspark.sql import Column

from sedona.sql.dataframe_api import ColumnOrName, call_sedona_function, validate_argument_types

_call_aggregate_function = partial(call_sedona_function, "st_aggregates")

__all__ = [
    "ST_Envelope_Aggr",
    "ST_Intersection_Aggr",
    "ST_Union_Aggr",
]


@validate_argument_types
def ST_Envelope_Aggr(geometry: ColumnOrName) -> Column:
    """Aggregate Function: Get the aggregate envelope of a geometry column.

    :param geometry: Geometry column to aggregate.
    :type geometry: ColumnOrName
    :return: Geometry representing the aggregate envelope of the geometry column.
    :rtype: Column
    """
    return _call_aggregate_function("ST_Envelope_Aggr", geometry)


@validate_argument_types
def ST_Intersection_Aggr(geometry: ColumnOrName) -> Column:
    """Aggregate Function: Get the aggregate intersection of a geometry column.

    :param geometry: Geometry column to aggregate.
    :type geometry: ColumnOrName
    :return: Geometry representing the aggregate intersection of the geometry column.
    :rtype: Column
    """
    return _call_aggregate_function("ST_Intersection_Aggr", geometry)


@validate_argument_types
def ST_Union_Aggr(geometry: ColumnOrName) -> Column:
    """Aggregate Function: Get the aggregate union of a geometry column.

    :param geometry: Geometry column to aggregate.
    :type geometry: ColumnOrName
    :return: Geometry representing the aggregate union of the geometry column.
    :rtype: Column
    """
    return _call_aggregate_function("ST_Union_Aggr", geometry)
