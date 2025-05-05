# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import inspect
import sys
from functools import partial

from pyspark.sql import Column

from sedona.spark.sql.dataframe_api import (
    ColumnOrName,
    call_sedona_function,
    validate_argument_types,
)

_call_aggregate_function = partial(call_sedona_function, "st_aggregates")


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


# Automatically populate __all__
__all__ = [
    name
    for name, obj in inspect.getmembers(sys.modules[__name__])
    if inspect.isfunction(obj)
]
