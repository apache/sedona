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

"""Scalar access to native geometry and explicit UDT columns in GeoPandas."""

from pyspark.sql import functions as F

from sedona.spark.sql import st_functions as stf
from sedona.spark.sql.types import LegacyGeometryType, USES_NATIVE_SPATIAL_TYPES


def geometry_srid(column, data_type):
    if USES_NATIVE_SPATIAL_TYPES and isinstance(data_type, LegacyGeometryType):
        # Explicit UDT SQL values use a signed little-endian SRID prefix.
        # Spark's built-in spatial functions accept only its native types.
        prefix = F.substring(F.unwrap_udt(column), 1, 4)
        unsigned = F.conv(F.hex(F.reverse(prefix)), 16, 10).cast("long")
        return (
            F.when(unsigned >= 2**31, unsigned - 2**32).otherwise(unsigned).cast("int")
        )
    return stf.ST_SRID(column)


def geometry_wkb(column, data_type):
    if USES_NATIVE_SPATIAL_TYPES and isinstance(data_type, LegacyGeometryType):
        # The remaining canonical payload is ISO WKB, including Z/M layouts.
        payload = F.unwrap_udt(column)
        return F.substring(payload, 5, F.length(payload))
    return stf.ST_AsBinary(column)
