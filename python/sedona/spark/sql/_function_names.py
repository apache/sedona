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

"""Resolve the names Sedona registers its Spark SQL functions under.

With ``spark.sedona.function.prefix`` set, Sedona registers every function only under the
prefixed name, for example ``sedona_ST_Point``. Code that calls a Sedona function by name
must add the prefix. This module is internal: it must import under pyspark-client, the
Spark Connect-only distribution, so it imports nothing JVM-bound at module level.
"""

from typing import Optional

from pyspark.errors import PySparkRuntimeError
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

try:
    from pyspark.sql.utils import is_remote
except ImportError:

    def is_remote():
        return False


PREFIX_CONFIG_KEY = "spark.sedona.function.prefix"

# A Spark Connect session caches the prefix it read from its conf under this attribute.
_PREFIX_CACHE_ATTRIBUTE = "_sedona_function_prefix"


def _active_or_default_session() -> Optional[SparkSession]:
    # SparkSession.active() is SparkSession.getActiveSession() falling back to the session the
    # builder created. The active session is thread-local, so a worker thread sees none.
    try:
        return SparkSession.active()
    except PySparkRuntimeError:
        return None


def function_prefix(spark: Optional[SparkSession] = None) -> str:
    """Return the prefix of Sedona's function names, or "" when there is no session.

    :param spark: the session to ask; defaults to the active session.
    """
    if spark is None:
        spark = _active_or_default_session()
        if spark is None:
            return ""
    if is_remote():
        # The server fixed the prefix when it started, so one round trip per session is enough.
        prefix = getattr(spark, _PREFIX_CACHE_ATTRIBUTE, None)
        if prefix is None:
            prefix = spark.conf.get(PREFIX_CONFIG_KEY, "")
            setattr(spark, _PREFIX_CACHE_ATTRIBUTE, prefix)
        return prefix
    # Never cache on classic: the JVM value changes from "" to the fixed prefix when the first
    # Sedona registrator runs, for example in SedonaContext.create.
    return spark._jvm.org.apache.sedona.sql.UDF.SedonaFunctionNames.effectivePrefix()


def resolve_function_name(name: str, spark: Optional[SparkSession] = None) -> str:
    """Return the name a Sedona function is registered under, for example sedona_ST_Point."""
    return function_prefix(spark) + name


def call_sedona_function_by_name(name: str, *args: Column) -> Column:
    """Call the Sedona SQL function ``name`` by its registered (prefixed) name."""
    return F.call_function(resolve_function_name(name), *args)
