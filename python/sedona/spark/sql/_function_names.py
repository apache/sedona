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

# A Sedona function that no platform has. On Spark Connect the prefix in effect is read from
# the name the server lists this function under. SedonaFunctionNames.scala has the same name.
_MARKER_FUNCTION_NAME = "__sedona_internal_coverage_invalid_edges_for_target"

# Lists the marker function. The namespace is named: without one Spark lists the functions of
# the session's current catalog, which may list none of Sedona's or may not support functions
# at all. SedonaFunctionNames.scala runs the same statement.
_MARKER_LOOKUP_SQL = (
    f"SHOW FUNCTIONS IN spark_catalog.default LIKE '*{_MARKER_FUNCTION_NAME}'"
)

# A Spark Connect session caches the prefix its server reported under this attribute.
_PREFIX_CACHE_ATTRIBUTE = "_sedona_function_prefix"


def _prefix_of_listed_names(listed) -> str:
    # Spark lists function names in lower case, and resolves them ignoring case. No name means
    # no Sedona on the server, or a Sedona from before the prefix.
    marker = _MARKER_FUNCTION_NAME.lower()
    names = sorted({name.lower() for name in listed if name.lower().endswith(marker)})
    if not names:
        return ""
    if len(names) > 1:
        # Sedona registers the marker under one name. The others are not Sedona's, for example
        # a temporary function of the session, and the list does not say which they are. A
        # guessed prefix could call a platform's function.
        raise RuntimeError(
            "Cannot tell the prefix of Sedona's function names: the session has more than "
            f"one function whose name ends in {_MARKER_FUNCTION_NAME} ({', '.join(names)}). "
            "Drop the ones that are not Sedona's."
        )
    return names[0][: -len(marker)]


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
        # The server's prefix does not change once fixed, so each session is asked once.
        prefix = getattr(spark, _PREFIX_CACHE_ATTRIBUTE, None)
        if prefix is None:
            # The server's function registry says under which names Sedona's functions are
            # registered, and a client cannot write to it as it can to its session's conf.
            # Listing the function also makes a server that loads Sedona through
            # spark.sql.extensions register Sedona for this session first. An error
            # propagates: a guessed plain name could call a platform's function.
            rows = spark.sql(_MARKER_LOOKUP_SQL).collect()
            prefix = _prefix_of_listed_names(row[0] for row in rows)
            setattr(spark, _PREFIX_CACHE_ATTRIBUTE, prefix)
        return prefix
    # Never cache on classic: the JVM value changes from "" to the fixed prefix when the first
    # Sedona registrator runs, for example in SedonaContext.create.
    # A Sedona jar older than the prefix has no SedonaFunctionNames, and such a JVM has no prefix.
    from py4j.protocol import Py4JError, Py4JNetworkError

    try:
        effective_prefix = (
            spark._jvm.org.apache.sedona.sql.UDF.SedonaFunctionNames.effectivePrefix
        )
    except Py4JNetworkError:
        raise
    except Py4JError:
        # The JVM has no such class or method.
        return ""
    try:
        return effective_prefix()
    except TypeError:
        # py4j returns a JavaPackage for a class it cannot find, and a JavaPackage is not
        # callable. The call takes no arguments, so this is the only TypeError it can raise.
        return ""


def resolve_function_name(name: str, spark: Optional[SparkSession] = None) -> str:
    """Return the name a Sedona function is registered under, for example sedona_ST_Point."""
    return function_prefix(spark) + name


def call_sedona_function_by_name(name: str, *args: Column) -> Column:
    """Call the Sedona SQL function ``name`` by its registered (prefixed) name."""
    return F.call_function(resolve_function_name(name), *args)
