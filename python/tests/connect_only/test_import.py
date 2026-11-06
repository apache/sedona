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

import pytest

try:
    from pyspark.util import is_remote_only
except ImportError:  # PySpark before 4.0: always the full distribution

    def is_remote_only():
        return False


# These tests run where pyspark-client, the Spark Connect-only distribution, replaces pyspark:
# no RDD API, no SparkContext, no py4j, no JVM. The CI job installs it on purpose.
pytestmark = pytest.mark.skipif(
    not is_remote_only(), reason="needs pyspark-client (Spark Connect only)"
)


def test_sedona_spark_imports_without_the_rdd_api():
    import sedona.spark

    for name in (
        "Circle",
        "Envelope",
        "GeoData",
        "Geography",
        "GeometryType",
        "GridType",
        "IndexType",
        "SedonaContext",
        "ST_Intersects",
        "ST_Point",
        "dataframe_to_arrow",
        "dbscan",
    ):
        assert hasattr(sedona.spark, name), name

    # The RDD API is not there to export.
    assert not hasattr(sedona.spark, "PointRDD")
    assert not hasattr(sedona.spark, "Adapter")


def test_star_import_works():
    namespace = {}
    exec("from sedona.spark import *", namespace)
    assert "SedonaContext" in namespace
    assert "ST_Point" in namespace
    assert "PointRDD" not in namespace


def test_sedona_context_builder_is_the_spark_connect_builder():
    from pyspark.sql import SparkSession

    from sedona.spark import SedonaContext

    builder = SedonaContext.builder()
    assert isinstance(builder, SparkSession.Builder)
    options = builder._options
    assert options["spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
    assert (
        options["spark.kryo.registrator"]
        == "org.apache.sedona.core.serde.SedonaKryoRegistrator"
    )


def test_core_package_resolves_range_query_raw_lazily():
    import sedona.spark.core as core

    assert "RangeQueryRaw" in core.__all__
    with pytest.raises(ImportError):
        core.RangeQueryRaw
    with pytest.raises(AttributeError):
        core.NoSuchName
