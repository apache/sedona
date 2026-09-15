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

import os

import pytest
from pyspark.sql import functions as F

from sedona.spark import SedonaContext

pytestmark = pytest.mark.skipif(
    bool(os.getenv("SPARK_REMOTE")),
    reason="Coverage simplification requires Spark Classic",
)


@pytest.fixture(scope="module")
def spark():
    if "SPARK_HOME" in os.environ and not os.environ["SPARK_HOME"]:
        del os.environ["SPARK_HOME"]
    builder = (
        SedonaContext.builder()
        .master("local[2]")
        .appName("CoverageSimplificationTest")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
    )
    if os.getenv("SEDONA_PYTHON_EXTRA_JARS"):
        builder = builder.config("spark.jars", os.environ["SEDONA_PYTHON_EXTRA_JARS"])
    session = SedonaContext.create(builder.getOrCreate())
    previous = session.sparkContext._jsc.sc().checkpointDir()
    yield session
    getattr(session.sparkContext._jsc.sc(), "checkpointDir_$eq")(previous)


def _source(spark):
    return spark.createDataFrame(
        [
            (0, "POLYGON ((0 0, 1 0, 1.05 1, 1 2, 0 2, 0 0))", "left"),
            (1, "POLYGON ((1 0, 2 0, 2 2, 1 2, 1.05 1, 1 0))", "right"),
        ],
        "id long, wkt string, label string",
    ).selectExpr("label", "ST_GeomFromWKT(wkt) geom", "id")


def test_shared_boundary_output_survives_intermediate_cleanup(spark, tmp_path):
    from sedona.spark.geopandas._coverage import simplify_coverage

    spark.sparkContext.setCheckpointDir(str(tmp_path))
    source = _source(spark)
    result = simplify_coverage(source, 0.3, False)

    assert result.columns == source.columns
    assert result.count() == 2
    assert result.selectExpr("sum(ST_NPoints(geom)) n").first().n == 10
    assert result.selectExpr("min(cast(ST_IsValid(geom) as int)) ok").first().ok == 1
    spark.catalog.clearCache()
    assert result.orderBy("id").collect() == result.orderBy("id").collect()
    assert [r.label for r in result.orderBy("id").collect()] == ["left", "right"]
    assert len(list(tmp_path.glob("*/rdd-*"))) == 1


def test_requires_checkpoint_directory(spark):
    from sedona.spark.geopandas._coverage import simplify_coverage

    context = spark.sparkContext
    previous = context._jsc.sc().checkpointDir()
    getattr(context._jsc.sc(), "checkpointDir_$eq")(context._jvm.scala.Option.empty())
    try:
        with pytest.raises(ValueError, match="checkpoint"):
            simplify_coverage(_source(spark), 0.3, False)
    finally:
        getattr(context._jsc.sc(), "checkpointDir_$eq")(previous)


def test_preserves_null_empty_srid_metadata_and_unrelated_files(spark, tmp_path):
    from sedona.spark.geopandas._coverage import simplify_coverage

    spark.sparkContext.setCheckpointDir(str(tmp_path))
    sentinel = tmp_path / "caller-data"
    sentinel.write_text("keep")
    source = spark.createDataFrame(
        [
            (0, None),
            (1, "POLYGON EMPTY"),
            (2, "MULTIPOLYGON EMPTY"),
            (3, "POLYGON ((0 0, 1 0, 2 0, 2 2, 0 2, 0 0))"),
        ],
        "id long, wkt string",
    ).selectExpr("id", "ST_SetSRID(ST_GeomFromWKT(wkt), 4326) geom")
    source = source.select(
        F.col("id").alias("id", metadata={"description": "physical identifier"}),
        F.col("geom").alias("geom", metadata={"crs": "EPSG:4326"}),
        F.lit("unchanged").alias("extra.column`name", metadata={"note": "keep"}),
    )
    caller_checkpoint = source.checkpoint(eager=True)
    result = simplify_coverage(source, 0.0, True)

    assert result.schema == source.schema
    rows = (
        result.selectExpr("id", "ST_AsText(geom) wkt", "ST_SRID(geom) srid")
        .orderBy("id")
        .collect()
    )
    assert [r.wkt for r in rows[:3]] == [None, "POLYGON EMPTY", "MULTIPOLYGON EMPTY"]
    assert rows[3].srid == 4326
    assert result.where("id = 3").selectExpr("ST_NPoints(geom) n").first().n == 5
    assert sentinel.read_text() == "keep"
    assert len(list(tmp_path.glob("*/rdd-*"))) == 2
    assert caller_checkpoint.count() == 4


@pytest.mark.parametrize(
    "wkt, message",
    [
        ("POINT (0 0)", "valid 2D"),
        ("POLYGON Z ((0 0 1, 2 0 1, 2 2 1, 0 0 1))", "valid 2D"),
        ("POLYGON M ((0 0 1, 2 0 1, 2 2 1, 0 0 1))", "valid 2D"),
        ("POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))", "valid 2D"),
        ("POLYGON ((0 0, 2 0, 2 0, 2 2, 0 2, 0 0))", "repeated"),
        ("POLYGON ((0 0, Infinity 0, 2 2, 0 2, 0 0))", "finite|valid 2D"),
    ],
)
def test_validation_failure_cleans_owned_checkpoints(spark, tmp_path, wkt, message):
    from sedona.spark.geopandas._coverage import simplify_coverage

    spark.sparkContext.setCheckpointDir(str(tmp_path))
    sentinel = tmp_path / "caller-data"
    sentinel.write_text("keep")
    source = spark.createDataFrame([(0, wkt)], "id long, wkt string").selectExpr(
        "id", "ST_GeomFromWKT(wkt) geom"
    )
    with pytest.raises(ValueError, match=message):
        simplify_coverage(source, 0.3, False)
    assert not list(tmp_path.glob("*/rdd-*"))
    assert sentinel.read_text() == "keep"


@pytest.mark.parametrize("ids", [[0, 0], [0, None]])
def test_requires_unique_nonnull_physical_ids(spark, tmp_path, ids):
    from sedona.spark.geopandas._coverage import simplify_coverage

    spark.sparkContext.setCheckpointDir(str(tmp_path))
    source = spark.createDataFrame([(i,) for i in ids], "id long").selectExpr(
        "id", "ST_GeomFromWKT('POLYGON EMPTY') geom"
    )
    with pytest.raises(ValueError, match="unique nonnull"):
        simplify_coverage(source, 0.0, False)
    assert not list(tmp_path.glob("*/rdd-*"))


def test_rejects_connect_with_clear_error(spark, monkeypatch):
    from sedona.spark.geopandas._coverage import simplify_coverage

    source = _source(spark)
    monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "1")
    with pytest.raises(NotImplementedError, match="Spark Classic"):
        simplify_coverage(source, 0.3, False)


@pytest.mark.parametrize("id_type", ["double", "string"])
def test_rejects_non_long_ids_before_checkpoint(spark, tmp_path, id_type):
    from sedona.spark.geopandas._coverage import simplify_coverage

    spark.sparkContext.setCheckpointDir(str(tmp_path))
    source = _source(spark).withColumn("id", F.col("id").cast(id_type))
    with pytest.raises(ValueError, match="LongType"):
        simplify_coverage(source, 0.0, False)
    assert not list(tmp_path.glob("*/rdd-*"))


def test_rejects_oversized_geometry_before_ring_expansion(spark, tmp_path):
    from sedona.spark.geopandas._coverage import simplify_coverage

    spark.sparkContext.setCheckpointDir(str(tmp_path))
    source = spark.range(1).selectExpr(
        "id",
        "ST_MakePolygon(ST_MakeLine(transform(sequence(0, 100000), i -> "
        "CASE WHEN i = 100000 THEN ST_Point(1D, 0D) ELSE "
        "ST_Point(cos(2*pi()*i/100000), sin(2*pi()*i/100000)) END))) geom",
    )
    with pytest.raises(ValueError, match="100000"):
        simplify_coverage(source, 0.0, False)
    assert not list(tmp_path.glob("*/rdd-*"))


@pytest.mark.parametrize(
    "wkt, x, y",
    [
        (
            "POLYGON ((0 0,100000000 100000001,100000000 100000003,100000001 100000002,0 0))",
            0,
            0,
        ),
        (
            "POLYGON ((0 0,100000000 100000001,100000001 100000002,0 10,0 0))",
            100000000,
            100000001,
        ),
    ],
)
def test_rounded_area_never_admits_noncollinear_edit(spark, wkt, x, y):
    from sedona.spark.geopandas._coverage import _candidates, _extract, _segments

    source = spark.createDataFrame([(0, wkt)], "id long, wkt string").selectExpr(
        "id", "ST_GeomFromWKT(wkt) geom"
    )
    occurrences = _extract(source)
    candidates = _candidates(occurrences, _segments(occurrences), 0.5, True)
    # The exact triangle area is 0.5, greater than tolerance squared (0.25).
    # A rounded determinant can incorrectly make that area zero.
    assert candidates.where((F.col("x") == x) & (F.col("y") == y)).count() == 0
