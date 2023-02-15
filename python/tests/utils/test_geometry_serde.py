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
import pytest

from pyspark.sql.types import (StructType, StringType)
from sedona.sql.types import GeometryType
from pyspark.sql.functions import expr

from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.wkt import loads as wkt_loads

from tests.test_base import TestBase

class TestGeometrySerde(TestBase):
    @pytest.mark.parametrize("geom", [
        GeometryCollection([Point([10.0, 20.0]), Polygon([(10.0, 10.0), (20.0, 20.0), (20.0, 10.0)])]),
        LineString([(10.0, 20.0), (30.0, 40.0)]),
        LineString([(10.0, 20.0, 30.0), (40.0, 50.0, 60.0)]),
        MultiLineString([[(10.0, 20.0), (30.0, 40.0)], [(50.0, 60.0), (70.0, 80.0)]]),
        MultiLineString([[(10.0, 20.0, 30.0), (40.0, 50.0, 60.0)], [(70.0, 80.0, 90.0), (100.0, 110.0, 120.0)]]),
        MultiPoint([(10.0, 20.0), (30.0, 40.0)]),
        MultiPoint([(10.0, 20.0, 30.0), (40.0, 50.0, 60.0)]),
        MultiPolygon([Polygon([(10.0, 10.0), (20.0, 20.0), (20.0, 10.0), (10.0, 10.0)]), Polygon([(-10.0, -10.0), (-20.0, -20.0), (-20.0, -10.0), (-10.0, -10.0)])]),
        MultiPolygon([Polygon([(10.0, 10.0, 10.0), (20.0, 20.0, 10.0), (20.0, 10.0, 10.0), (10.0, 10.0, 10.0)]), Polygon([(-10.0, -10.0, -10.0), (-20.0, -20.0, -10.0), (-20.0, -10.0, -10.0), (-10.0, -10.0, -10.0)])]),
        Point((10.0, 20.0)),
        Point((10.0, 20.0, 30.0)),
        Polygon([(10.0, 10.0), (20.0, 20.0), (20.0, 10.0), (10.0, 10.0)]),
        Polygon([(10.0, 10.0, 10.0), (20.0, 20.0, 10.0), (20.0, 10.0, 10.0), (10.0, 10.0, 10.0)]),
    ])
    def test_spark_serde(self, geom):
        returned_geom = TestGeometrySerde.spark.createDataFrame([(geom,)], StructType().add("geom", GeometryType())).take(1)[0][0]
        assert geom.equals_exact(returned_geom, 1e-6)

    @pytest.mark.parametrize("wkt", [
        # empty geometries
        'POINT EMPTY',
        'LINESTRING EMPTY',
        'POLYGON EMPTY',
        'MULTIPOINT EMPTY',
        'MULTILINESTRING EMPTY',
        'MULTIPOLYGON EMPTY',
        'GEOMETRYCOLLECTION EMPTY',
        # non-empty geometries
        'POINT (10 20)',
        'POINT (10 20 30)',
        'LINESTRING (10 20, 30 40)',
        'LINESTRING (10 20 30, 40 50 60)',
        'POLYGON ((10 10, 20 20, 20 10, 10 10))',
        'POLYGON ((10 10 10, 20 20 10, 20 10 10, 10 10 10))',
        'POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))',
        # non-empty multi geometries
        'MULTIPOINT ((10 20), (30 40))',
        'MULTIPOINT ((10 20 30), (40 50 60))',
        'MULTILINESTRING ((10 20, 30 40), (50 60, 70 80))',
        'MULTILINESTRING ((10 20 30, 40 50 60), (70 80 90, 100 110 120))',
        'MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((-10 -10, -20 -20, -20 -10, -10 -10)))',
        'MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1)))',
        'GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40))',
        'GEOMETRYCOLLECTION (POINT (10 20 30), LINESTRING (10 20 30, 40 50 60))',
        'GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40), POLYGON ((10 10, 20 20, 20 10, 10 10)))',
        # nested geometry collection
        'GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40)))',
        'GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40)))',
        # multi geometries containing empty geometries
        'MULTIPOINT (EMPTY, (10 20))',
        'MULTIPOINT (EMPTY, EMPTY)',
        'MULTILINESTRING (EMPTY, (10 20, 30 40))',
        'MULTILINESTRING (EMPTY, EMPTY)',
        'MULTIPOLYGON (EMPTY, ((10 10, 20 20, 20 10, 10 10)))',
        'MULTIPOLYGON (EMPTY, EMPTY)',
        'GEOMETRYCOLLECTION (POINT (10 20), POINT EMPTY, LINESTRING (10 20, 30 40))',
        'GEOMETRYCOLLECTION (MULTIPOINT EMPTY, MULTILINESTRING EMPTY, MULTIPOLYGON EMPTY, GEOMETRYCOLLECTION EMPTY)',
    ])
    def test_spark_serde_compatibility_with_scala(self, wkt):
        geom = wkt_loads(wkt)
        schema = StructType().add("geom", GeometryType())
        returned_geom = TestGeometrySerde.spark.createDataFrame([(geom,)], schema).take(1)[0][0]
        assert geom.equals(returned_geom)

        # serialized by python, deserialized by scala
        returned_wkt = TestGeometrySerde.spark.createDataFrame([(geom,)], schema).selectExpr("ST_AsText(geom)").take(1)[0][0]
        assert wkt_loads(returned_wkt).equals(geom)

        # serialized by scala, deserialized by python
        schema = StructType().add("wkt", StringType())
        returned_geom = TestGeometrySerde.spark.createDataFrame([(wkt,)], schema).selectExpr("ST_GeomFromText(wkt)").take(1)[0][0]
        assert geom.equals(returned_geom)

    @pytest.mark.parametrize("wkt", [
        'POINT ZM (1 2 3 4)',
        'LINESTRING ZM (1 2 3 4, 5 6 7 8)',
        'POLYGON ZM ((10 10 10 1, 20 20 10 1, 20 10 10 1, 10 10 10 1))',
        'MULTIPOINT ZM ((10 20 30 1), (40 50 60 1))',
        'MULTILINESTRING ZM ((10 20 30 1, 40 50 60 1), (70 80 90 1, 100 110 120 1))',
        'MULTIPOLYGON ZM (((10 10 10 1, 20 20 10 1, 20 10 10 1, 10 10 10 1)), ' +
        '((0 0 0 1, 0 10 0 1, 10 10 0 1, 10 0 0 1, 0 0 0 1), (1 1 0 1, 1 2 0 1, 2 2 0 1, 2 1 0 1, 1 1 0 1)))',
        'GEOMETRYCOLLECTION (POINT ZM (10 20 30 1), LINESTRING ZM (10 20 30 1, 40 50 60 1))',
    ])
    def test_spark_serde_on_4d_geoms(self, wkt):
        geom = wkt_loads(wkt)
        schema = StructType().add("wkt", StringType())
        returned_geom, n_dims = TestGeometrySerde.spark.createDataFrame([(wkt,)], schema)\
            .selectExpr("ST_GeomFromText(wkt)", "ST_NDims(ST_GeomFromText(wkt))")\
            .take(1)[0]
        assert n_dims == 4
        assert geom.equals(returned_geom)

    @pytest.mark.parametrize("wkt", [
        'POINT M (1 2 3)',
        'LINESTRING M (1 2 3, 5 6 7)',
        'POLYGON M ((10 10 10, 20 20 10, 20 10 10, 10 10 10))',
        'MULTIPOINT M ((10 20 30), (40 50 60))',
        'MULTILINESTRING M ((10 20 30, 40 50 60), (70 80 90, 100 110 120))',
        'MULTIPOLYGON M (((10 10 10, 20 20 10, 20 10 10, 10 10 10)), ' +
        '((0 0 0, 0 10 0, 10 10 0, 10 0 0, 0 0 0), (1 1 0, 1 2 0, 2 2 0, 2 1 0, 1 1 0)))',
        'GEOMETRYCOLLECTION (POINT M (10 20 30), LINESTRING M (10 20 30, 40 50 60))',
        ])
    def test_spark_serde_on_xym_geoms(self, wkt):
        geom = wkt_loads(wkt)
        schema = StructType().add("wkt", StringType())
        returned_geom, n_dims, z_min = TestGeometrySerde.spark.createDataFrame([(wkt,)], schema) \
            .withColumn("geom", expr("ST_GeomFromText(wkt)")) \
            .selectExpr("geom", "ST_NDims(geom)", "ST_ZMin(geom)") \
            .take(1)[0]
        assert n_dims == 3
        assert z_min is None
        assert geom.equals(returned_geom)
