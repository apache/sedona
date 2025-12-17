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

from shapely.geometry import Polygon
from tests import csv_point_input_location, union_polygon_input_location
from tests.test_base import TestBase


class TestConstructors(TestBase):

    def test_st_envelope_aggr(self):
        point_csv_df = (
            self.spark.read.format("csv")
            .option("delimiter", ",")
            .option("header", "false")
            .load(csv_point_input_location)
        )

        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable"
        )
        point_df.createOrReplaceTempView("pointdf")
        boundary = self.spark.sql(
            "select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf"
        )

        coordinates = [
            (1.1, 101.1),
            (1.1, 1100.1),
            (1000.1, 1100.1),
            (1000.1, 101.1),
            (1.1, 101.1),
        ]

        polygon = Polygon(coordinates)

        assert boundary.take(1)[0][0] == polygon

    def test_st_union_aggr(self):
        polygon_csv_df = (
            self.spark.read.format("csv")
            .option("delimiter", ",")
            .option("header", "false")
            .load(union_polygon_input_location)
        )

        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable"
        )
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        union = self.spark.sql(
            "select ST_Union_Aggr(polygondf.polygonshape) from polygondf"
        )

        assert union.take(1)[0][0].area == 10100

    def test_st_collect_aggr_points(self):
        self.spark.sql(
            """
            SELECT explode(array(
              ST_GeomFromWKT('POINT(1 2)'),
              ST_GeomFromWKT('POINT(3 4)'),
              ST_GeomFromWKT('POINT(5 6)')
            )) AS geom
            """
        ).createOrReplaceTempView("points_table")

        result = self.spark.sql("SELECT ST_Collect_Agg(geom) FROM points_table").take(
            1
        )[0][0]

        assert result.geom_type == "MultiPoint"
        assert len(result.geoms) == 3

    def test_st_collect_aggr_polygons(self):
        self.spark.sql(
            """
            SELECT explode(array(
              ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
              ST_GeomFromWKT('POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))')
            )) AS geom
            """
        ).createOrReplaceTempView("polygons_table")

        result = self.spark.sql("SELECT ST_Collect_Agg(geom) FROM polygons_table").take(
            1
        )[0][0]

        assert result.geom_type == "MultiPolygon"
        assert len(result.geoms) == 2
        assert result.area == 2.0

    def test_st_collect_aggr_mixed_types(self):
        self.spark.sql(
            """
            SELECT explode(array(
              ST_GeomFromWKT('POINT(1 2)'),
              ST_GeomFromWKT('LINESTRING(0 0, 1 1)'),
              ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')
            )) AS geom
            """
        ).createOrReplaceTempView("mixed_geom_table")

        result = self.spark.sql(
            "SELECT ST_Collect_Agg(geom) FROM mixed_geom_table"
        ).take(1)[0][0]

        assert result.geom_type == "GeometryCollection"
        assert len(result.geoms) == 3

    def test_st_collect_aggr_preserves_duplicates(self):
        # Test that ST_Collect_Agg keeps duplicate geometries (unlike ST_Union_Aggr)
        self.spark.sql(
            """
            SELECT explode(array(
              ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
              ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')
            )) AS geom
            """
        ).createOrReplaceTempView("duplicate_polygons_table")

        result = self.spark.sql(
            "SELECT ST_Collect_Agg(geom) FROM duplicate_polygons_table"
        ).take(1)[0][0]

        # ST_Collect_Agg should preserve both polygons
        assert len(result.geoms) == 2
        # Area should be 2 because it doesn't merge overlapping areas
        assert result.area == 2.0

    # Test aliases for *_Aggr functions with *_Agg suffix
    def test_st_envelope_agg_alias(self):
        point_csv_df = (
            self.spark.read.format("csv")
            .option("delimiter", ",")
            .option("header", "false")
            .load(csv_point_input_location)
        )

        point_csv_df.createOrReplaceTempView("pointtable_alias")
        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable_alias._c0 as Decimal(24,20)), cast(pointtable_alias._c1 as Decimal(24,20))) as arealandmark from pointtable_alias"
        )
        point_df.createOrReplaceTempView("pointdf_alias")
        boundary = self.spark.sql(
            "select ST_Envelope_Agg(pointdf_alias.arealandmark) from pointdf_alias"
        )

        coordinates = [
            (1.1, 101.1),
            (1.1, 1100.1),
            (1000.1, 1100.1),
            (1000.1, 101.1),
            (1.1, 101.1),
        ]

        polygon = Polygon(coordinates)
        assert boundary.take(1)[0][0].equals(polygon)

    def test_st_intersection_agg_alias(self):
        polygon_wkt_df = (
            self.spark.read.format("csv")
            .option("delimiter", "\t")
            .option("header", "false")
            .load(union_polygon_input_location)
        )

        polygon_wkt_df.createOrReplaceTempView("polygontable_alias")
        polygon_df = self.spark.sql(
            "select ST_GeomFromWKT(polygontable_alias._c0) as countyshape from polygontable_alias"
        )
        polygon_df.createOrReplaceTempView("polygondf_alias")
        intersection = self.spark.sql(
            "select ST_Intersection_Agg(polygondf_alias.countyshape) from polygondf_alias"
        )

        result = intersection.take(1)[0][0]
        assert result.area > 0

    def test_st_union_agg_alias(self):
        polygon_wkt_df = (
            self.spark.read.format("csv")
            .option("delimiter", "\t")
            .option("header", "false")
            .load(union_polygon_input_location)
        )

        polygon_wkt_df.createOrReplaceTempView("polygontable_union_alias")
        polygon_df = self.spark.sql(
            "select ST_GeomFromWKT(polygontable_union_alias._c0) as countyshape from polygontable_union_alias"
        )
        polygon_df.createOrReplaceTempView("polygondf_union_alias")
        union = self.spark.sql(
            "select ST_Union_Agg(polygondf_union_alias.countyshape) from polygondf_union_alias"
        )

        result = union.take(1)[0][0]
        assert result.area > 0
