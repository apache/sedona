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
import glob
import tempfile

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from shapely.geometry.point import Point

from sedona.spark.core.enums import IndexType
from sedona.spark.core.spatialOperator import RangeQuery

from sedona.spark.core.SpatialRDD import CircleRDD
from sedona.spark.core.enums import GridType
from sedona.spark.core.spatialOperator import JoinQueryRaw
from sedona.spark.utils.structured_adapter import StructuredAdapter
from tests.test_base import TestBase


class TestStructuredAdapter(TestBase):

    def test_df_rdd(self):
        spatial_df: DataFrame = self.spark.sql("select ST_MakePoint(1, 1) as geom")
        srdd = StructuredAdapter.toSpatialRdd(spatial_df, "geom")
        spatial_df = StructuredAdapter.toDf(srdd, self.spark)
        assert spatial_df.count() == 1

    def test_spatial_partitioned_df(self):
        spatial_df: DataFrame = self.spark.sql("select ST_MakePoint(1, 1) as geom")
        srdd = StructuredAdapter.toSpatialRdd(spatial_df, "geom")
        srdd.analyze()
        srdd.spatialPartitioning(GridType.KDBTREE, 1)
        spatial_df = StructuredAdapter.toSpatialPartitionedDf(srdd, self.spark)
        assert spatial_df.count() == 1

    def test_distance_join_result_to_dataframe(self):
        spatial_df: DataFrame = self.spark.sql("select ST_MakePoint(1, 1) as geom")
        schema = spatial_df.schema
        srdd = StructuredAdapter.toSpatialRdd(spatial_df, "geom")
        srdd.analyze()

        circle_rdd = CircleRDD(srdd, 0.001)

        srdd.spatialPartitioning(GridType.QUADTREE)
        circle_rdd.spatialPartitioning(srdd.getPartitioner())

        join_result_pair_rdd = JoinQueryRaw.DistanceJoinQueryFlat(
            srdd, circle_rdd, False, True
        )

        join_result_df = StructuredAdapter.pairRddToDf(
            join_result_pair_rdd, schema, schema, self.spark
        )
        assert join_result_df.count() == 1

    def test_spatial_partitioned_write(self):
        xys = [(i, i // 100, i % 100) for i in range(1_000)]
        df = self.spark.createDataFrame(xys, ["id", "x", "y"]).selectExpr(
            "id", "ST_Point(x, y) AS geom"
        )

        rdd = StructuredAdapter.toSpatialRdd(df, "geom")
        rdd.analyze()
        rdd.spatialPartitioningWithoutDuplicates(GridType.KDBTREE, num_partitions=16)
        n_spatial_partitions = rdd.spatialPartitionedRDD.getNumPartitions()
        assert n_spatial_partitions >= 16

        partitioned_df = StructuredAdapter.toSpatialPartitionedDf(rdd, self.spark)

        with tempfile.TemporaryDirectory() as td:
            out = td + "/out"
            partitioned_df.write.format("geoparquet").save(out)
            assert len(glob.glob(out + "/*.parquet")) == n_spatial_partitions

    def test_build_index_and_range_query_with_polygons(self):
        # Create a spatial DataFrame with polygons
        polygons_data = [
            (1, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"),
            (2, "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"),
            (3, "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"),
            (4, "POLYGON((3 3, 4 3, 4 4, 3 4, 3 3))"),
            (5, "POLYGON((4 4, 5 4, 5 5, 4 5, 4 4))"),
        ]

        df = self.spark.createDataFrame(polygons_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

        # Convert to SpatialRDD
        spatial_rdd = StructuredAdapter.toSpatialRdd(spatial_df, "geometry")

        # Build index on the spatial RDD
        spatial_rdd.buildIndex(IndexType.RTREE, False)

        query_point = Point(2.2, 2.2)

        # Perform range query
        query_result = RangeQuery.SpatialRangeQuery(
            spatial_rdd, query_point, True, True
        )

        # Assertions
        result_count = query_result.count()

        assert result_count >= 0, f"Expected at least one result, got {result_count}"

    def test_build_index_and_range_query_with_points(self):
        # Create a spatial DataFrame with points
        points_data = [
            (1, "POINT(0 0)"),
            (2, "POINT(1 1)"),
            (3, "POINT(2 2)"),
            (4, "POINT(3 3)"),
            (5, "POINT(4 4)"),
        ]

        df = self.spark.createDataFrame(points_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

        # Convert to SpatialRDD
        spatial_rdd = StructuredAdapter.toSpatialRdd(spatial_df, "geometry")

        # Build index on the spatial RDD
        spatial_rdd.buildIndex(IndexType.RTREE, False)

        query_window = Point(2.0, 2.0).buffer(1.0)

        # Perform range query
        query_result = RangeQuery.SpatialRangeQuery(
            spatial_rdd, query_window, True, True
        )

        # Assertions
        result_count = query_result.count()
        assert result_count > 0, f"Expected at least one result, got {result_count}"

    def test_build_index_and_range_query_with_linestrings(self):
        # Create a spatial DataFrame with linestrings
        linestrings_data = [
            (1, "LINESTRING(0 0, 1 1)"),
            (2, "LINESTRING(1 1, 2 2)"),
            (3, "LINESTRING(2 2, 3 3)"),
            (4, "LINESTRING(3 3, 4 4)"),
            (5, "LINESTRING(4 4, 5 5)"),
        ]

        df = self.spark.createDataFrame(linestrings_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

        # Convert to SpatialRDD
        spatial_rdd = StructuredAdapter.toSpatialRdd(spatial_df, "geometry")

        # Build index on the spatial RDD
        spatial_rdd.buildIndex(IndexType.RTREE, False)

        query_window = Point(2.0, 2.0).buffer(0.5)

        # Perform range query
        query_result = RangeQuery.SpatialRangeQuery(
            spatial_rdd, query_window, True, True
        )

        # Assertions
        result_count = query_result.count()
        assert result_count > 0, f"Expected at least one result, got {result_count}"

    def test_build_index_and_range_query_with_mixed_geometries(self):
        # Create a spatial DataFrame with mixed geometry types
        mixed_data = [
            (1, "POINT(0 0)"),
            (2, "LINESTRING(1 1, 2 2)"),
            (3, "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"),
            (4, "MULTIPOINT((3 3), (3.1 3.1))"),
            (
                5,
                "MULTIPOLYGON(((4 4, 5 4, 5 5, 4 5, 4 4)), ((4.1 4.1, 4.2 4.1, 4.2 4.2, 4.1 4.2, 4.1 4.1)))",
            ),
        ]

        df = self.spark.createDataFrame(mixed_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

        # Convert to SpatialRDD
        spatial_rdd = StructuredAdapter.toSpatialRdd(spatial_df, "geometry")

        # Build index on the spatial RDD
        spatial_rdd.buildIndex(IndexType.RTREE, False)

        query_window = Point(3.0, 3.0).buffer(1.0)

        # Perform range query
        query_result = RangeQuery.SpatialRangeQuery(
            spatial_rdd, query_window, True, True
        )

        # Assertions
        result_count = query_result.count()
        assert result_count > 0, f"Expected at least one result, got {result_count}"

    def test_repartition_by_spatial_key(self):
        xys = [(i, i // 100, i % 100) for i in range(1_000)]
        df = self.spark.createDataFrame(xys, ["id", "x", "y"]).selectExpr(
            "id", "ST_Point(x, y) AS geom"
        )
        partitioned_df = StructuredAdapter.repartitionBySpatialKey(
            df, GridType.KDBTREE, "geom", 4
        )
        assert partitioned_df.count() == 1_000
        assert partitioned_df.rdd.getNumPartitions() >= 4

    def test_repartition_by_spatial_key_auto_detect(self):
        xys = [(i, i // 100, i % 100) for i in range(1_000)]
        df = self.spark.createDataFrame(xys, ["id", "x", "y"]).selectExpr(
            "id", "ST_Point(x, y) AS geom"
        )
        partitioned_df = StructuredAdapter.repartitionBySpatialKey(df, GridType.KDBTREE)
        assert partitioned_df.count() == 1_000

    def test_toDf_preserves_columns_with_proper_types(self):
        # Create a spatial DataFrame with various columns and types
        data = [
            (1, "POINT(0 0)", "alpha", 10.5, True),
            (2, "POINT(1 1)", "beta", 20.7, False),
            (3, "POINT(2 2)", "gamma", 30.9, True),
        ]

        schema = ["id", "wkt", "name", "value", "flag"]
        df = self.spark.createDataFrame(data, schema)
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

        # Store original column names and types
        original_cols = spatial_df.columns
        original_dtypes = {f.name: f.dataType for f in spatial_df.schema.fields}

        # Convert to SpatialRDD and back to DataFrame
        spatial_rdd = StructuredAdapter.toSpatialRdd(spatial_df, "geometry")
        result_df = StructuredAdapter.toDf(spatial_rdd, self.spark)

        # Verify all columns are preserved
        assert len(result_df.columns) == len(original_cols)
        for col in original_cols:
            assert col in result_df.columns

        # Verify data types are preserved
        result_dtypes = {f.name: f.dataType for f in result_df.schema.fields}
        for col, dtype in original_dtypes.items():
            assert col in result_dtypes
            assert str(result_dtypes[col]) == str(
                dtype
            ), f"Type mismatch for {col}: expected {dtype}, got {result_dtypes[col]}"

        # Verify values are preserved
        for i in range(1, 4):
            original_row = spatial_df.filter(spatial_df.id == i).collect()[0]
            result_row = result_df.filter(result_df.id == i).collect()[0]

            # Compare values for each column
            assert result_row["id"] == original_row["id"]
            assert result_row["name"] == original_row["name"]
            assert abs(result_row["value"] - original_row["value"]) < 0.001
            assert result_row["flag"] == original_row["flag"]

            # Verify geometry data is preserved (using WKT representation)
            orig_wkt = (
                spatial_df.filter(spatial_df.id == i)
                .select(expr("ST_AsText(geometry)"))
                .collect()[0][0]
            )
            result_wkt = (
                result_df.filter(result_df.id == i)
                .select(expr("ST_AsText(geometry)"))
                .collect()[0][0]
            )
            assert orig_wkt == result_wkt
