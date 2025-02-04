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
import glob
import tempfile

from pyspark.sql import DataFrame

from sedona.core.SpatialRDD import CircleRDD
from sedona.core.enums import GridType
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.utils.structured_adapter import StructuredAdapter
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
