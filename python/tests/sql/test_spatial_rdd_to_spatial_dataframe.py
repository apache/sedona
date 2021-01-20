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

import os

from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sedona.core.SpatialRDD import PointRDD
from sedona.core.enums import FileDataSplitter
from sedona.sql.types import GeometryType
from tests.test_base import TestBase
from shapely.geometry import Point

from tests.tools import tests_resource

point_input_path = os.path.join(tests_resource, "arealm-small.csv")

crs_test_point = os.path.join(tests_resource, "crs-test-point.csv")

offset = 1
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11


class TestSpatialRDDToDataFrame(TestBase):

    def test_list_to_rdd_and_df(self):
        point_data = [
            [Point(21, 52.0), "1", 1],
            [Point(22, 52.0), "2", 2],
            [Point(23.0, 52), "3", 3],
            [Point(23, 54), "4", 4],
            [Point(24.0, 56.0), "5", 5]
        ]
        schema = StructType(
            [
                StructField("geom", GeometryType(), False),
                StructField("id_1", StringType(), False),
                StructField("id_2", IntegerType(), False),
            ]
        )

        rdd_data = self.spark.sparkContext.parallelize(point_data)
        df = self.spark.createDataFrame(rdd_data, schema, verifySchema=False)
        df.show()
        df.printSchema()

    def test_point_rdd(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        raw_spatial_rdd = spatial_rdd.rawSpatialRDD.map(
            lambda x: [x.geom, *x.getUserData().split("\t")]
        )

        self.spark.createDataFrame(raw_spatial_rdd).show()

        schema = StructType(
            [
                StructField("geom", GeometryType()),
                StructField("name", StringType())
            ]
        )

        spatial_rdd_with_schema = self.spark.createDataFrame(
            raw_spatial_rdd, schema, verifySchema=False
        )

        spatial_rdd_with_schema.show()

        assert spatial_rdd_with_schema.take(1)[0][0].wkt == "POINT (32.324142 -88.331492)"

