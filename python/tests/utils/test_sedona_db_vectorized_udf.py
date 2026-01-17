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

import numpy as np

from sedona.spark.sql.functions import sedona_db_vectorized_udf
from sedona.spark.utils.geometry_serde import to_sedona, from_sedona
from tests.test_base import TestBase
import pyarrow as pa
import shapely
from sedona.sql import GeometryType
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import DoubleType, IntegerType, ByteType
from sedona.spark.sql import ST_X


class TestSedonaDBArrowFunction(TestBase):
    def test_vectorized_udf(self):
        @sedona_db_vectorized_udf(
            return_type=GeometryType(), input_types=[ByteType(), IntegerType()]
        )
        def my_own_function(geom, distance):
            geom_wkb = pa.array(geom.storage.to_array())
            geometry_array = np.asarray(geom_wkb, dtype=object)
            distance = pa.array(distance.to_array())
            geom = from_sedona(geometry_array)
            result_shapely = shapely.centroid(geom)

            return pa.array(to_sedona(result_shapely))

        df = self.spark.createDataFrame(
            [
                (1, "POINT (1 1)"),
                (2, "POINT (2 2)"),
                (3, "POINT (3 3)"),
            ],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        df.select(ST_X(my_own_function(df.wkt, lit(100)).alias("geom"))).show()

    def test_geometry_to_double(self):
        @sedona_db_vectorized_udf(return_type=DoubleType(), input_types=[ByteType()])
        def geometry_to_non_geometry_udf(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geometry_array = np.asarray(geom_wkb, dtype=object)
            geom = from_sedona(geometry_array)

            result_shapely = shapely.get_x(shapely.centroid(geom))

            return pa.array(result_shapely)

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        values = df.select(
            geometry_to_non_geometry_udf(df.wkt).alias("x_coord")
        ).collect()

        values_list = [row["x_coord"] for row in values]

        assert values_list == [1.0, 2.0, 3.0]

    def test_geometry_to_int(self):
        @sedona_db_vectorized_udf(return_type=IntegerType(), input_types=[ByteType()])
        def geometry_to_int(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geometry_array = np.asarray(geom_wkb, dtype=object)

            geom = from_sedona(geometry_array)

            result_shapely = shapely.get_num_points(geom)

            return pa.array(result_shapely)

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        values = df.select(geometry_to_int(df.wkt)).collect()

        values_list = [row[0] for row in values]

        assert values_list == [0, 0, 0]

    def test_geometry_crs_preservation(self):
        @sedona_db_vectorized_udf(return_type=GeometryType(), input_types=[ByteType()])
        def return_same_geometry(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geometry_array = np.asarray(geom_wkb, dtype=object)

            geom = from_sedona(geometry_array)

            return pa.array(to_sedona(geom))

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_SetSRID(ST_GeomFromWKT(wkt), 3857)"))

        result_df = df.select(return_same_geometry(df.wkt).alias("geom"))

        crs_list = (
            result_df.selectExpr("ST_SRID(geom)").rdd.flatMap(lambda x: x).collect()
        )

        assert crs_list == [3857, 3857, 3857]
