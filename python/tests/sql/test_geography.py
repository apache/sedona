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

# from pyspark.sql.functions import expr
# from pyspark.sql.types import StructType
# from shapely.wkt import loads as wkt_loads
# from sedona.spark.core.geom.geography import Geography
# from sedona.spark.sql.types import GeographyType
from tests.test_base import TestBase


class TestGeography(TestBase):

    def test_deserialize_geography(self):
        """Test serialization and deserialization of Geography objects"""
        # geog_df = self.spark.range(0, 10).withColumn(
        #     "geog", expr("ST_GeogFromWKT(CONCAT('POINT (', id, ' ', id + 1, ')'))")
        # )
        # rows = geog_df.collect()
        # assert len(rows) == 10
        # for row in rows:
        #     id = row["id"]
        #     geog = row["geog"]
        #     assert geog.geometry.wkt == f"POINT ({id} {id + 1})"

    def test_serialize_geography(self):
        wkt = "MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((-10 -10, -20 -20, -20 -10, -10 -10)))"
        # geog = Geography(wkt_loads(wkt))
        # schema = StructType().add("geog", GeographyType())
        # returned_geog = self.spark.createDataFrame([(geog,)], schema).take(1)[0][0]
        # assert geog.geometry.equals(returned_geog.geometry)
