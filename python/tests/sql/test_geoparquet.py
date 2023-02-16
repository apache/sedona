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
import os.path

from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads as wkt_loads
import geopandas

from tests.test_base import TestBase
from tests import geoparquet_input_location
from tests import plain_parquet_input_location


class TestGeoParquet(TestBase):
    def test_interoperability_with_geopandas(self, tmp_path):
        test_data = [
            [1, Point(0, 0), LineString([(1, 2), (3, 4), (5, 6)])],
            [2, LineString([(1, 2), (3, 4), (5, 6)]), Point(1, 1)],
            [3, Point(1, 1), LineString([(1, 2), (3, 4), (5, 6)])]
        ]
        df = self.spark.createDataFrame(data=test_data, schema=["id", "g0", "g1"]).repartition(1)
        geoparquet_save_path = os.path.join(tmp_path, "test.parquet")
        df.write.format("geoparquet").save(geoparquet_save_path)

        # Load geoparquet file written by sedona using geopandas
        gdf = geopandas.read_parquet(geoparquet_save_path)
        assert gdf.dtypes['g0'].name == 'geometry'
        assert gdf.dtypes['g1'].name == 'geometry'

        # Load geoparquet file written by geopandas using sedona
        gdf = geopandas.GeoDataFrame([
            {'g': wkt_loads('POINT (1 2)'), 'i': 10},
            {'g': wkt_loads('LINESTRING (1 2, 3 4)'), 'i': 20}
        ], geometry='g')
        geoparquet_save_path2 = os.path.join(tmp_path, "test_2.parquet")
        gdf.to_parquet(geoparquet_save_path2)
        df2 = self.spark.read.format("geoparquet").load(geoparquet_save_path2)
        assert df2.count() == 2
        row = df2.collect()[0]
        assert isinstance(row['g'], BaseGeometry)

    def test_load_geoparquet_with_spatial_filter(self):
        df = self.spark.read.format("geoparquet").load(geoparquet_input_location)\
            .where("ST_Contains(geometry, ST_GeomFromText('POINT (35.174722 -6.552465)'))")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]['name'] == 'Tanzania'

    def test_load_plain_parquet_file(self):
        with pytest.raises(Exception) as excinfo:
            self.spark.read.format("geoparquet").load(plain_parquet_input_location)
        assert "does not contain valid geo metadata" in str(excinfo.value)
