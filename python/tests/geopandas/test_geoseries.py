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

import pandas as pd
import sedona.geopandas as sgpd
from tests.test_base import TestBase
import os
from shapely import wkt


class TestGeoSeries(TestBase):
    def setup_method(self):
        project_root_dir = os.path.realpath(__file__)
        for _ in range(4):
            project_root_dir = os.path.dirname(project_root_dir)
        resourceFolder = os.path.join(
            project_root_dir, "spark/common/src/test/resources/"
        )
        self.mixedWktGeometryInputLocation = resourceFolder + "county_small.tsv"
        self.mixedWkbGeometryInputLocation = resourceFolder + "county_small_wkb.tsv"

    def _load_wkt_df(self, file_path: str) -> sgpd.GeoSeries:
        polygon_wkt_df = pd.read_csv(file_path, delimiter="\t", header=None)

        geometries = [wkt.loads(wkt_str) for wkt_str in polygon_wkt_df[0]]
        geo_series = sgpd.GeoSeries(geometries)
        return geo_series

    def test_area(self):
        geo_series = self._load_wkt_df(self.mixedWktGeometryInputLocation)
        result = geo_series.area.to_pandas()
        assert result.count() > 0

    def test_buffer(self):
        geo_series = self._load_wkt_df(self.mixedWktGeometryInputLocation)
        result = geo_series.buffer(1).to_pandas()
        assert result.count() > 0
