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

# The test file is to ensure compatibility with the path structure and imports with Apache Sedona < 1.8.0
# We will drop this test file in the future when we remove path compatibility for Apache Sedona < 1.8.0
from tests.test_base import TestBase


class TestPathCompatibility(TestBase):

    def test_spatial_rdd_imports(self):
        from sedona.core.SpatialRDD import CircleRDD, PolygonRDD, PointRDD

    def test_enums_imports(self):
        from sedona.core.enums import FileDataSplitter, GridType, IndexType

    def test_geometry_imports(self):
        from sedona.core.geom.circle import Circle
        from sedona.core.geom.envelope import Envelope
        from sedona.core.geom.geography import Geography

    def test_sql_type_imports(self):
        from sedona.sql.types import GeographyType, GeometryType, RasterType

    def test_spatial_operators_imports(self):
        from sedona.core.spatialOperator import JoinQuery
        from sedona.core.spatialOperator import JoinQueryRaw, KNNQuery, RangeQuery

    def test_stac_imports(self):
        from sedona.stac.client import Client
        from sedona.stac.collection_client import CollectionClient

    def test_stats_imports(self):
        from sedona.stats.hotspot_detection.getis_ord import g_local
        from sedona.stats.weighting import (
            add_distance_band_column,
            add_binary_distance_band_column,
        )

    def test_util_imports(self):
        from sedona.utils.adapter import Adapter
        from sedona.utils.spatial_rdd_parser import GeoData

    def test_format_mapper_imports(self):
        from sedona.core.formatMapper.geo_json_reader import GeoJsonReader
        from sedona.core.formatMapper.shapefileParser.shape_file_reader import (
            ShapefileReader,
        )

    def test_sql_module_imports(self):
        from sedona.sql import st_aggregates
        from sedona.sql import st_constructors
        from sedona.sql import st_functions
        from sedona.sql import st_predicates

        # One from each module
        from sedona.sql.st_aggregates import ST_Union_Aggr
        from sedona.sql.st_constructors import ST_MakePoint
        from sedona.sql.st_functions import ST_X
        from sedona.sql.st_predicates import ST_Intersects

    def test_raster_utils_imports(self):
        from sedona.raster_utils.SedonaUtils import SedonaUtils

    def test_import_df_functions_from_sedona_sql(self):
        # one from each module
        from sedona.sql import ST_MakePoint, ST_Y, ST_Touches, ST_Envelope_Aggr

    def test_geoarrow_imports(self):
        from sedona.geoarrow import create_spatial_dataframe, dataframe_to_arrow

    def test_sedona_util_imports(self):
        from sedona.utils import KryoSerializer
        from sedona.utils import SedonaKryoRegistrator

    def test_maps_imports(self):
        # Test Map imports
        from sedona.maps import SedonaKepler, SedonaPyDeck

    def test_raster_imports(self):

        from sedona.raster import awt_raster
        from sedona.raster.awt_raster import AWTRaster
        from sedona.raster.data_buffer import DataBuffer
        from sedona.raster.meta import SampleDimension
