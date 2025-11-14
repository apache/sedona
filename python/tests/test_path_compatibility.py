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

from sedona.core.SpatialRDD import CircleRDD, PolygonRDD, PointRDD
from sedona.core.enums import FileDataSplitter, GridType, IndexType
from sedona.core.formatMapper.geo_json_reader import GeoJsonReader
from sedona.core.formatMapper.shapefileParser.shape_file_reader import ShapefileReader
from sedona.core.geom.circle import Circle
from sedona.core.geom.envelope import Envelope
from sedona.core.geom.geography import Geography
from sedona.core.spatialOperator import JoinQuery
from sedona.core.spatialOperator import JoinQueryRaw, KNNQuery, RangeQuery
from sedona.stats.clustering.dbscan import dbscan
from sedona.stats.outlier_detection.local_outlier_factor import (
    local_outlier_factor,
)
from sedona.stats.hotspot_detection.getis_ord import g_local
from sedona.stats.weighting import add_distance_band_column
from sedona.stats.weighting import add_binary_distance_band_column
from sedona.stats.weighting import add_weighted_distance_band_column

from sedona.sql import st_aggregates as sta
from sedona.sql import st_constructors as stc
from sedona.sql import st_functions as stf
from sedona.sql import st_predicates as stp
from sedona.sql.st_aggregates import ST_Union_Aggr
from sedona.sql.st_functions import ST_X
from sedona.sql.st_predicates import ST_Intersects
from sedona.sql.types import GeographyType, GeometryType, RasterType
from sedona.stac.client import Client
from sedona.stac.collection_client import CollectionClient
from sedona.stats.clustering import dbscan
from sedona.stats.hotspot_detection.getis_ord import g_local
from sedona.stats.weighting import (
    add_distance_band_column,
    add_binary_distance_band_column,
)
from sedona.utils.adapter import Adapter
from sedona.utils.spatial_rdd_parser import GeoData
from sedona.utils.structured_adapter import StructuredAdapter
from tests.test_base import TestBase
from sedona.raster_utils.SedonaUtils import SedonaUtils
from sedona.sql import ST_MakePoint, ST_Y, ST_Touches, ST_Envelope_Aggr
from sedona.geoarrow import create_spatial_dataframe, dataframe_to_arrow
from sedona.utils import KryoSerializer, SedonaKryoRegistrator
from sedona.maps import SedonaKepler, SedonaPyDeck


class TestPathCompatibility(TestBase):

    def test_spatial_rdd_imports(self):
        # Test CircleRDD, PolygonRDD and PointRDD imports
        assert PointRDD is not None
        assert CircleRDD is not None
        assert PolygonRDD is not None

    def test_enums_imports(self):
        # Test FileDataSplitter, GridType, IndexType imports
        assert FileDataSplitter is not None
        assert GridType is not None
        assert IndexType is not None

    def test_geometry_imports(self):
        # Test Envelope, Geography, Circle imports
        assert Envelope is not None
        assert Geography is not None
        assert Circle is not None

    def test_sql_type_imports(self):
        # Test GeographyType and GeometryType imports
        assert GeographyType is not None
        assert GeometryType is not None
        assert RasterType is not None

    def test_spatial_operators_imports(self):
        # Test JoinQuery, KNNQuery, RangeQuery imports
        assert JoinQuery is not None
        assert JoinQueryRaw is not None
        assert KNNQuery is not None
        assert RangeQuery is not None

    def test_stac_imports(self):
        # Test STAC related imports
        assert Client is not None
        assert CollectionClient is not None

    def test_stats_imports(self):
        # Test statistics related imports
        assert dbscan is not None
        assert g_local is not None
        assert add_distance_band_column is not None
        assert add_binary_distance_band_column is not None
        assert add_weighted_distance_band_column is not None
        assert local_outlier_factor is not None

    def test_util_imports(self):
        # Test utility imports
        assert Adapter is not None
        assert GeoData is not None
        assert StructuredAdapter is not None

    def test_format_mapper_imports(self):
        # Test GeoJsonReader and ShapefileReader imports
        assert GeoJsonReader is not None
        assert ShapefileReader is not None

    def test_sql_module_imports(self):
        # Test SQL module imports
        assert sta is not None
        assert stc is not None
        assert stf is not None
        assert stp is not None
        assert ST_MakePoint is not None
        assert ST_X is not None
        assert ST_Union_Aggr is not None
        assert ST_Intersects is not None

    def test_geoarrow_import(self):
        # Test create_spatial_dataframe import
        assert create_spatial_dataframe is not None
        assert dataframe_to_arrow is not None

    def test_raster_utils_imports(self):
        # Test raster utils imports
        assert SedonaUtils is not None

    def test_import_df_functions_from_sedona_sql(self):
        # one from each module
        assert ST_MakePoint is not None
        assert ST_Y is not None
        assert ST_Touches is not None
        assert ST_Envelope_Aggr is not None

    def test_geoarrow_imports(self):
        assert create_spatial_dataframe is not None
        assert dataframe_to_arrow is not None

    def test_sedona_util_imports(self):
        assert KryoSerializer is not None
        assert SedonaKryoRegistrator is not None

    def test_maps_imports(self):
        # Test Map imports
        assert SedonaKepler is not None
        assert SedonaPyDeck is not None
