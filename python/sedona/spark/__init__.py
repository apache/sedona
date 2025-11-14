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

try:
    import pyspark
except ImportError:
    raise ImportError(
        "Apache Sedona requires PySpark. Please install PySpark before using Sedona spark."
    )

from sedona.spark.SedonaContext import SedonaContext
from sedona.spark.core.SpatialRDD import (
    CircleRDD,
    LineStringRDD,
    PointRDD,
    PolygonRDD,
    RectangleRDD,
    SpatialRDD,
)
from sedona.spark.core.enums import FileDataSplitter, GridType, IndexType
from sedona.spark.core.formatMapper import GeoJsonReader, WkbReader, WktReader
from sedona.spark.core.formatMapper.shapefileParser import ShapefileReader
from sedona.spark.core.geom.circle import Circle
from sedona.spark.core.geom.envelope import Envelope
from sedona.spark.core.geom.geography import Geography
from sedona.spark.core.spatialOperator import (
    JoinQuery,
    JoinQueryRaw,
    KNNQuery,
    RangeQuery,
    RangeQueryRaw,
)
from sedona.spark.geoarrow import create_spatial_dataframe, dataframe_to_arrow
from sedona.spark.geoarrow.geoarrow import dataframe_to_arrow
from sedona.spark.maps.SedonaKepler import SedonaKepler
from sedona.spark.maps.SedonaPyDeck import SedonaPyDeck
from sedona.spark.raster_utils.SedonaUtils import SedonaUtils
from sedona.spark.register import SedonaRegistrator
from sedona.spark.sql.functions import sedona_vectorized_udf
from sedona.spark.sql.st_aggregates import *
from sedona.spark.sql.st_constructors import *
from sedona.spark.sql.st_functions import *
from sedona.spark.sql.st_predicates import *
from sedona.spark.sql.types import GeometryType, GeographyType, RasterType
from sedona.spark.stac import Client
from sedona.spark.stac.collection_client import CollectionClient
from sedona.spark.stats.clustering.dbscan import dbscan
from sedona.spark.stats.outlier_detection.local_outlier_factor import (
    local_outlier_factor,
)
from sedona.spark.stats.autocorrelation.moran import MoranResult, Moran
from sedona.spark.stats.hotspot_detection.getis_ord import g_local
from sedona.spark.stats.weighting import add_distance_band_column
from sedona.spark.stats.weighting import add_binary_distance_band_column
from sedona.spark.stats.weighting import add_weighted_distance_band_column
from sedona.spark.utils import KryoSerializer, SedonaKryoRegistrator
from sedona.spark.utils.adapter import Adapter
from sedona.spark.utils.spatial_rdd_parser import GeoData
from sedona.spark.utils.structured_adapter import StructuredAdapter
