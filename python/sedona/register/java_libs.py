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

from enum import Enum


class SedonaJvmLib(Enum):
    JoinParams = "org.apache.sedona.python.wrapper.adapters.JoinParamsAdapter"
    Adapter = "org.apache.sedona.sql.utils.Adapter"
    JoinQuery = "org.apache.sedona.core.spatialOperator.JoinQuery"
    KNNQuery = "org.apache.sedona.core.spatialOperator.KNNQuery"
    RangeQuery = "org.apache.sedona.core.spatialOperator.RangeQuery"
    Envelope = "org.locationtech.jts.geom.Envelope"
    GeoSerializerData = "org.apache.sedona.python.wrapper.adapters.GeoSparkPythonConverter"
    GeometryAdapter = "org.apache.sedona.python.wrapper.adapters.GeometryAdapter"
    PointRDD = "org.apache.sedona.core.spatialRDD.PointRDD"
    PolygonRDD = "org.apache.sedona.core.spatialRDD.PolygonRDD"
    CircleRDD = "org.apache.sedona.core.spatialRDD.CircleRDD"
    LineStringRDD = "org.apache.sedona.core.spatialRDD.LineStringRDD"
    RectangleRDD = "org.apache.sedona.core.spatialRDD.RectangleRDD"
    SpatialRDD = "org.apache.sedona.core.spatialRDD.SpatialRDD"
    FileDataSplitter = "org.apache.sedona.core.enums.FileDataSplitter"
    GeoJsonReader = "org.apache.sedona.core.formatMapper.GeoJsonReader"
    ShapeFileReader = "org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader"
    SedonaSQLRegistrator = "org.apache.sedona.sql.utils.SedonaSQLRegistrator"
    StorageLevel = "org.apache.spark.storage.StorageLevel"
    GridType = "org.apache.sedona.core.enums.GridType"
    IndexType = "org.apache.sedona.core.enums.IndexType"
    AdapterWrapper = "org.apache.sedona.python.wrapper.utils.PythonAdapterWrapper"
    WktReader = "org.apache.sedona.core.formatMapper.WktReader"
    RawJvmIndexRDDSetter = "org.apache.sedona.python.wrapper.adapters.RawJvmIndexRDDSetter"
    SpatialObjectLoaderAdapter = "org.apache.sedona.python.wrapper.adapters.SpatialObjectLoaderAdapter"
    WkbReader = "org.apache.sedona.core.formatMapper.WkbReader"
    EnvelopeAdapter = "org.apache.sedona.python.wrapper.adapters.EnvelopeAdapter"
    PythonConverter = "org.apache.sedona.python.wrapper.adapters.PythonConverter"
    PythonRddToJavaRDDAdapter = "org.apache.sedona.python.wrapper.adapters.PythonRddToJavaRDDAdapter"
    st_constructors = "org.apache.spark.sql.sedona_sql.expressions.st_constructors"
    st_functions = "org.apache.spark.sql.sedona_sql.expressions.st_functions"
    st_predicates = "org.apache.spark.sql.sedona_sql.expressions.st_predicates"
    st_aggregates = "org.apache.spark.sql.sedona_sql.expressions.st_aggregates"

    @classmethod
    def from_str(cls, geo_lib: str) -> 'SedonaJvmLib':
        try:
            lib = getattr(cls, geo_lib.upper())
        except AttributeError:
            raise AttributeError(f"{cls.__class__.__name__} has no {geo_lib} attribute")
        return lib
