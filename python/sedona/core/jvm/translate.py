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

from typing import List


class JvmSedonaPythonConverter:

    def __init__(self, jvm):
        self._jvm = jvm

    def translate_spatial_rdd_to_python(self, spatial_rdd):
        return self._jvm.PythonConverter.translateSpatialRDDToPython(spatial_rdd)

    def translate_spatial_pair_rdd_to_python(self, spatial_rdd):
        return self._jvm.PythonConverter.translateSpatialPairRDDToPython(spatial_rdd)

    def translate_spatial_pair_rdd_with_list_to_python(self, spatial_rdd):
        return self._jvm.PythonConverter.translateSpatialPairRDDWithListToPython(spatial_rdd)

    def translate_python_rdd_to_java(self, java_rdd):
        return self._jvm.PythonConverter.translatePythonRDDToJava(java_rdd)

    def translate_geometry_seq_to_python(self, geometry_seq) -> List:
        return self._jvm.PythonConverter.translateGeometrySeqToPython(geometry_seq)


class JvmJoinParamsAdapter:

    def __init__(self, jvm):
        self._jvm = jvm

    def adapt(self, useIndex: bool, indexType: str, joinBuildSide: str):
        return self._jvm.createJoinParams(useIndex, indexType, joinBuildSide)


class JvmRawJvmIndexRDDAdapter:

    def __init__(self, jvm):
        self._jvm = jvm

    def setRawIndexRDD(self, spatial_rdd, index_rdd):
        self._jvm.RawJvmIndexAdapter.setRawIndexRDD(spatial_rdd, index_rdd)


class JvmSpatialObjectLoaderAdapter:

    def __init__(self, jvm):
        self._jvm = jvm

    def loadPointSpatialRDD(self, jsc, path):
        return self._jvm.SpatialObjectLoaderAdapter.loadPointSpatialRDD(jsc, path)

    def loadPolygonSpatialRDD(self, jsc, path):
        self._jvm.SpatialObjectLoaderAdapter.loadPolygonSpatialRDD(jsc, path)

    def loadSpatialRDD(self, jsc, path):
        self._jvm.SpatialObjectLoaderAdapter.loadSpatialRDD(jsc, path)

    def loadLineStringSpatialRDD(self, jsc, path):
        self._jvm.SpatialObjectLoaderAdapter(jsc, path)

    def loadIndexRDD(self, jsc, path):
        self._jvm.SpatialObjectLoaderAdapter(jsc, path)


class JvmGeometryAdapter:

    def __init__(self, jvm):
        self._jvm = jvm

    def translate_to_java(self, bytes):
        bytes_data = [el for el in bytes]
        return self._jvm.GeometryAdapter.translateToJava(bytes_data)


class SedonaPythonConverter:

    def __init__(self, jvm):
        self.jvm = jvm

    def translate_spatial_rdd_to_python(self, spatial_rdd):
        return self.jvm.PythonConverter.translateSpatialRDDToPython(spatial_rdd)


class PythonRddToJavaRDDAdapter:

    def __init__(self, jvm):
        self._jvm = jvm

    def deserialize_to_point_raw_rdd(self, java_spatial_rdd):
        return self._jvm.PythonRddToJavaRDDAdapter.deserializeToPointRawRDD(java_spatial_rdd)

    def deserialize_to_polygon_raw_rdd(self, java_spatial_rdd):
        return self._jvm.PythonRddToJavaRDDAdapter.deserializeToPolygonRawRDD(java_spatial_rdd)

    def deserialize_to_linestring_raw_rdd(self, java_spatial_rdd):
        return self._jvm.PythonRddToJavaRDDAdapter.deserializeToLineStringRawRDD(java_spatial_rdd)


class SpatialObjectLoaderAdapter:

    def __init__(self, jvm):
        self._jvm = jvm

    def load_point_spatial_rdd(self, sc, path):
        return self._jvm.SpatialObjectLoaderAdapter.loadPointSpatialRDD(sc, path)

    def load_polygon_spatial_rdd(self, sc, path):
        return self._jvm.SpatialObjectLoaderAdapter.loadPolygonSpatialRDD(sc, path)

    def load_spatial_rdd(self, sc, path):
        return self._jvm.SpatialObjectLoaderAdapter.loadSpatialRDD(sc, path)

    def load_line_string_spatial_rdd(self, sc, path):
        return self._jvm.SpatialObjectLoaderAdapter.loadLineStringSpatialRDD(sc, path)

    def load_index_rdd(self, sc, path):
        return self._jvm.SpatialObjectLoaderAdapter.loadIndexRDD(sc, path)
