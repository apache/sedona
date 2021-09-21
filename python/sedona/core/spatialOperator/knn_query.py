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

import attr
from shapely.geometry.base import BaseGeometry

from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.jvm.translate import JvmSedonaPythonConverter
from sedona.utils.binary_parser import BinaryParser
from sedona.utils.decorators import require
from sedona.utils.geometry_adapter import GeometryAdapter
from sedona.utils.spatial_rdd_parser import SpatialRDDParserData


@attr.s
class KNNQuery:

    @classmethod
    @require(["KNNQuery", "GeometryAdapter"])
    def SpatialKnnQuery(self, spatialRDD: SpatialRDD, originalQueryPoint: BaseGeometry, k: int, useIndex: bool):
        """

        :param spatialRDD: spatialRDD
        :param originalQueryPoint: shapely.geometry.Point
        :param k: int
        :param useIndex: bool
        :return: pyspark.RDD
        """
        jvm = spatialRDD._jvm
        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(jvm, originalQueryPoint)

        knn_neighbours = jvm.KNNQuery.SpatialKnnQuery(spatialRDD._srdd, jvm_geom, k, useIndex)

        srdd = JvmSedonaPythonConverter(jvm).translate_geometry_seq_to_python(knn_neighbours)

        geoms_data = []
        for arr in srdd:
            binary_parser = BinaryParser(arr)
            geom = SpatialRDDParserData.deserialize(binary_parser)
            geoms_data.append(geom)

        return geoms_data
