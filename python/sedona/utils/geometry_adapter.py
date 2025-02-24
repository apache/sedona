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

from shapely.geometry.base import BaseGeometry

from sedona.core.geom.envelope import Envelope
from sedona.core.jvm.translate import JvmGeometryAdapter
from sedona.utils.spatial_rdd_parser import GeometryFactory


class GeometryAdapter:

    @classmethod
    def create_jvm_geometry_from_base_geometry(cls, jvm, geom: BaseGeometry):
        """
        :param jvm:
        :param geom:
        :return:
        """
        if isinstance(geom, Envelope):
            jvm_geom = geom.create_jvm_instance(jvm)
        else:
            decoded_geom = GeometryFactory.to_bytes(geom)
            jvm_geom = JvmGeometryAdapter(jvm).translate_to_java(decoded_geom)

        return jvm_geom
