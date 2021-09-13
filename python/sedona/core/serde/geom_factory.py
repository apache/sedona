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

from shapely.geometry.base import BaseGeometry

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.shape import ShapeSerde
from sedona.core.serde.wkb.wkb import WkbSerde

geometry_serializers = {
    0: ShapeSerde(),
    1: WkbSerde()
}

geometry_serializers_instances = {
    "shape": ShapeSerde(),
    "wkb": WkbSerde()
}

serializers = {
    "shape": 0,
    "wkb": 1
}


class SerializationFactory:

    def __init__(self, geometry_serde: 'GeometrySerde'):
        self.geometry_serde = geometry_serde

    def serialize(self, geom: BaseGeometry, binary_buffer: BinaryBuffer) -> List[int]:
        return self.geometry_serde.to_bytes(geom, binary_buffer)

    def deserialize(self, bytes: BinaryParser) -> BaseGeometry:
        return self.geometry_serde.geometry_from_bytes(bytes)
