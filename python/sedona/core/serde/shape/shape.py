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
from sedona.core.serde.serializer import GeometrySerde
from sedona.core.serde.shape.enums import GeomEnum
from sedona.core.serde.shape.linestring import LineStringParser
from sedona.core.serde.shape.multilinestring import MultiLineStringParser
from sedona.core.serde.shape.multipoint import MultiPointParser
from sedona.core.serde.shape.multipolygon import MultiPolygonParser
from sedona.core.serde.shape.point import PointParser
from sedona.core.serde.shape.polygon import PolygonParser
from sedona.core.serde.shape.polyline import PolyLineParser
from sedona.core.serde.shape.undefined import UndefinedParser
from sedona.sql.exceptions import GeometryUnavailableException
from sedona.utils.abstract_parser import GeometryParser
from sedona.utils.decorators import classproperty


class ShapeSerde(GeometrySerde):

    byte_number = 0

    def deserialize(self, bin_parser: BinaryParser) -> BaseGeometry:
        gm_type = bin_parser.read_byte()
        if GeomEnum.has_value(gm_type):
            name = GeomEnum.get_name(gm_type)
            parser: GeometryParser = self.parsers[name]
            geom = parser.deserialize(bin_parser)
            return geom
        else:
            raise GeometryUnavailableException(f"Can not deserialize object")

    def serialize(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        buffer.put_byte(self.byte_number)
        geom_name = str(geom.__class__.__name__).lower()
        try:
            appr_parser = self.parsers[geom_name]
        except KeyError:
            raise KeyError(f"Parser for geometry {geom_name}")
        return appr_parser.serialize(geom, buffer)

    @classproperty
    def parsers(self):
        geom_cls = dict(
            undefined=UndefinedParser,
            point=PointParser,
            polyline=PolyLineParser,
            multilinestring=MultiLineStringParser,
            linestring=LineStringParser,
            polygon=PolygonParser,
            multipoint=MultiPointParser,
            multipolygon=MultiPolygonParser
        )
        return geom_cls
