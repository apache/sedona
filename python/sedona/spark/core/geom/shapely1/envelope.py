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

import math
import pickle

from sedona.spark.utils.decorators import require
from shapely.geometry import Point, Polygon
from shapely.geometry.base import BaseGeometry


class Envelope(Polygon):

    def __init__(self, minx=0, maxx=1, miny=0, maxy=1):
        self.minx = minx
        self.maxx = maxx
        self.miny = miny
        self.maxy = maxy
        super().__init__(
            [
                [self.minx, self.miny],
                [self.minx, self.maxy],
                [self.maxx, self.maxy],
                [self.maxx, self.miny],
            ]
        )

    def isClose(self, a, b) -> bool:
        return math.isclose(a, b, rel_tol=1e-9)

    def __eq__(self, other) -> bool:
        return (
            self.isClose(self.minx, other.minx)
            and self.isClose(self.miny, other.miny)
            and self.isClose(self.maxx, other.maxx)
            and self.isClose(self.maxy, other.maxy)
        )

    @require(["Envelope"])
    def create_jvm_instance(self, jvm):
        return jvm.Envelope(self.minx, self.maxx, self.miny, self.maxy)

    @classmethod
    def from_jvm_instance(cls, java_obj):
        return cls(
            minx=java_obj.getMinX(),
            maxx=java_obj.getMaxX(),
            miny=java_obj.getMinY(),
            maxy=java_obj.getMaxY(),
        )

    def to_bytes(self):
        from sedona.spark.utils.binary_parser import BinaryBuffer

        bin_buffer = BinaryBuffer()
        bin_buffer.put_double(self.minx)
        bin_buffer.put_double(self.maxx)
        bin_buffer.put_double(self.miny)
        bin_buffer.put_double(self.maxy)
        return bin_buffer.byte_array

    @classmethod
    def from_shapely_geom(cls, geometry: BaseGeometry):
        if isinstance(geometry, Point):
            return cls(geometry.x, geometry.x, geometry.y, geometry.y)
        else:
            envelope = geometry.envelope
            exteriors = envelope.exterior
            coordinates = list(exteriors.coords)
            x_coord = [coord[0] for coord in coordinates]
            y_coord = [coord[1] for coord in coordinates]

        return cls(min(x_coord), max(x_coord), min(y_coord), max(y_coord))

    def __reduce__(self):
        return (
            self.__class__,
            (),
            dict(
                minx=self.minx,
                maxx=self.maxx,
                miny=self.miny,
                maxy=self.maxy,
            ),
        )

    def __getstate__(self):
        return dict(
            minx=self.minx,
            maxx=self.maxx,
            miny=self.miny,
            maxy=self.maxy,
        )

    def __setstate__(self, state):
        minx = state.get("minx", 0)
        maxx = state.get("maxx", 1)
        miny = state.get("miny", 0)
        maxy = state.get("maxy", 1)
        self.__init__(minx, maxx, miny, maxy)

    @property
    def __array_interface__(self):
        raise NotImplementedError()

    def _get_coords(self):
        raise NotImplementedError()

    def _set_coords(self, ob):
        raise NotImplementedError()

    @property
    def coords(self):
        raise NotImplementedError()

    def __repr__(self):
        return f"Envelope({self.minx}, {self.maxx}, {self.miny}, {self.maxy})"

    @classmethod
    def serialize_for_java(cls, envelopes):
        return pickle.dumps(envelopes)
