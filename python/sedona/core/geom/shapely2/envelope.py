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

from shapely.geometry import Polygon, box
from shapely.geometry.base import BaseGeometry

from sedona.utils.decorators import require


class Envelope(Polygon):

    __slots__ = []

    def __new__(cls, minx=0, maxx=1, miny=0, maxy=1):
        polygon = box(minx, miny, maxx, maxy)
        polygon.__class__ = cls
        return polygon

    @property
    def minx(self):
        return self.bounds[0]

    @property
    def miny(self):
        return self.bounds[1]

    @property
    def maxx(self):
        return self.bounds[2]

    @property
    def maxy(self):
        return self.bounds[3]

    def isClose(self, a, b) -> bool:
        return math.isclose(a, b, rel_tol=1e-9)

    def __eq__(self, other) -> bool:
        minx, miny, maxx, maxy = self.bounds
        other_minx, other_miny, other_maxx, other_maxy = other.bounds
        return (
            self.isClose(minx, other_minx)
            and self.isClose(miny, other_miny)
            and self.isClose(maxx, other_maxx)
            and self.isClose(maxy, other_maxy)
        )

    @require(["Envelope"])
    def create_jvm_instance(self, jvm):
        minx, miny, maxx, maxy = self.bounds
        return jvm.Envelope(minx, maxx, miny, maxy)

    @classmethod
    def from_jvm_instance(cls, java_obj):
        return cls(
            minx=java_obj.getMinX(),
            maxx=java_obj.getMaxX(),
            miny=java_obj.getMinY(),
            maxy=java_obj.getMaxY(),
        )

    def to_bytes(self):
        from sedona.utils.binary_parser import BinaryBuffer

        minx, miny, maxx, maxy = self.bounds
        bin_buffer = BinaryBuffer()
        bin_buffer.put_double(minx)
        bin_buffer.put_double(maxx)
        bin_buffer.put_double(miny)
        bin_buffer.put_double(maxy)
        return bin_buffer.byte_array

    @classmethod
    def from_shapely_geom(cls, geometry: BaseGeometry):
        minx, miny, maxx, maxy = geometry.bounds
        return cls(minx, maxx, miny, maxy)

    def __repr__(self):
        minx, miny, maxx, maxy = self.bounds
        return f"Envelope({minx}, {maxx}, {miny}, {maxy})"

    @classmethod
    def serialize_for_java(cls, envelopes):
        tmp_envelopes = []
        for e in envelopes:
            minx, miny, maxx, maxy = e.bounds
            tmp_envelopes.append(TmpEnvelopeForPickle(minx, maxx, miny, maxy))
        return pickle.dumps(tmp_envelopes)


class TmpEnvelopeForPickle:
    """Temporary envelope object for generating pickled results compatible with
    shapely1.envelope. We are defining a separated class because we cannot
    implement __setstate__ in Envelope class, since Shapely 2 geometries are
    immutable.

    """

    def __init__(self, minx, maxx, miny, maxy):
        self.minx = minx
        self.maxx = maxx
        self.miny = miny
        self.maxy = maxy

    def __getstate__(self):
        return dict(minx=self.minx, maxx=self.maxx, miny=self.miny, maxy=self.maxy)

    def __setstate__(self, state):
        minx = state.get("minx", 0)
        maxx = state.get("maxx", 1)
        miny = state.get("miny", 0)
        maxy = state.get("maxy", 1)
        self.__init__(minx, maxx, miny, maxy)
