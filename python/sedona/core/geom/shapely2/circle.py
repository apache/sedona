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

from math import sqrt

from shapely.geometry import (
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    box,
)
from shapely.geometry.base import BaseGeometry

from sedona.core.geom.envelope import Envelope


class Circle(Polygon):

    __slots__ = []

    def __new__(cls, centerGeometry: BaseGeometry, givenRadius: float):
        minx, miny, maxx, maxy = centerGeometry.bounds
        center_x = (minx + maxx) * 0.5
        center_y = (miny + maxy) * 0.5
        polygon = Point(center_x, center_y).buffer(givenRadius)
        polygon.__class__ = cls
        return polygon

    @property
    def radius(self):
        return self.getRadius()

    @property
    def centerGeometry(self):
        return self.getCenterGeometry()

    @property
    def MBR(self):
        return self.getEnvelopeInternal()

    def getCenterGeometry(self) -> BaseGeometry:
        return self.centroid

    def getCenterPoint(self):
        return self.centroid

    def getRadius(self) -> float:
        minx, miny, maxx, maxy = self.bounds
        return (maxx - minx) * 0.5

    def covers(self, other: BaseGeometry) -> bool:
        if isinstance(other, Point):
            return self.covers_point(other)
        elif isinstance(other, LineString):
            return self.covers_linestring(other)
        elif isinstance(other, Polygon):
            return self.covers_linestring(other.exterior)
        elif isinstance(other, MultiPoint):
            return all([self.covers_point(point) for point in other.geoms])
        elif isinstance(other, MultiPolygon):
            return all(
                [self.covers_linestring(polygon.exterior) for polygon in other.geoms]
            )
        elif isinstance(other, MultiLineString):
            return all(
                [self.covers_linestring(linestring) for linestring in other.geoms]
            )
        else:
            raise TypeError("Not supported")

    def covers_point(self, point: Point):
        p = self.getCenterPoint()
        r = self.getRadius()
        delta_x = point.x - p.x
        delta_y = point.y - p.y
        return (delta_x * delta_x + delta_y * delta_y) <= r * r

    def covers_linestring(self, linestring: LineString):
        for point in linestring.coords:
            if not self.covers_point(Point(*point)):
                return False
        return True

    def intersects(self, other: BaseGeometry):
        return super().intersects(other)

    def getEnvelopeInternal(self):
        return self._compute_envelope_internal()

    @property
    def is_empty(self):
        return self.getRadius() == 0

    def _compute_envelope_internal(self):
        if self.is_empty:
            return Envelope()
        minx, miny, maxx, maxy = self.bounds
        return Envelope(minx, maxx, miny, maxy)

    def __str__(self):
        return (
            "Circle of radius "
            + str(self.getRadius())
            + " around "
            + str(self.centerGeometry)
        )
