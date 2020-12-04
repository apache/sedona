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

from shapely.geometry import Polygon, Point, LineString, MultiPoint, MultiPolygon, MultiLineString
from shapely.geometry.base import BaseGeometry

from sedona.core.geom.envelope import Envelope


class Circle(Polygon):

    def __init__(self, centerGeometry: BaseGeometry, givenRadius: float):
        self.MBR = None
        self.centerGeometry = centerGeometry
        self.radius = givenRadius
        center_geometry_mbr = Envelope.from_shapely_geom(self.centerGeometry)
        self.centerPoint = self.centerPoint = Point(
            (center_geometry_mbr.minx + center_geometry_mbr.maxx) / 2.0,
            (center_geometry_mbr.miny + center_geometry_mbr.maxy) / 2.0
        )

        width = center_geometry_mbr.maxx - center_geometry_mbr.minx
        length = center_geometry_mbr.maxy - center_geometry_mbr.miny

        center_geometry_internal_radius = sqrt(width ** 2 + length ** 2) / 2.0
        self.radius = givenRadius if givenRadius > center_geometry_internal_radius else center_geometry_internal_radius
        self.MBR = Envelope(
            self.centerPoint.x - self.radius,
            self.centerPoint.x + self.radius,
            self.centerPoint.y - self.radius,
            self.centerPoint.y + self.radius
        )
        super().__init__(self.centerPoint.buffer(self.radius))

    def getCenterGeometry(self) -> BaseGeometry:
        return self.centerGeometry

    def getCenterPoint(self):
        return self.centerPoint

    def getRadius(self) -> float:
        return self.radius

    def setRadius(self, givenRadius: float):
        center_geometry_mbr = Envelope.from_shapely_geom(self.centerGeometry)
        width = center_geometry_mbr.maxx - center_geometry_mbr.minx
        length = center_geometry_mbr.maxy - center_geometry_mbr.miny
        center_geometry_internal_radius = sqrt(width ** 2 + length ** 2) / 2
        self.radius = givenRadius if givenRadius > center_geometry_internal_radius else center_geometry_internal_radius
        self.MBR = Envelope(
            self.centerPoint.x - self.radius,
            self.centerPoint.x + self.radius,
            self.centerPoint.y - self.radius,
            self.centerPoint.y + self.radius
        )

    def covers(self, other: BaseGeometry) -> bool:
        if isinstance(other, Point):
            return self.covers_point(other)
        elif isinstance(other, LineString):
            return self.covers_linestring(other)
        elif isinstance(other, Polygon):
            return self.covers_linestring(other.exterior)
        elif isinstance(other, MultiPoint):
            return all([self.covers_point(point) for point in other])
        elif isinstance(other, MultiPolygon):
            return all([self.covers_linestring(polygon.exterior) for polygon in other.geoms])
        elif isinstance(other, MultiLineString):
            return all([self.covers_linestring(linestring) for linestring in other.geoms])
        else:
            raise TypeError("Not supported")

    def covers_point(self, point: Point):
        delta_x = point.x - self.centerPoint.x
        delta_y = point.y - self.centerPoint.y

        return (delta_x * delta_x + delta_y * delta_y) <= self.radius * self.radius

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
        return self.MBR is None

    def _compute_envelope_internal(self):
        if self.is_empty:
            return Envelope()
        return self.MBR

    def __str__(self):
        return "Circle of radius " + str(self.radius) + " around " + str(self.centerGeometry)

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
