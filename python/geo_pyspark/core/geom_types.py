from math import sqrt

import attr
from shapely.geometry import LineString, Point, Polygon, MultiPoint, MultiPolygon, MultiLineString
from shapely.geometry.base import BaseGeometry

from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.decorators import require


@attr.s
class JvmCoordinate(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    def _create_jvm_instance(self):
        return self.jvm.CoordinateFactory.createCoordinates(self.x, self.y)


@attr.s
class JvmPoint(JvmObject):
    coordinate = attr.ib(type=JvmCoordinate)

    def _create_jvm_instance(self):

        return self.jvm.GeomFactory.createPoint(self.coordinate)


class Envelope(Polygon):

    def __init__(self, minx=0, maxx=1, miny=0, maxy=1):
        self.minx = minx
        self.maxx = maxx
        self.miny = miny
        self.maxy = maxy
        super().__init__([
            [self.minx, self.miny],
            [self.minx, self.maxy],
            [self.maxx, self.maxy],
            [self.maxx, self.miny]
        ])

    @require([GeoSparkLib.Envelope])
    def create_jvm_instance(self, jvm):
        return jvm.Envelope(
            self.minx, self.maxx, self.miny, self.maxy
        )

    @classmethod
    def from_jvm_instance(cls, java_obj):
        return cls(
            minx=java_obj.getMinX(),
            maxx=java_obj.getMaxX(),
            miny=java_obj.getMinY(),
            maxy=java_obj.getMaxY(),
        )

    def to_bytes(self):
        from geo_pyspark.utils.binary_parser import BinaryBuffer
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
        return (self.__class__, (), dict(
            minx=self.minx,
            maxx=self.maxx,
            miny=self.miny,
            maxy=self.maxy,

        ))

    def __getstate__(self):
        return dict(
            minx=self.minx,
            maxx=self.maxx,
            miny=self.miny,
            maxy=self.maxy,

        )

    def __setstate__(self, state):
        self.minx = state.get("minx", 0)
        self.minx = state.get("maxx", 1)
        self.minx = state.get("miny", 0)
        self.minx = state.get("maxy", 1)

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
