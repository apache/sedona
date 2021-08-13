from typing import Union, Iterable, Tuple, List

import attr
from shapely.geometry import Point, LinearRing
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from shapely.geometry import LineString
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPoint
from shapely.geometry.base import BaseGeometry

from sedona.sql.enums import ShapeEnum
from sedona.sql.exceptions import InvalidGeometryException
from sedona.sql.geometry import GeomEnum
from sedona.utils.abstract_parser import GeometryParser
from sedona.utils.binary_parser import BinaryParser, BinaryBuffer, ByteOrderType
from sedona.utils.types import numeric


def read_coordinates(parser: BinaryParser, read_scale: int):
    coordinates = []
    for i in range(read_scale):
        coordinates.append((parser.read_double(ByteOrderType.LITTLE_ENDIAN), parser.read_double(ByteOrderType.LITTLE_ENDIAN)))
    return coordinates


def put_coordinates(coordinates: Iterable[Iterable[numeric]], binary_buffer: BinaryBuffer):
    for coordinate in coordinates:
        binary_buffer.put_double(Point(coordinate).x, ByteOrderType.LITTLE_ENDIAN)
        binary_buffer.put_double(Point(coordinate).y, ByteOrderType.LITTLE_ENDIAN)


def add_shape_geometry_metadata(geom_type: int, binary_buffer: BinaryBuffer):
    binary_buffer.put_byte(ShapeEnum.shape.value)
    binary_buffer.put_byte(geom_type)


def reverse_linear_ring(linear_ring: LinearRing, ccw: bool = True) -> List[Tuple[numeric, numeric]]:
    if linear_ring.is_ccw == ccw:
        return linear_ring.coords
    else:
        return list(reversed(linear_ring.coords))


def get_number_of_polygon_points(geom: Polygon) -> int:
    interior_point_num = sum([el.coords.__len__() for el in geom.interiors])
    exterior_num_points = geom.exterior.coords.__len__()
    return interior_point_num + exterior_num_points


def get_number_of_rings(geom: Polygon) -> int:
    return geom.interiors.__len__() + 1


def add_offsets_to_polygon(geom: Polygon, binary_buffer: BinaryBuffer, initial_offset: int) -> int:
    offset = initial_offset
    num_rings = get_number_of_rings(geom)
    binary_buffer.put_int(offset, ByteOrderType.LITTLE_ENDIAN)
    offset += geom.exterior.coords.__len__()
    for _ in range(num_rings - 1):
        binary_buffer.put_int(offset, ByteOrderType.LITTLE_ENDIAN)
        offset = offset + geom.interiors[_].coords.__len__()

    return offset


@attr.s
class OffsetsReader:

    @staticmethod
    def read_offsets(parser, num_parts, max_offset):
        offsets = []
        for i in range(num_parts):
            offsets.append(parser.read_int(ByteOrderType.LITTLE_ENDIAN))
        offsets.append(max_offset)
        return offsets


@attr.s
class PointParser(GeometryParser):
    name = "Point"

    @classmethod
    def serialize(cls, obj: Point, binary_buffer: BinaryBuffer):
        if isinstance(obj, Point):
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.point.value, binary_buffer)
            binary_buffer.put_double(obj.x, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_double(obj.y, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_byte(0)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Point:
        x = parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        y = parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        return Point(x, y)


@attr.s
class UndefinedParser(GeometryParser):
    name = "Undefined"

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError()


@attr.s
class LineStringParser(GeometryParser):
    name = "LineString"

    @classmethod
    def serialize(cls, obj: LineString, binary_buffer: BinaryBuffer):
        if isinstance(obj, LineString):
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.polyline.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)
            num_points = obj.coords.__len__()
            binary_buffer.put_int(1, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)

            put_coordinates(obj.coords, binary_buffer)

            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[LineString, MultiLineString]:
        raise NotImplemented()


@attr.s
class MultiLineStringParser(GeometryParser):
    name = "MultiLineString"

    @classmethod
    def serialize(cls, obj: MultiLineString, binary_buffer: BinaryBuffer):
        if isinstance(obj, MultiLineString):
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.polyline.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)

            num_parts = len(obj.geoms)
            num_points = sum([len(el.coords) for el in obj.geoms])

            binary_buffer.put_int(num_parts, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)

            offset = 0
            for _ in range(num_parts):
                binary_buffer.put_int(offset, ByteOrderType.LITTLE_ENDIAN)
                offset = offset + obj.geoms[_].coords.__len__()

            for geom in obj.geoms:
                put_coordinates(geom.coords, binary_buffer)

            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[LineString, MultiLineString]:
        raise NotImplemented()


@attr.s
class PolyLineParser(GeometryParser):
    name = "Polyline"

    @classmethod
    def serialize(cls, obj: Union[MultiLineString, LineString], binary_buffer: BinaryBuffer):
        raise NotImplementedError("")

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[LineString, MultiLineString]:
        for _ in range(4):
            parser.read_double(ByteOrderType.LITTLE_ENDIAN)

        num_parts = parser.read_int(ByteOrderType.LITTLE_ENDIAN)
        num_points = parser.read_int(ByteOrderType.LITTLE_ENDIAN)

        offsets = OffsetsReader.read_offsets(parser, num_parts, num_points)
        lines = []
        for i in range(num_parts):
            read_scale = offsets[i + 1] - offsets[i]
            coordinate_sequence = read_coordinates(parser, read_scale)
            lines.append(LineString(coordinate_sequence))

        if num_parts == 1:
            line = lines[0]
        elif num_parts > 1:
            line = MultiLineString(lines)
        else:
            raise InvalidGeometryException("Invalid geometry")

        return line


@attr.s
class PolygonParser(GeometryParser):
    name = "Polygon"

    @classmethod
    def serialize(cls, obj: Polygon, binary_buffer: BinaryBuffer):
        if isinstance(obj, Polygon):
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.polygon.value, binary_buffer)

            num_rings = get_number_of_rings(obj)

            num_points = get_number_of_polygon_points(obj)

            binary_buffer.add_empty_bytes("double", 4, ByteOrderType.LITTLE_ENDIAN)

            binary_buffer.put_int(num_rings, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)

            add_offsets_to_polygon(obj, binary_buffer, 0)

            coordinates_exterior = reverse_linear_ring(obj.exterior, False)
            put_coordinates(coordinates_exterior, binary_buffer)

            for ring in obj.interiors:
                coordinates = reverse_linear_ring(ring)
                put_coordinates(coordinates, binary_buffer)

            binary_buffer.put_byte(-127)

        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[Polygon, MultiPolygon]:
        for _ in range(4):
            parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        num_rings = parser.read_int(ByteOrderType.LITTLE_ENDIAN)
        num_points = parser.read_int(ByteOrderType.LITTLE_ENDIAN)
        offsets = OffsetsReader.read_offsets(parser, num_parts=num_rings, max_offset=num_points)
        polygons = []
        holes = []
        shells_ccw = False
        shell = None
        for i in range(num_rings):
            read_scale = offsets[i + 1] - offsets[i]
            cs_ring = read_coordinates(parser, read_scale)
            if (len(cs_ring)) < 3:
                continue

            ring = LinearRing(cs_ring)

            if shell is None:
                shell = ring
                shells_ccw = LinearRing(cs_ring).is_ccw
            elif LinearRing(cs_ring).is_ccw != shells_ccw:
                holes.append(ring)
            else:
                if shell is not None:
                    polygon = Polygon(shell, holes)
                    polygons.append(polygon)
                shell = ring
                holes = []

        if shell is not None:
            geometry = Polygon(shell, holes)
            polygons.append(geometry)

        if polygons.__len__() == 1:
            return polygons[0]

        return MultiPolygon(polygons)


@attr.s
class MultiPolygonParser(GeometryParser):
    name = "MultiPolygon"

    @classmethod
    def serialize(cls, obj: MultiPolygon, binary_buffer: BinaryBuffer):
        if isinstance(obj, MultiPolygon):
            num_polygons = len(obj.geoms)
            num_points = sum([get_number_of_polygon_points(polygon) for polygon in obj.geoms])
            num_rings = sum([get_number_of_rings(polygon) for polygon in obj.geoms])
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.polygon.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_rings, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)

            offset = 0
            for geom in obj.geoms:
                offset = add_offsets_to_polygon(geom, binary_buffer, offset)

            for geom in obj.geoms:
                coordinates_exterior = reverse_linear_ring(geom.exterior, False)
                put_coordinates(coordinates_exterior, binary_buffer)

                for ring in geom.interiors:
                    coordinates = reverse_linear_ring(ring)
                    put_coordinates(coordinates, binary_buffer)

            binary_buffer.put_byte(1)
            binary_buffer.put_byte(3)
            binary_buffer.put_byte(1)
            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> MultiPolygon:
        raise NotImplementedError("For multipolygon, PolygonParser class is used.")


@attr.s
class MultiPointParser(GeometryParser):
    name = "MultiPoint"

    @classmethod
    def serialize(cls, obj: MultiPoint, binary_buffer: BinaryBuffer):
        if isinstance(obj, MultiPoint):
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.multipoint.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(len(obj.geoms), ByteOrderType.LITTLE_ENDIAN)
            for point in obj.geoms:
                binary_buffer.put_double(point.x, ByteOrderType.LITTLE_ENDIAN)
                binary_buffer.put_double(point.y, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> MultiPoint:
        for _ in range(4):
            parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        number_of_points = parser.read_int(ByteOrderType.LITTLE_ENDIAN)

        coordinates = read_coordinates(parser, number_of_points)

        return MultiPoint(coordinates)