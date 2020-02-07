from typing import Union, Iterable, Tuple, List

import attr
from shapely.geometry import Point, LinearRing
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from shapely.geometry import LineString
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPoint
from shapely.geometry.base import BaseGeometry

from geospark.core.geom.circle import Circle
from geospark.sql.enums import ShapeEnum, GeomEnum
from geospark.sql.exceptions import InvalidGeometryException
from geospark.utils.abstract_parser import GeometryParser
from geospark.utils.types import numeric


def read_coordinates(parser: 'BinaryParser', read_scale: int):
    coordinates = []
    for i in range(read_scale):
        coordinates.append((parser.read_double(), parser.read_double()))
    return coordinates


def put_coordinates(coordinates: Iterable[Iterable[numeric]], binary_buffer: 'BinaryBuffer'):
    for coordinate in coordinates:
        binary_buffer.put_double(Point(coordinate).x)
        binary_buffer.put_double(Point(coordinate).y)


def add_shape_geometry_metadata(geom_type: int, binary_buffer: 'BinaryBuffer'):
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


def add_offsets_to_polygon(geom: Polygon, binary_buffer: 'BinaryBuffer', initial_offset: int) -> int:
    offset = initial_offset
    num_rings = get_number_of_rings(geom)
    binary_buffer.put_int(offset)
    offset += geom.exterior.coords.__len__()
    for _ in range(num_rings - 1):
        binary_buffer.put_int(offset)
        offset = offset + geom.interiors[_].coords.__len__()

    return offset


@attr.s
class OffsetsReader:

    @staticmethod
    def read_offsets(parser, num_parts, max_offset):
        offsets = []
        for i in range(num_parts):
            offsets.append(parser.read_int())
        offsets.append(max_offset)
        return offsets


@attr.s
class PointParser(GeometryParser):
    name = "Point"

    @classmethod
    def serialize(cls, obj: Point, binary_buffer: 'BinaryBuffer'):
        if isinstance(obj, Point):
            add_shape_geometry_metadata(GeomEnum.point.value, binary_buffer)
            binary_buffer.put_double(obj.x)
            binary_buffer.put_double(obj.y)
            binary_buffer.put_int(-2130640127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> Point:
        x = parser.read_double()
        y = parser.read_double()
        has_user_data = parser.read_boolean()
        if has_user_data:
            for _ in range(3):
                parser.read_byte()

        return Point(x, y)


@attr.s
class UndefinedParser(GeometryParser):
    name = "Undefined"

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: 'BinaryBuffer'):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> BaseGeometry:
        raise NotImplementedError()


@attr.s
class LineStringParser(GeometryParser):
    name = "LineString"

    @classmethod
    def serialize(cls, obj: LineString, binary_buffer: 'BinaryBuffer'):
        if isinstance(obj, LineString):
            add_shape_geometry_metadata(GeomEnum.polyline.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)
            num_points = obj.coords.__len__()
            binary_buffer.put_int(1)
            binary_buffer.put_int(num_points)
            binary_buffer.put_int(0)

            put_coordinates(obj.coords, binary_buffer)

            binary_buffer.put_int(-2130640127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> Union[LineString, MultiLineString]:
        raise NotImplemented()


@attr.s
class MultiLineStringParser(GeometryParser):
    name = "MultiLineString"

    @classmethod
    def serialize(cls, obj: MultiLineString, binary_buffer: 'BinaryBuffer'):
        if isinstance(obj, MultiLineString):
            add_shape_geometry_metadata(GeomEnum.polyline.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)

            num_parts = len(obj.geoms)
            num_points = sum([len(el.coords) for el in obj.geoms])

            binary_buffer.put_int(num_parts)
            binary_buffer.put_int(num_points)

            offset = 0
            for _ in range(num_parts):
                binary_buffer.put_int(offset)
                offset = offset + obj.geoms[_].coords.__len__()

            for geom in obj.geoms:
                put_coordinates(geom.coords, binary_buffer)

            binary_buffer.put_int(-2130640127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> Union[LineString, MultiLineString]:
        raise NotImplemented()


@attr.s
class PolyLineParser(GeometryParser):
    name = "Polyline"

    @classmethod
    def serialize(cls, obj: Union[MultiLineString, LineString], binary_buffer: 'BinaryBuffer'):
        raise NotImplementedError("")

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> Union[LineString, MultiLineString]:
        for _ in range(4):
            parser.read_double()

        num_parts = parser.read_int()
        num_points = parser.read_int()

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
        has_user_data = parser.read_boolean()
        if has_user_data:
            parser.read_byte()
            parser.read_byte()
            parser.read_byte()

        return line


@attr.s
class PolygonParser(GeometryParser):
    name = "Polygon"

    @classmethod
    def serialize(cls, obj: Polygon, binary_buffer: 'BinaryBuffer'):
        if isinstance(obj, Polygon):
            add_shape_geometry_metadata(GeomEnum.polygon.value, binary_buffer)

            num_rings = get_number_of_rings(obj)

            num_points = get_number_of_polygon_points(obj)

            binary_buffer.add_empty_bytes("double", 4)

            binary_buffer.put_int(num_rings)
            binary_buffer.put_int(num_points)

            add_offsets_to_polygon(obj, binary_buffer, 0)

            coordinates_exterior = reverse_linear_ring(obj.exterior, False)
            put_coordinates(coordinates_exterior, binary_buffer)

            for ring in obj.interiors:
                coordinates = reverse_linear_ring(ring)
                put_coordinates(coordinates, binary_buffer)

            binary_buffer.put_int(-2130640127)

        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> Union[Polygon, MultiPolygon]:
        for _ in range(4):
            parser.read_double()
        num_rings = parser.read_int()
        num_points = parser.read_int()
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

        has_user_data = parser.read_boolean()
        if has_user_data:
            parser.read_byte()
            parser.read_byte()
            parser.read_byte()

        if polygons.__len__() == 1:
            return polygons[0]

        return MultiPolygon(polygons)


@attr.s
class MultiPolygonParser(GeometryParser):
    name = "MultiPolygon"

    @classmethod
    def serialize(cls, obj: MultiPolygon, binary_buffer: 'BinaryBuffer'):
        if isinstance(obj, MultiPolygon):
            num_polygons = len(obj.geoms)
            num_points = sum([get_number_of_polygon_points(polygon) for polygon in obj.geoms])
            num_rings = sum([get_number_of_rings(polygon) for polygon in obj.geoms])

            add_shape_geometry_metadata(GeomEnum.polygon.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)
            binary_buffer.put_int(num_rings)
            binary_buffer.put_int(num_points)

            offset = 0
            for geom in obj.geoms:
                offset = add_offsets_to_polygon(geom, binary_buffer, offset)

            for geom in obj.geoms:
                coordinates_exterior = reverse_linear_ring(geom.exterior, False)
                put_coordinates(coordinates_exterior, binary_buffer)

                for ring in geom.interiors:
                    coordinates = reverse_linear_ring(ring)
                    put_coordinates(coordinates, binary_buffer)

            binary_buffer.put_int(-2130640127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> MultiPolygon:
        raise NotImplementedError("For multipolygon, PolygonParser class is used.")


@attr.s
class MultiPointParser(GeometryParser):
    name = "MultiPoint"

    @classmethod
    def serialize(cls, obj: MultiPoint, binary_buffer: 'BinaryBuffer'):
        if isinstance(obj, MultiPoint):
            add_shape_geometry_metadata(GeomEnum.multipoint.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)
            binary_buffer.put_int(len(obj.geoms))
            for point in obj.geoms:
                binary_buffer.put_double(point.x)
                binary_buffer.put_double(point.y)
            binary_buffer.put_int(-2130640127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: 'BinaryParser') -> MultiPoint:
        for _ in range(4):
            parser.read_double()
        number_of_points = parser.read_int()

        coordinates = read_coordinates(parser, number_of_points)
        has_user_data = parser.read_boolean()
        return MultiPoint(coordinates)


@attr.s
class CircleParser(GeometryParser):
    name = "Circle"

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: 'BinaryBuffer'):
        pass

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser'):
        radius = bin_parser.read_double_reverse()
        primitive_geom_type = bin_parser.read_byte()
        parser = GeomEnum.get_name(primitive_geom_type)
        geom = PARSERS[parser].deserialize(bin_parser)
        return Circle(geom, radius)


PARSERS = dict(
    undefined=UndefinedParser,
    point=PointParser,
    polyline=PolyLineParser,
    multilinestring=MultiLineStringParser,
    linestring=LineStringParser,
    polygon=PolygonParser,
    multipoint=MultiPointParser,
    multipolygon=MultiPolygonParser
)
