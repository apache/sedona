from typing import Iterable, Tuple, List

from shapely.geometry import Point, LinearRing
from shapely.geometry import Polygon

from sedona.core.serde.shape.enums import ShapeEnum
from sedona.utils.binary_parser import ByteOrderType
from sedona.utils.types import numeric


def read_coordinates(parser: 'BinaryParser', read_scale: int):
    coordinates = []
    for i in range(read_scale):
        coordinates.append(
            (parser.read_double(ByteOrderType.LITTLE_ENDIAN), parser.read_double(ByteOrderType.LITTLE_ENDIAN)))
    return coordinates


def put_coordinates(coordinates: Iterable[Iterable[numeric]], binary_buffer: 'BinaryBuffer'):
    for coordinate in coordinates:
        binary_buffer.put_double(Point(coordinate).x, ByteOrderType.LITTLE_ENDIAN)
        binary_buffer.put_double(Point(coordinate).y, ByteOrderType.LITTLE_ENDIAN)


def add_shape_geometry_metadata(geom_type: int, binary_buffer: 'BinaryBuffer'):
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
    binary_buffer.put_int(offset, ByteOrderType.LITTLE_ENDIAN)
    offset += geom.exterior.coords.__len__()
    for _ in range(num_rings - 1):
        binary_buffer.put_int(offset, ByteOrderType.LITTLE_ENDIAN)
        offset = offset + geom.interiors[_].coords.__len__()

    return offset
