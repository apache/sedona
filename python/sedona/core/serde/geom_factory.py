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
