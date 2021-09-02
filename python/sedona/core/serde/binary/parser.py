import struct
from typing import List, Union

import attr
from pyspark import SparkContext
from shapely.wkb import loads

from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.size import BYTE_SIZE, INT_SIZE, size_dict, DOUBLE_SIZE, CHAR_SIZE, BOOLEAN_SIZE


@attr.s
class BinaryParser:
    bytes = attr.ib(type=Union[bytearray, List[int]])
    current_index = attr.ib(default=0)

    def __attrs_post_init__(self):
        no_negatives = self.remove_negatives(self.bytes)
        self.bytes = self._convert_to_binary_array(no_negatives)

    def read_geometry(self, length: int):
        geom_bytes = b"".join([struct.pack("b", el) if el < 128 else struct.pack("b", el - 256) for el in
                               self.bytes[self.current_index: self.current_index + length]])

        geom = loads(geom_bytes)
        self.current_index += length
        return geom

    def read_double(self, byte_order=ByteOrderType.LITTLE_ENDIAN):
        data = self.unpack("d", self.bytes, byte_order)
        self.current_index = self.current_index + DOUBLE_SIZE
        return data

    def read_int(self, byte_order=ByteOrderType.LITTLE_ENDIAN):
        data = self.unpack('i', self.bytes, byte_order)
        self.current_index = self.current_index + INT_SIZE
        return data

    def read_byte(self, byte_order=ByteOrderType.LITTLE_ENDIAN):
        data = self.unpack("b", self.bytes, byte_order)
        self.current_index = self.current_index + BYTE_SIZE
        return data

    def read_char(self, byte_order=ByteOrderType.LITTLE_ENDIAN):
        data = self.unpack("c", self.bytes, byte_order)
        self.current_index = self.current_index + CHAR_SIZE
        return data

    def read_boolean(self, byte_order=ByteOrderType.BIG_ENDIAN):
        data = self.unpack("?", self.bytes, byte_order)
        self.current_index = self.current_index + BOOLEAN_SIZE
        return data

    def read_string(self, length: int, encoding: str = "utf8"):
        string = self.bytes[self.current_index: self.current_index + length]
        self.current_index += length

        try:
            encoded_string = string.decode(encoding, "ignore")
        except UnicodeEncodeError:
            raise UnicodeEncodeError
        return encoded_string

    def read_kryo_string(self, length: int, sc: SparkContext) -> str:
        array_length = length - self.current_index
        byte_array = sc._gateway.new_array(sc._jvm.Byte, array_length)

        for index, bt in enumerate(self.bytes[self.current_index: length]):
            byte_array[index] = self.bytes[self.current_index + index]
        decoded_string = sc._jvm.org.imbruced.geo_pyspark.serializers.GeoSerializerData.deserializeUserData(
            byte_array
        )
        self.current_index = length
        return decoded_string

    def unpack(self, tp: str, bytes: bytearray, byte_order=ByteOrderType.LITTLE_ENDIAN):
        max_index = self.current_index + size_dict[tp]
        bytes = self._convert_to_binary_array(bytes[self.current_index: max_index])
        return struct.unpack(byte_order.value + tp, bytes)[0]

    @classmethod
    def remove_negatives(cls, bytes):
        return [cls.remove_negative(bt) for bt in bytes]

    @classmethod
    def remove_negative(cls, byte):
        bt_pos = byte if byte >= 0 else byte + 256
        return bt_pos

    @staticmethod
    def _convert_to_binary_array(bytes):
        if type(bytes) == list:
            bytes = bytearray(bytes)
        return bytes
