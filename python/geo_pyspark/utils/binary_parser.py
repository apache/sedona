import struct
from typing import List, Union

import attr

DOUBLE_SIZE = 8
INT_SIZE = 4
BYTE_SIZE = 1

size_dict = {
    "d": DOUBLE_SIZE,
    "i": INT_SIZE,
    "b": BYTE_SIZE
}


@attr.s
class BinaryParser:
    bytes = attr.ib(type=Union[bytearray, List[int]])
    current_index = attr.ib(default=0)

    def __attrs_post_init__(self):
        no_negatives = self.remove_negatives(self.bytes)
        self.bytes = self._convert_to_binary_array(no_negatives)

    def read_double(self):
        data = self.unpack("d", self.bytes)
        self.current_index = self.current_index + DOUBLE_SIZE
        return data

    def read_int(self):
        data = self.unpack("i", self.bytes)
        self.current_index = self.current_index + INT_SIZE
        return data

    def read_byte(self):
        data = self.unpack("b", self.bytes)
        self.current_index = self.current_index + BYTE_SIZE
        return data

    def unpack(self, tp: str, bytes: bytearray):
        max_index = self.current_index + size_dict[tp]
        bytes = self._convert_to_binary_array(bytes[self.current_index: max_index])
        return struct.unpack(tp, bytes)[0]

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


class BinaryBuffer:

    def __init__(self):
        self.array = []

    def put_double(self, value):
        bytes = self.__pack("d", value)
        self.__extend_buffer(bytes)

    def put_int(self, value):
        bytes = self.__pack("i", value)
        self.__extend_buffer(bytes)

    def put_byte(self, value):
        bytes = self.__pack("b", value)
        self.__extend_buffer(bytes)

    def __pack(self, type, value):
        return struct.pack(type, value)

    def __extend_buffer(self, bytes):
        self.array.extend(list(bytes))

    def __translate_values(self, values):
        return [el if el < 128 else el - 256 for el in values]

    def add_empty_bytes(self, tp: str, number_of_empty):
        if tp == "double":
            for _ in range(number_of_empty):
                self.put_double(0.0)
        elif tp == "int":
            for _ in range(number_of_empty):
                self.put_int(0)
        elif tp == "double":
            for _ in range(number_of_empty):
                self.put_byte(0)
        else:
            raise TypeError(f"Passed {tp} is not available")

    @property
    def byte_array(self):
        return self.__translate_values(self.array)
