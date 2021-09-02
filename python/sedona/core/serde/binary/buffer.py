import struct

from sedona.core.serde.binary.order import ByteOrderType


class BinaryBuffer:

    def __init__(self):
        self.array = []

    def put_double(self, value, byte_order=ByteOrderType.BIG_ENDIAN):
        bytes = self.__pack("d", value, byte_order)
        self.__extend_buffer(bytes)

    def put_int(self, value, byte_order=ByteOrderType.BIG_ENDIAN):
        bytes = self.__pack("i", value, byte_order)
        self.__extend_buffer(bytes)

    def put_byte(self, value, byte_order=ByteOrderType.BIG_ENDIAN):
        bytes = self.__pack("b", value, byte_order)
        self.__extend_buffer(bytes)

    def put(self, value):
        self.__extend_buffer(value)

    def __pack(self, type, value, byte_order=ByteOrderType.BIG_ENDIAN):
        return struct.pack(byte_order.value + type, value)

    def __extend_buffer(self, bytes):
        self.array.extend(list(bytes))

    def __translate_values(self, values):
        return [el if el < 128 else el - 256 for el in values]

    def add_empty_bytes(self, tp: str, number_of_empty: int):
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