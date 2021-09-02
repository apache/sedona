import attr

from sedona.core.serde.binary.order import ByteOrderType


@attr.s
class OffsetsReader:

    @staticmethod
    def read_offsets(parser, num_parts, max_offset):
        offsets = []
        for i in range(num_parts):
            offsets.append(parser.read_int(ByteOrderType.LITTLE_ENDIAN))
        offsets.append(max_offset)
        return offsets
