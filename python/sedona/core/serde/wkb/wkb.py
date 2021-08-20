
class WkbSerde(GeometrySerde):

    def geometry_from_bytes(self, bin_parser: BinaryParser) -> BaseGeometry:
        geom_length = bin_parser.read_int(ByteOrderType.BIG_ENDIAN)
        geom = bin_parser.read_geometry(geom_length)
        return geom

    def to_bytes(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        buffer.put_int(1, ByteOrderType.BIG_ENDIAN)
        geom_bytes = dumps(geom, srid=4326)
        buffer.put_int(len(geom_bytes), ByteOrderType.BIG_ENDIAN)
        return [*buffer.byte_array, *geom_bytes, *[0]]