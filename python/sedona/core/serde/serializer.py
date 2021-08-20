class GeometrySerde:

    def geometry_from_bytes(self, bin_parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError("child class should implement method geometry from bytes")

    def to_bytes(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        raise NotImplementedError("child class should implement method to bytes")
