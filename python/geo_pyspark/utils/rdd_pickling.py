from pyspark import PickleSerializer

from geo_pyspark.utils.binary_parser import BinaryParser
from geo_pyspark.utils.spatial_rdd_parser import SpatialRDDParserData, SpatialPairRDDParserData, \
    SpatialRDDParserDataMultipleRightGeom

PARSERS = {
    0: SpatialRDDParserData(),
    1: SpatialRDDParserDataMultipleRightGeom(),
    2: SpatialPairRDDParserData(),
}


class GeoSparkPickler(PickleSerializer):

    def __init__(self):
        super().__init__()

    def loads(self, obj, encoding="bytes"):
        binary_parser = BinaryParser(obj)
        spatial_parser_number = binary_parser.read_int()
        spatial_parser = self.get_parser(spatial_parser_number)
        parsed_row = spatial_parser.deserialize(binary_parser)

        return parsed_row

    def dumps(self, obj):
        raise NotImplementedError()

    def get_parser(self, number: int):
        return PARSERS[number]
