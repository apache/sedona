import attr
from pyspark import SparkContext

from geospark.core.SpatialRDD import PolygonRDD, PointRDD, LineStringRDD
from geospark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geospark.core.formatMapper.geo_reader import GeoDataReader
from geospark.core.jvm.config import since
from geospark.utils.decorators import require
from geospark.utils.meta import MultipleMeta


@attr.s
class ShapefileReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    @require(["ShapeFileReader"])
    @since("1.2.0")
    def validate_imports(cls):
        return True

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str) -> SpatialRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        ShapefileReader.validate_imports()
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToGeometryRDD(
            jsc,
            inputPath
        )
        spatial_rdd = SpatialRDD(sc=sc)

        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToPolygonRDD(cls, sc: SparkContext, inputPath: str) -> PolygonRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        ShapefileReader.validate_imports()
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToPolygonRDD(
            jsc,
            inputPath
        )
        spatial_rdd = PolygonRDD()
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToPointRDD(cls, sc: SparkContext, inputPath: str) -> PointRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        ShapefileReader.validate_imports()
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToPointRDD(
            jsc,
            inputPath
        )
        spatial_rdd = PointRDD()
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToLineStringRDD(cls, sc: SparkContext, inputPath: str) -> LineStringRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        ShapefileReader.validate_imports()
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToLineStringRDD(
            jsc,
            inputPath
        )
        spatial_rdd = LineStringRDD()
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd
