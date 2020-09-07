import attr
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

from geospark.register.java_libs import GeoSparkLib
from geospark.utils.prep import assign_all

assign_all()
jvm_import = str


@attr.s
class GeoSparkRegistrator:

    @classmethod
    def registerAll(cls, spark: SparkSession) -> bool:
        """
        This is the core of whole package, It uses py4j to run wrapper which takes existing SparkSession
        and register all User Defined Functions by GeoSpark developers, for this SparkSession.

        :param spark: pyspark.sql.SparkSession, spark session instance
        :return: bool, True if registration was correct.
        """
        spark.sql("SELECT 1 as geom").count()
        PackageImporter.import_jvm_lib(spark._jvm)
        cls.register(spark)
        return True

    @classmethod
    def register(cls, spark: SparkSession):
        return spark._jvm.GeoSparkSQLRegistrator.registerAll(spark._jsparkSession)


class PackageImporter:

    @staticmethod
    def import_jvm_lib(jvm) -> bool:
        from geospark.core.utils import ImportedJvmLib
        """
        Imports all the specified methods and functions in jvm
        :param jvm: Jvm gateway from py4j
        :return:
        """
        for lib in GeoSparkLib:
            java_import(jvm, lib.value)
            ImportedJvmLib.import_lib(lib.name)

        return True
