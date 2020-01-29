import sys

from pyspark.sql import SparkSession

from geo_pyspark.register import upload_jars, GeoSparkRegistrator
from geo_pyspark.utils import KryoSerializer, GeoSparkKryoRegistrator
from geo_pyspark.utils.decorators import classproperty


class TestBase:

    @classproperty
    def spark(self):
        if not hasattr(self, "__spark"):
            sys.path.append("/home/pawel/Desktop/spark-2.4.4-bin-hadoop2.7/python")
            sys.path.append("/home/pawel/Desktop/spark-2.4.4-bin-hadoop2.7/python/py4j-0.10.7")
            upload_jars()

            spark = SparkSession. \
                builder. \
                config("spark.serializer", KryoSerializer.getName).\
                config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) .\
                master("local[*]").\
                getOrCreate()

            GeoSparkRegistrator.registerAll(spark)

            setattr(self, "__spark", spark)
        return getattr(self, "__spark")

    @classproperty
    def sc(self):
        if not hasattr(self, "__spark"):
            setattr(self, "__sc", self.spark._sc)
        return getattr(self, "__sc")

