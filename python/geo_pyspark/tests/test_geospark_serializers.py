from unittest import TestCase

from pyspark.sql import SparkSession

from geo_pyspark.register import GeoSparkRegistrator, upload_jars
from geo_pyspark.utils import GeoSparkKryoRegistrator, KryoSerializer

upload_jars()

spark = SparkSession.builder.\
        master("local[*]").\
        appName("TestApp").\
        config("spark.serializer", KryoSerializer.getName).\
        config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) .\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestGeometryConvert(TestCase):

    def test_creating_point(self):
        spark.sql("SELECT st_GeomFromWKT('Point(21.0 52.0)')").show()

    def test_spark_config(self):
        kryo_reg = ('spark.kryo.registrator', 'org.datasyslab.geospark.serde.GeoSparkKryoRegistrator')
        serializer = ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        spark_config = spark.sparkContext._conf.getAll()
        self.assertEqual(kryo_reg in spark_config, True)
        self.assertEqual(serializer in spark_config, True)
