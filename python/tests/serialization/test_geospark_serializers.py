from tests.test_base import TestBase


class TestGeometryConvert(TestBase):

    def test_creating_point(self):
        self.spark.sql("SELECT st_GeomFromWKT('Point(21.0 52.0)')").show()

    def test_spark_config(self):
        kryo_reg = ('spark.kryo.registrator', 'org.datasyslab.geospark.serde.GeoSparkKryoRegistrator')
        serializer = ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        spark_config = self.spark.sparkContext._conf.getAll()
        assert kryo_reg in spark_config
        assert serializer in spark_config
