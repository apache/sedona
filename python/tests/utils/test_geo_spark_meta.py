from geo_pyspark.core.jvm.config import compare_versions, GeoSparkMeta
from tests.test_base import TestBase


class TestGeoSparkMeta(TestBase):

    def test_meta(self):
        assert not compare_versions("1.2.0", "1.1.5")
        assert compare_versions("1.3.5", "1.2.0")
        assert not compare_versions("", "1.2.0")
        assert not compare_versions("1.3.5", "")
        GeoSparkMeta.version = "1.2.0"
        assert GeoSparkMeta.version == "1.2.0"
        GeoSparkMeta.version = "1.3.0"
        assert GeoSparkMeta.version == "1.3.0"
