from geospark.core.jvm.config import is_greater_or_equal_version, GeoSparkMeta
from tests.test_base import TestBase


class TestGeoSparkMeta(TestBase):

    def test_meta(self):
        assert not is_greater_or_equal_version("1.1.5", "1.2.0")
        assert is_greater_or_equal_version("1.2.0", "1.1.5")
        assert is_greater_or_equal_version("1.3.5", "1.2.0")
        assert not is_greater_or_equal_version("", "1.2.0")
        assert not is_greater_or_equal_version("1.3.5", "")
        GeoSparkMeta.version = "1.2.0"
        assert GeoSparkMeta.version == "1.2.0"
        GeoSparkMeta.version = "1.3.0"
        assert GeoSparkMeta.version == "1.3.0"
