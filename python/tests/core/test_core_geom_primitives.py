from geo_pyspark.core.geom_types import JvmCoordinate, JvmPoint, Envelope
from tests.test_base import TestBase


class TestGeomPrimitives(TestBase):

    def test_jvm_point(self):
        coordinate = JvmCoordinate(self.spark._jvm, 1.0, 1.0).jvm_instance
        jvm_point = JvmPoint(self.spark._jvm, coordinate).jvm_instance
        assert jvm_point.toString() == "POINT (1 1)"

    def test_jvm_envelope(self):
        envelope = Envelope(0.0, 5.0, 0.0, 5.0)
        jvm_instance = envelope.create_jvm_instance(self.spark.sparkContext._jvm)
        envelope_area = jvm_instance.getArea()
        assert envelope_area == 25.0, f"Expected area to be equal 25 but {envelope_area} was found"

    def test_jvm_coordinates(self):
        coordinate = JvmCoordinate(self.spark._jvm, 1.0, 1.0).jvm_instance
        assert coordinate.toString() == "(1.0, 1.0, NaN)", "Coordinate should has (1.0, 1.0, NaN) as string rep"
