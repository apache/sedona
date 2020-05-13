from geospark.core.geom.envelope import Envelope
from tests.test_base import TestBase


class TestGeomPrimitives(TestBase):

    def test_jvm_envelope(self):
        envelope = Envelope(0.0, 5.0, 0.0, 5.0)
        jvm_instance = envelope.create_jvm_instance(self.spark.sparkContext._jvm)
        envelope_area = jvm_instance.getArea()
        assert envelope_area == 25.0, f"Expected area to be equal 25 but {envelope_area} was found"
