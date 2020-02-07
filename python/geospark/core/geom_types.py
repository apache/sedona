import attr

from geospark.core.jvm.abstract import JvmObject


@attr.s
class JvmCoordinate(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    def _create_jvm_instance(self):
        return self.jvm.CoordinateFactory.createCoordinates(self.x, self.y)


@attr.s
class JvmPoint(JvmObject):
    coordinate = attr.ib(type=JvmCoordinate)

    def _create_jvm_instance(self):

        return self.jvm.GeomFactory.createPoint(self.coordinate)
