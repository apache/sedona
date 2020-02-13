from geospark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geospark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geospark.utils.meta import MultipleMeta


class CircleRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self, spatialRDD: SpatialRDD, Radius: float):
        """

        :param spatialRDD: SpatialRDD
        :param Radius: float
        """
        super()._do_init(spatialRDD._sc)
        self._srdd = self._jvm_spatial_rdd(
            spatialRDD._srdd,
            Radius
        )

    def getCenterPointAsSpatialRDD(self) -> 'PointRDD':
        from geospark.core.SpatialRDD import PointRDD
        srdd = self._srdd.getCenterPointAsSpatialRDD()
        point_rdd = PointRDD()
        point_rdd.set_srdd(srdd)
        return point_rdd

    def getCenterPolygonAsSpatialRDD(self) -> 'PolygonRDD':
        from geospark.core.SpatialRDD import PolygonRDD
        srdd = self._srdd.getCenterPolygonAsSpatialRDD()
        polygon_rdd = PolygonRDD()
        polygon_rdd.set_srdd(srdd)
        return polygon_rdd

    def getCenterLineStringRDDAsSpatialRDD(self) -> 'LineStringRDD':
        from geospark.core.SpatialRDD import LineStringRDD
        srdd = self._srdd.getCenterPolygonAsSpatialRDD()
        linestring_rdd = LineStringRDD()
        linestring_rdd.set_srdd(srdd)
        return linestring_rdd

    def getCenterRectangleRDDAsSpatialRDD(self) -> 'RectangleRDD':
        from geospark.core.SpatialRDD import RectangleRDD
        srdd = self._srdd.getCenterLineStringRDDAsSpatialRDD()
        rectangle_rdd = RectangleRDD()
        rectangle_rdd.set_srdd(srdd)
        return rectangle_rdd

    @property
    def _jvm_spatial_rdd(self):
        spatial_factory = SpatialRDDFactory(self._sc)
        return spatial_factory.create_circle_rdd()

    def MinimumBoundingRectangle(self):
        raise NotImplementedError("CircleRDD has not MinimumBoundingRectangle method.")
