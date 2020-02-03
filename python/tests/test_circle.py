import pytest
from shapely import wkt
from shapely.geometry import Point

from geo_pyspark.core.geom.circle import Circle
from geo_pyspark.core.geom.envelope import Envelope


class TestCircle:

    def test_get_center(self):
        point = Point(0.0, 0.0)
        circle = Circle(point, 0.1)
        assert circle.centerGeometry.x == point.x and circle.centerGeometry.y == point.y

    def test_get_radius(self):
        point = Point(0.0, 0.0)
        circle = Circle(point, 0.1)
        assert circle.getRadius() == pytest.approx(0.1, 0.01)

    def test_set_radius(self):
        point = Point(0.0, 0.0)
        circle = Circle(point, 0.1)
        circle.setRadius(0.1)
        assert circle.getRadius() == pytest.approx(0.1, 0.01)

    def test_get_envelope_internal(self):
        point = Point(0.0, 0.0)
        circle = Circle(point, 0.1)
        assert Envelope(-0.1, 0.1, -0.1, 0.1) == circle.getEnvelopeInternal()

    def test_covers(self):
        circle = Circle(Point(0.0, 0.0), 0.5)

        assert circle.covers(Point(0.0, 0.0))

        assert circle.covers(Point(0.1, 0.2))

        assert not circle.covers(Point(0.4, 0.4))
        assert not circle.covers(Point(-1, 0.4))

        assert circle.covers(wkt.loads("MULTIPOINT ((0.1 0.1), (0.2 0.4))"))
        assert not circle.covers(wkt.loads("MULTIPOINT ((0.1 0.1), (1.2 0.4))"))
        assert not circle.covers(wkt.loads("MULTIPOINT ((1.1 0.1), (0.2 1.4))"))

        assert circle.covers(wkt.loads("POLYGON ((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1))"))
        assert circle.covers(wkt.loads("POLYGON ((-0.5 0, 0 0.5, 0.5 0, -0.5 0))"))
        assert not circle.covers(wkt.loads("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"))
        assert not circle.covers(wkt.loads("POLYGON ((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4))"))

        assert circle.covers(
            wkt.loads("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)),((-0.5 0, 0 0.5, 0.5 0, -0.5 0)))")
        )

        assert not circle.covers(
            wkt.loads("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)),((0 0, 0 1, 1 1, 1 0, 0 0)))")
        )
        assert not circle.covers(
            wkt.loads("MULTIPOLYGON (((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4)),((0 0, 0 1, 1 1, 1 0, 0 0)))")
        )

        assert circle.covers(wkt.loads("LINESTRING (-0.1 0, 0.2 0.3)"))

        assert circle.covers(wkt.loads("LINESTRING (-0.5 0, 0 0.5, 0.5 0)"))

        assert not circle.covers(wkt.loads("LINESTRING (-0.1 0, 0 1)"))

        assert not circle.covers(wkt.loads("LINESTRING (0.4 0.4, 0.45 0.45)"))

        assert circle.covers(wkt.loads("MULTILINESTRING ((-0.1 0, 0.2 0.3), (-0.5 0, 0 0.5, 0.5 0))"))
        assert not circle.covers(wkt.loads("MULTILINESTRING ((-0.1 0, 0.2 0.3), (-0.1 0, 0 1))"))
        assert not circle.covers(wkt.loads("MULTILINESTRING ((0.4 0.4, 0.45 0.45), (-0.1 0, 0 1))"))

    def test_intersects(self):
        circle = Circle(Point(0.0, 0.0), 0.5)
        assert (circle.intersects(Point(0, 0)))
        assert (circle.intersects(Point(0.1, 0.2)))
        assert not (circle.intersects(Point(0.4, 0.4)))
        assert not (circle.intersects(Point(-1, 0.4)))

        assert circle.intersects(wkt.loads("MULTIPOINT ((0.1 0.1), (0.2 0.4))"))
        assert circle.intersects(wkt.loads("MULTIPOINT ((0.1 0.1), (1.2 0.4))"))
        assert not circle.intersects(wkt.loads("MULTIPOINT ((1.1 0.1), (0.2 1.4))"))

        assert circle.intersects(wkt.loads("POLYGON ((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1))"))
        assert circle.intersects(wkt.loads("POLYGON ((-0.5 0, 0 0.5, 0.5 0, -0.5 0))"))

        assert circle.intersects(wkt.loads("POLYGON ((0 0, 1 1, 1 0, 0 0))"))

        assert circle.intersects(wkt.loads("POLYGON ((-1 -1, -1 1, 1 1, 1.5 0.5, 1 -1, -1 -1))"))

        assert circle.intersects(
            wkt.loads("POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1),(-0.1 -0.1, 0.1 -0.1, 0.1 0.1, -0.1 0.1, -0.1 -0.1))")
        )

        assert not circle.intersects(wkt.loads("POLYGON ((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4))"))
        assert not circle.intersects(wkt.loads("POLYGON ((-1 0, -1 1, 0 1, 0 2, -1 2, -1 0))"))
        assert not circle.intersects(
            wkt.loads("POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1),(-0.6 -0.6, 0.6 -0.6, 0.6 0.6, -0.6 0.6, -0.6 -0.6))")
        )

        assert circle.intersects(
            wkt.loads("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)),((-0.5 0, 0 0.5, 0.5 0, -0.5 0)))")
        )
        assert circle.intersects(
            wkt.loads("MULTIPOLYGON (((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1)), ((-1 0, -1 1, 0 1, 0 2, -1 2, -1 0)))")
        )
        assert not circle.intersects(
            wkt.loads(
               "MULTIPOLYGON (((0.4 0.4, 0.4 0.45, 0.45 0.45, 0.45 0.4, 0.4 0.4)),((-1 0, -1 1, 0 1, 0 2, -1 2, -1 0)))"
            ))

        assert circle.intersects(wkt.loads("LINESTRING (-1 -1, 1 1)"))

        assert circle.intersects(wkt.loads("LINESTRING (-1 0.5, 1 0.5)"))

        assert circle.intersects(wkt.loads("LINESTRING (0 0, 0.1 0.2)"))

        assert not circle.intersects(wkt.loads("LINESTRING (0.4 0.4, 1 1)"))
        assert not circle.intersects(wkt.loads("LINESTRING (-0.4 -0.4, -2 -3.2)"))
        assert not circle.intersects(wkt.loads("LINESTRING (0.1 0.5, 1 0.5)"))

        assert circle.intersects(wkt.loads("MULTILINESTRING ((-1 -1, 1 1), (-1 0.5, 1 0.5))"))
        assert circle.intersects(wkt.loads("MULTILINESTRING ((-1 -1, 1 1), (0.4 0.4, 1 1))"))
        assert not circle.intersects(wkt.loads("MULTILINESTRING ((0.1 0.5, 1 0.5), (0.4 0.4, 1 1))"))

    def test_equality(self):
        assert Circle(Point(-112.574945, 45.987772), 0.01) == Circle(Point(-112.574945, 45.987772), 0.01)

        assert Circle(Point(-112.574945, 45.987772), 0.01) == Circle(Point(-112.574945, 45.987772), 0.01)

    def test_radius(self):
        polygon = wkt.loads(
            "POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1),(-0.6 -0.6, 0.6 -0.6, 0.6 0.6, -0.6 0.6, -0.6 -0.6))"
        )
        circle = Circle(polygon, 1.0)
        pytest.approx(circle.radius, 1.414213, 0.001)
        pytest.approx(circle.MBR.minx, -1.414213, 0.001)
        pytest.approx(circle.MBR.maxx, 1.414213, 0.001)
        pytest.approx(circle.MBR.miny, -1.414213, 0.001)
        pytest.approx(circle.MBR.maxy, 1.414213, 0.001)
