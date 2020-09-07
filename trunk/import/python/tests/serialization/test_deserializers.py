import os

from shapely.geometry import MultiPoint, Point, MultiLineString, LineString, Polygon, MultiPolygon
import geopandas as gpd

from tests.data import data_path
from tests.test_base import TestBase


class TestGeometryConvert(TestBase):

    def test_register_functions(self):
        df = self.spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.show()

    def test_collect(self):

        df = self.spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.collect()

    def test_loading_from_file_deserialization(self):
        geom = self.spark.read.\
            options(delimiter="|", header=True).\
            csv(os.path.join(data_path, "counties.csv")).\
            limit(1).\
            createOrReplaceTempView("counties")

        geom_area = self.spark.sql("SELECT st_area(st_geomFromWKT(geom)) as area from counties").collect()[0][0]
        polygon_shapely = self.spark.sql("SELECT st_geomFromWKT(geom) from counties").collect()[0][0]
        assert geom_area == polygon_shapely.area

    def test_polygon_with_holes_deserialization(self):
        geom = self.spark.sql(
            """select st_geomFromWKT('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),
(20 30, 35 35, 30 20, 20 30))') as geom"""
        ).collect()[0][0]

        assert geom.area == 675.0
        assert type(geom) == Polygon

    def test_multipolygon_with_holes_deserialization(self):
        geom = self.spark.sql(
            """select st_geomFromWKT('MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),
((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),
(30 20, 20 15, 20 25, 30 20)))')"""
        ).collect()[0][0]
        assert type(geom) == MultiPolygon

        assert geom.area == 712.5

    def test_multipolygon_deserialization(self):
        geom = self.spark.sql(
            """select st_geomFromWKT()"""
        )

    def test_point_deserialization(self):
        geom = self.spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""").collect()[0][0]
        assert geom.wkt == Point(-6.0, 52.0).wkt

    def test_multipoint_deserialization(self):
        geom = self.spark.sql("""select st_geomFromWKT('MULTIPOINT(1 2, -2 3)') as geom""").collect()[0][0]

        assert geom.wkt == MultiPoint([(1, 2), (-2, 3)]).wkt

    def test_linestring_deserialization(self):
        geom = self.spark.sql(
            """select st_geomFromWKT('LINESTRING (30 10, 10 30, 40 40)')"""
        ).collect()[0][0]

        assert type(geom) == LineString

        assert geom.wkt == LineString([(30, 10), (10, 30), (40, 40)]).wkt

    def test_multilinestring_deserialization(self):
        geom = self.spark.sql(
            """SELECT st_geomFromWKT('MULTILINESTRING ((10 10, 20 20, 10 40),
                        (40 40, 30 30, 40 20, 30 10))') as geom"""
        ).collect()[0][0]

        assert type(geom) == MultiLineString
        assert geom.wkt == MultiLineString([
                ((10, 10), (20, 20), (10, 40)),
                ((40, 40), (30, 30), (40, 20), (30, 10))
            ]).wkt

    def test_from_geopandas_convert(self):
        gdf = gpd.read_file(os.path.join(data_path, "gis_osm_pois_free_1.shp"))

        self.spark.createDataFrame(
            gdf
        ).show()

    def test_to_geopandas(self):
        counties = self.spark. \
            read. \
            option("delimiter", "|"). \
            option("header", "true"). \
            csv(os.path.join(data_path, "counties.csv")).limit(1)

        counties.createOrReplaceTempView("county")

        counties_geom = self.spark.sql(
            "SELECT *, st_geomFromWKT(geom) as geometry from county"
        )

        gdf = counties_geom.toPandas()
        print(gpd.GeoDataFrame(gdf, geometry="geometry"))
