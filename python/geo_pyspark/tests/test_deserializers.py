import os
from unittest import TestCase

from pyspark.sql import SparkSession
from shapely.geometry import MultiPoint, Point, MultiLineString, LineString, Polygon, MultiPolygon
import geopandas as gpd

from geo_pyspark.data import data_path
from geo_pyspark.register import GeoSparkRegistrator, upload_jars

upload_jars()

spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestGeometryConvert(TestCase):

    def test_register_functions(self):
        df = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.show()

    def test_collect(self):

        df = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.collect()

    def test_loading_from_file_deserialization(self):
        geom = spark.read.\
            options(delimiter="|", header=True).\
            csv(os.path.join(data_path, "counties.csv")).\
            limit(1).\
            createOrReplaceTempView("counties")

        geom_area = spark.sql("SELECT st_area(st_geomFromWKT(geom)) as area from counties").collect()[0][0]
        polygon_shapely = spark.sql("SELECT st_geomFromWKT(geom) from counties").collect()[0][0]
        self.assertEqual(geom_area, polygon_shapely.area)

    def test_polygon_with_holes_deserialization(self):
        geom = spark.sql(
            """select st_geomFromWKT('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),
(20 30, 35 35, 30 20, 20 30))') as geom"""
        ).collect()[0][0]

        self.assertEqual(geom.area, 675.0)
        self.assertEqual(type(geom), Polygon)

    def test_multipolygon_with_holes_deserialization(self):
        geom = spark.sql(
            """select st_geomFromWKT('MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),
((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),
(30 20, 20 15, 20 25, 30 20)))')"""
        ).collect()[0][0]
        self.assertEqual(type(geom), MultiPolygon)

        self.assertEqual(geom.area, 712.5)

    def test_multipolygon_deserialization(self):
        geom = spark.sql(
            """select st_geomFromWKT()"""
        )

    def test_point_deserialization(self):
        geom = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""").collect()[0][0]
        self.assertEqual(
            geom.wkt,
            Point(-6.0, 52.0).wkt
        )

    def test_multipoint_deserialization(self):
        geom = spark.sql("""select st_geomFromWKT('MULTIPOINT(1 2, -2 3)') as geom""").collect()[0][0]

        self.assertEqual(
            geom.wkt,
            MultiPoint([(1, 2), (-2, 3)]).wkt
        )

    def test_linestring_deserialization(self):
        geom = spark.sql(
            """select st_geomFromWKT('LINESTRING (30 10, 10 30, 40 40)')"""
        ).collect()[0][0]

        self.assertEqual(type(geom), LineString)

        self.assertEqual(
            geom.wkt,
            LineString([(30, 10), (10, 30), (40, 40)]).wkt
        )

    def test_multilinestring_deserialization(self):
        geom = spark.sql(
            """SELECT st_geomFromWKT('MULTILINESTRING ((10 10, 20 20, 10 40),
                        (40 40, 30 30, 40 20, 30 10))') as geom"""
        ).collect()[0][0]

        self.assertEqual(type(geom), MultiLineString)

        self.assertEqual(
            geom.wkt,
            MultiLineString([
                ((10, 10), (20, 20), (10, 40)),
                ((40, 40), (30, 30), (40, 20), (30, 10))
            ]).wkt
        )

    def test_from_geopandas_convert(self):
        gdf = gpd.read_file(os.path.join(data_path, "gis_osm_pois_free_1.shp"))

        spark.createDataFrame(
            gdf
        ).show()

    def test_to_geopandas(self):
        counties = spark. \
            read. \
            option("delimiter", "|"). \
            option("header", "true"). \
            csv(os.path.join(data_path, "counties.csv")).limit(1)

        counties.createOrReplaceTempView("county")

        counties_geom = spark.sql(
            "SELECT *, st_geomFromWKT(geom) as geometry from county"
        )

        gdf = counties_geom.toPandas()
        print(gpd.GeoDataFrame(gdf, geometry="geometry"))
