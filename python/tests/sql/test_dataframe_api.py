import math

from pyspark.sql import functions as f

from sedona.sql import st_functions as stf
from sedona.sql import st_constructors as stc
from sedona.sql import st_aggregates as sta
from sedona.sql import st_predicates as stp
from tests.test_base import TestBase

class TestDataFrameAPI(TestBase):

    def test_call_function_using_columns(self):
        df = (
            self.spark
            .sql("SELECT 0.0 AS x, 1.0 AS y")
            .select(stc.ST_Point(f.col("x"), f.col("y")))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 1)"
        assert(actual_result == expected_result)

    def test_call_function_using_floats(self):
        df = (
            self.spark
            .sql("SELECT 2")
            .select(stc.ST_Point(0.0, 1.0))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 1)"
        assert(actual_result == expected_result)

    # constructors
    def test_ST_Point(self):
        df = (
            self.spark
            .sql("SELECT 0.0 AS x, 1.0 AS y")
            .select(stc.ST_Point("x", "y"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 1)"
        assert(actual_result == expected_result)

    def test_ST_PointFromText(self):
        df = (
            self.spark
            .sql("SELECT '0.0,1.0' AS c")
            .select(stc.ST_PointFromText("c", f.lit(',')))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 1)"
        assert(actual_result == expected_result)

    def test_ST_PolygonFromText(self):
        df = (
            self.spark
            .sql("SELECT '0.0,0.0,1.0,0.0,1.0,1.0,0.0,0.0' AS c")
            .select(stc.ST_PolygonFromText("c", f.lit(',')))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
        assert(actual_result == expected_result)

    def test_ST_LineFromText(self):
        df = (
            self.spark
            .sql("SELECT 'Linestring(1 2, 3 4)' AS wkt")
            .select(stc.ST_LineFromText("wkt"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (1 2, 3 4)"
        assert(actual_result == expected_result)

    def test_ST_LineStringFromText(self):
        df = (
            self.spark
            .sql("SELECT '0.0,0.0,1.0,0.0' AS c")
            .select(stc.ST_LineStringFromText("c", f.lit(',')))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (0 0, 1 0)"
        assert(actual_result == expected_result)

    def test_ST_GeomFromWKT(self):
        df = (
            self.spark
            .sql("SELECT 'POINT(0.0 1.0)' AS wkt")
            .select(stc.ST_GeomFromWKT("wkt"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 1)"
        assert(actual_result == expected_result)

    def test_ST_GeomFromText(self):
        df = (
            self.spark
            .sql("SELECT 'POINT(0.0 1.0)' AS wkt")
            .select(stc.ST_GeomFromText("wkt"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 1)"
        assert(actual_result == expected_result)

    def test_ST_GeomFromWKB(self):
        wkb = '0102000000020000000000000084d600c00000000080b5d6bf00000060e1eff7bf00000080075de5bf'
        df = (
            self.spark
            .sql(f"SELECT X'{wkb}' AS wkb")
            .select(stc.ST_GeomFromWKB("wkb"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)"
        assert(actual_result == expected_result)

    def test_ST_GeomFromGeoJSON(self):
        geojson = "{ \"type\": \"Feature\", \"properties\": { \"prop\": \"01\" }, \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 0.0, 1.0 ] }},"
        df = (
            self.spark
            .sql(f"SELECT '{geojson}' AS geojson")
            .select(stc.ST_GeomFromGeoJSON("geojson"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 1)"
        assert(actual_result == expected_result)

    def test_ST_PolygonFromEnvelope(self):
        df = (
            self.spark
            .sql("SELECT 0.0 AS minx, 1.0 AS miny, 2.0 AS maxx, 3.0 AS maxy")
            .select(stc.ST_PolygonFromEnvelope("minx", "miny", "maxx", "maxy"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"
        assert(actual_result == expected_result)


    def test_ST_PolygonFromEnvelope(self):
        df = (
            self.spark
            .sql("SELECT null AS c")
            .select(stc.ST_PolygonFromEnvelope(0.0, 1.0, 2.0, 3.0))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 1, 0 3, 2 3, 2 1, 0 1))"
        assert(actual_result == expected_result)

    def test_ST_GeomFromGeoHash(self):
        df = (
            self.spark
            .sql("SELECT 's00twy01mt' AS geohash")
            .select(stc.ST_GeomFromGeoHash("geohash", 4))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0.703125 0.87890625, 0.703125 1.0546875, 1.0546875 1.0546875, 1.0546875 0.87890625, 0.703125 0.87890625))"
        assert(actual_result == expected_result)

    def test_ST_GeomFromGML(self):
        gml_string = "<gml:LineString srsName=\"EPSG:4269\"><gml:coordinates>-71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932</gml:coordinates></gml:LineString>"
        df = (
            self.spark
            .sql(f"SELECT '{gml_string}' AS gml")
            .select(stc.ST_GeomFromGML("gml"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (-71.16028 42.258729, -71.160837 42.259112, -71.161143 42.25932)"
        assert(actual_result == expected_result)

    def test_ST_GeomFromKML(self):
        kml_string = "<LineString><coordinates>-71.1663,42.2614 -71.1667,42.2616</coordinates></LineString>"
        df = (
            self.spark
            .sql(f"SELECT '{kml_string}' as kml")
            .select(stc.ST_GeomFromKML("kml"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (-71.1663 42.2614, -71.1667 42.2616)"
        assert(actual_result == expected_result)

    # functions
    def test_ST_ConvexHull(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_ConvexHull("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        assert(actual_result == "POLYGON ((0 0, 1 1, 1 0, 0 0))")

    def test_ST_Buffer(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(1.0, 1.0) AS geom")
            .select(stf.ST_Buffer("geom", 1.0).alias("geom"))
        )
        actual_result = df.selectExpr("ST_PrecisionReduce(geom, 2)").collect()[0][0].wkt
        expected_result = "POLYGON ((1.98 0.8, 1.92 0.62, 1.83 0.44, 1.71 0.29, 1.56 0.17, 1.38 0.08, 1.2 0.02, 1 0, 0.8 0.02, 0.62 0.08, 0.44 0.17, 0.29 0.29, 0.17 0.44, 0.08 0.62, 0.02 0.8, 0 1, 0.02 1.2, 0.08 1.38, 0.17 1.56, 0.29 1.71, 0.44 1.83, 0.62 1.92, 0.8 1.98, 1 2, 1.2 1.98, 1.38 1.92, 1.56 1.83, 1.71 1.71, 1.83 1.56, 1.92 1.38, 1.98 1.2, 2 1, 1.98 0.8))"
        assert(actual_result == expected_result)

    def test_ST_Envelope(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_Envelope("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
        assert(actual_result == expected_result)


    def test_ST_YMax(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_YMax("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 1.0
        assert(actual_result == expected_result)


    def test_ST_YMin(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_YMin("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 0.0
        assert(actual_result == expected_result)


    def test_ST_Centroid(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))') AS geom")
            .select(stf.ST_Centroid("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (1 1)"
        assert(actual_result == expected_result)


    def test_ST_Length(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
            .select(stf.ST_Length("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 1.0
        assert(actual_result == expected_result)


    def test_ST_Area(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_Area("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 0.5
        assert(actual_result == expected_result)


    def test_ST_Distance(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 0.0) as b")
            .select(stf.ST_Distance("a", "b"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 1.0
        assert(actual_result == expected_result)


    def test_ST_3DDistance(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0, 0.0) AS a, ST_Point(3.0, 0.0, 4.0) as b")
            .select(stf.ST_3DDistance("a", "b"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 5.0
        assert(actual_result == expected_result)


    def test_ST_Transform(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(1.0, 1.0) AS geom")
            .select(stf.ST_Transform("geom", f.lit("EPSG:4326"), f.lit("EPSG:32649")).alias("geom"))
        )
        actual_result = df.selectExpr("ST_PrecisionReduce(geom, 2)").collect()[0][0].wkt
        expected_result = "POINT (-33741810.95 1823994.03)"
        assert(actual_result == expected_result)


    def test_ST_Intersection(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') AS a, ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') AS b")
            .select(stf.ST_Intersection("a", "b"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"
        assert(actual_result == expected_result)


    def test_ST_IsValid(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') AS geom")
            .select(stf.ST_IsValid("geom"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_PrecisionReduce(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.12, 0.23) AS geom")
            .select(stf.ST_PrecisionReduce("geom", 1))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0.1 0.2)"
        assert(actual_result == expected_result)


    def test_ST_IsSimple(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWkt('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_IsSimple("geom"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_MakeValid(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))') AS geom")
            .select(stf.ST_MakeValid("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))"
        assert(actual_result == expected_result)


    def test_ST_SimplifyPreserveTopology(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 0.9, 1 1, 0 0))') AS geom")
            .select(stf.ST_SimplifyPreserveTopology("geom", 0.2))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 0, 1 0, 1 1, 0 0))"
        assert(actual_result == expected_result)


    def test_ST_AsText(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_AsText("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = "POINT (0 0)"
        assert(actual_result == expected_result)


    def test_ST_AsGeoJSON(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_AsGeoJSON("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = "{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}"
        assert(actual_result == expected_result)


    def test_ST_AsBinary(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_AsBinary("geom"))
        )
        actual_result = df.collect()[0][0].hex()
        expected_result = "010100000000000000000000000000000000000000"
        assert(actual_result == expected_result)


    def test_ST_AsGML(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_AsGML("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = "<gml:Point>\n  <gml:coordinates>\n    0.0,0.0 \n  </gml:coordinates>\n</gml:Point>\n"
        assert(actual_result == expected_result)


    def test_ST_AsKML(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_AsKML("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = "<Point>\n  <coordinates>0.0,0.0</coordinates>\n</Point>\n"
        assert(actual_result == expected_result)


    def test_ST_SRID(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_SRID("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 0
        assert(actual_result == expected_result)


    def test_ST_SetSRID(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_SetSRID("geom", 3021).alias("geom"))
        )
        actual_result = df.selectExpr("ST_SRID(geom)").collect()[0][0]
        expected_result = 3021
        assert(actual_result == expected_result)


    def test_ST_AsEWKB(self):
        df = (
            self.spark
            .sql("SELECT ST_SetSRID(ST_Point(0.0, 0.0), 3021) AS geom")
            .select(stf.ST_AsEWKB("geom"))
        )
        actual_result = df.collect()[0][0].hex()
        expected_result = "0101000020cd0b000000000000000000000000000000000000"
        assert(actual_result == expected_result) 


    def test_ST_NPoints(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1)') AS geom")
            .select(stf.ST_NPoints("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 2
        assert(actual_result == expected_result)


    def test_ST_GeometryType(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS geom")
            .select(stf.ST_GeometryType("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = "ST_Point"
        assert(actual_result == expected_result)


    def test_ST_Difference(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') AS a,ST_GeomFromWKT('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))') AS b")
            .select(stf.ST_Difference("a", "b"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 -3, -3 -3, -3 3, 0 3, 0 -3))"
        assert(actual_result == expected_result)


    def test_ST_SymDifference(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))') AS a, ST_GeomFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))') AS b")
            .select(stf.ST_SymDifference("a", "b"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))"
        assert(actual_result == expected_result)


    def test_ST_Union(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))') AS a, ST_GeomFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))') AS b")
            .select(stf.ST_Union("a", "b"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))"
        assert(actual_result == expected_result)


    def test_ST_Azimuth(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 1.0) AS b")
            .select(stf.ST_Azimuth("a", "b"))
        )
        actual_result = df.collect()[0][0] * 180 / math.pi
        expected_result = 45.0
        assert(actual_result == expected_result)


    def test_ST_X(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 1.0, 2.0) AS geom")
            .select(stf.ST_X("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 0.0
        assert(actual_result == expected_result)


    def test_ST_Y(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 1.0, 2.0) AS geom")
            .select(stf.ST_Y("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 1.0
        assert(actual_result == expected_result)


    def test_ST_Z(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 1.0, 2.0) AS geom")
            .select(stf.ST_Z("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 2.0
        assert(actual_result == expected_result)


    def test_ST_StartPoint(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
            .select(stf.ST_StartPoint("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 0)"
        assert(actual_result == expected_result)


    def test_ST_Boundary(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_Boundary("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (0 0, 1 0, 1 1, 0 0)"
        assert(actual_result == expected_result)


    def test_ST_EndPoint(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
            .select(stf.ST_EndPoint("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (1 0)"
        assert(actual_result == expected_result)


    def test_ST_ExteriorRing(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS geom")
            .select(stf.ST_ExteriorRing("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (0 0, 1 0, 1 1, 0 0)"
        assert(actual_result == expected_result)


    def test_ST_GeometryN(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0))') AS geom")
            .select(stf.ST_GeometryN("geom", 0))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 0)"
        assert(actual_result == expected_result)


    def test_ST_InteriorRingN(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
            .select(stf.ST_InteriorRingN("geom", 0))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (1 1, 2 2, 2 1, 1 1)"
        assert(actual_result == expected_result)


    def test_ST_Dump(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom")
            .select(stf.ST_Dump("geom"))
        )
        actual_result = sorted([geom.wkt for geom in df.collect()[0][0]])
        expected_result = ["POINT (0 0)", "POINT (1 1)"]
        assert(actual_result == expected_result)


    def test_ST_DumpPoints(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
            .select(stf.ST_DumpPoints("geom"))
        )
        actual_result = sorted([geom.wkt for geom in df.collect()[0][0]])
        expected_result = ["POINT (0 0)", "POINT (1 0)"]
        assert(actual_result == expected_result)


    def test_ST_IsClosed(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
            .select(stf.ST_IsClosed("geom"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_NumInteriorRings(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 3 0, 3 3, 0 0), (1 1, 2 2, 2 1, 1 1))') AS geom")
            .select(stf.ST_NumInteriorRings("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 1
        assert(actual_result == expected_result)


    def test_ST_AddPoint_default_index(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
            .select(stf.ST_AddPoint("line", "point"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (0 0, 1 0, 1 1)"
        assert(actual_result == expected_result)


    def test_ST_AddPoint_specified_index(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line, ST_Point(1.0, 1.0) AS point")
            .select(stf.ST_AddPoint("line", "point", 1))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (0 0, 1 1, 1 0)"
        assert(actual_result == expected_result)


    def test_ST_RemovePoint(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1)') AS geom")
            .select(stf.ST_RemovePoint("geom", 1))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (0 0, 1 1)"
        assert(actual_result == expected_result)


    def test_ST_IsRing(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)') AS geom")
            .select(stf.ST_IsRing("geom"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_SubDivide(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS geom")
            .select(stf.ST_SubDivide("geom", 5))
        )
        actual_result = sorted([geom.wkt for geom in df.collect()[0][0]])
        expected_result = ["LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)"]
        assert(actual_result == expected_result)


    def test_ST_SubDivideExplode(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)') AS geom")
            .select(stf.ST_SubDivideExplode("geom", 5))
        )
        actual_results = sorted([row[0].wkt for row in df.collect()])
        expected_result = ["LINESTRING (0 0, 2.5 0)", "LINESTRING (2.5 0, 5 0)"]
        assert(actual_results == expected_result)


    def test_ST_NumGeometries(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1))') AS geom")
            .select(stf.ST_NumGeometries("geom"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 2
        assert(actual_result == expected_result)


    def test_ST_LineMerge(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('MULTILINESTRING ((0 0, 1 0), (1 0, 2 0))') AS geom")
            .select(stf.ST_LineMerge("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (0 0, 1 0, 2 0)"
        assert(actual_result == expected_result)

    def test_ST_FlipCoordinates(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 1.0) AS geom")
            .select(stf.ST_FlipCoordinates("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (1 0)"
        assert(actual_result == expected_result)

    def test_ST_MinimumBoundingCircle_default_quadrants(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
            .select(stf.ST_MinimumBoundingCircle("geom").alias("geom")).selectExpr("ST_PrecisionReduce(geom, 2)")
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0.99 -0.1, 0.96 -0.19, 0.92 -0.28, 0.85 -0.35, 0.78 -0.42, 0.69 -0.46, 0.6 -0.49, 0.5 -0.5, 0.4 -0.49, 0.31 -0.46, 0.22 -0.42, 0.15 -0.35, 0.08 -0.28, 0.04 -0.19, 0.01 -0.1, 0 0, 0.01 0.1, 0.04 0.19, 0.08 0.28, 0.15 0.35, 0.22 0.42, 0.31 0.46, 0.4 0.49, 0.5 0.5, 0.6 0.49, 0.69 0.46, 0.78 0.42, 0.85 0.35, 0.92 0.28, 0.96 0.19, 0.99 0.1, 1 0, 0.99 -0.1))"
        assert(actual_result == expected_result)

    def test_ST_MinimumBoundingCircle_specified_quadrants(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
            .select(stf.ST_MinimumBoundingCircle("geom", 2).alias("geom"))
        )
        actual_result = df.selectExpr("ST_PrecisionReduce(geom, 2)").collect()[0][0].wkt
        expected_result = "POLYGON ((0.99 -0.1, 0.96 -0.19, 0.92 -0.28, 0.85 -0.35, 0.78 -0.42, 0.69 -0.46, 0.6 -0.49, 0.5 -0.5, 0.4 -0.49, 0.31 -0.46, 0.22 -0.42, 0.15 -0.35, 0.08 -0.28, 0.04 -0.19, 0.01 -0.1, 0 0, 0.01 0.1, 0.04 0.19, 0.08 0.28, 0.15 0.35, 0.22 0.42, 0.31 0.46, 0.4 0.49, 0.5 0.5, 0.6 0.49, 0.69 0.46, 0.78 0.42, 0.85 0.35, 0.92 0.28, 0.96 0.19, 0.99 0.1, 1 0, 0.99 -0.1))"
        assert(actual_result == expected_result)


    def test_ST_MinimumBoundingRadius(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS geom")
            .select(stf.ST_MinimumBoundingRadius("geom").alias("c"))
        )
        rowResult = df.select("c.center", "c.radius").collect()[0]
        
        actualCenter = rowResult[0].wkt
        actualRadius = rowResult[1]
        
        expectedCenter = "POINT (0.5 0)"
        expectedRadius = 0.5

        assert(actualCenter == expectedCenter)
        assert(actualRadius == expectedRadius)


    def test_ST_LineSubstring(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line")
            .select(stf.ST_LineSubstring("line", 0.5, 1.0))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (1 0, 2 0)"
        assert(actual_result == expected_result)


    def test_ST_LineInterpolatePoint(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 0)') AS line")
            .select(stf.ST_LineInterpolatePoint("line", 0.5))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (1 0)"
        assert(actual_result == expected_result)


    def test_ST_Multi(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS point")
            .select(stf.ST_Multi("point"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "MULTIPOINT (0 0)"
        assert(actual_result == expected_result)


    def test_ST_PointOnSurface(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
            .select(stf.ST_PointOnSurface("line"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 0)"
        assert(actual_result == expected_result)


    def test_ST_Reverse(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
            .select(stf.ST_Reverse("line"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "LINESTRING (1 0, 0 0)"
        assert(actual_result == expected_result)

    def test_ST_PointN(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
            .select(stf.ST_PointN("line", 2))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (1 0)"
        assert(actual_result == expected_result)


    def test_ST_AsEWKT(self):
        df = (
            self.spark
            .sql("SELECT ST_SetSRID(ST_Point(0.0, 0.0), 4326) AS point")
            .select(stf.ST_AsEWKT("point"))
        )
        actual_result = df.collect()[0][0]
        expected_result = "SRID=4326;POINT (0 0)"
        assert(actual_result == expected_result)


    def test_ST_Force_2D(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0, 1.0) AS point")
            .select(stf.ST_Force_2D("point"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POINT (0 0)"
        assert(actual_result == expected_result)


    def test_ST_IsEmpty(self):
        df = (
            self.spark
            .sql("SELECT ST_Difference(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0)) AS empty_geom")
            .select(stf.ST_IsEmpty("empty_geom"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_XMax(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
            .select(stf.ST_XMax("line"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 1.0
        assert(actual_result == expected_result)

    def test_ST_XMin(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS line")
            .select(stf.ST_XMin("line"))
        )
        actual_result = df.collect()[0][0]
        expected_result = 0.0
        assert(actual_result == expected_result)

    def test_ST_BuildArea(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('MULTILINESTRING ((0 0, 1 0), (1 0, 1 1), (1 1, 0 0))') AS multiline")
            .select(stf.ST_BuildArea("multiline").alias("geom"))
        )
        actual_result = df.selectExpr("ST_Normalize(geom)").collect()[0][0].wkt
        expected_result = "POLYGON ((0 0, 1 1, 1 0, 0 0))"
        assert(actual_result == expected_result)

    def test_ST_Collect_with_array(self):
        df = (
            self.spark
            .sql("SELECT array(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0)) as points")
            .select(stf.ST_Collect("points"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "MULTIPOINT (0 0, 1 1)"
        assert(actual_result == expected_result)


    def test_ST_Collect_with_varargs(self):
        df = (
            self.spark
            .sql("SELECT ST_Point(0.0, 0.0) AS a, ST_Point(1.0, 1.0) AS b")
            .select(stf.ST_Collect("a", "b"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "MULTIPOINT (0 0, 1 1)"
        assert(actual_result == expected_result)

    def test_ST_CollectionExtract_with_default_type(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 0))') AS geom")
            .select(stf.ST_CollectionExtract("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "MULTILINESTRING ((0 0, 1 0))"
        assert(actual_result == expected_result)

    def test_ST_CollectionExtract_with_specified_type(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 0))') AS geom")
            .select(stf.ST_CollectionExtract("geom", 1))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "MULTIPOINT (0 0)"
        assert(actual_result == expected_result)

    def test_ST_Normalize(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 0))') AS polygon")
            .select(stf.ST_Normalize("polygon"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 0, 1 1, 1 0, 0 0))"
        assert(actual_result == expected_result)

    # predicates
    def test_ST_Contains(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(0.5, 0.25) AS b")
            .select(stp.ST_Contains("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)

    def test_ST_Intersects(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 1)') AS a, ST_GeomFromWKT('LINESTRING (0 1, 1 0)') AS b")
            .select(stp.ST_Intersects("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_Within(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_Point(0.5, 0.25) AS b")
            .select(stp.ST_Within("b", "a"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_Equals(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS a, ST_GeomFromWKT('LINESTRING (1 0, 0 0)') AS b")
            .select(stp.ST_Equals("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_Crosses(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') AS a,ST_GeomFromWKT('LINESTRING(1 5, 5 1)') AS b")
            .select(stp.ST_Crosses("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)


    def test_ST_Touches(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((1 1, 1 0, 2 0, 1 1))') AS b")
            .select(stp.ST_Touches("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)

    def test_ST_Overlaps(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((0.5 1, 1.5 0, 2 0, 0.5 1))') AS b")
            .select(stp.ST_Overlaps("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)

    def test_ST_Disjoint(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') AS a, ST_GeomFromWKT('POLYGON ((2 0, 3 0, 3 1, 2 0))') AS b")
            .select(stp.ST_Disjoint("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(actual_result)

    def test_ST_OrderingEquals(self):
        df = (
            self.spark
            .sql("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS a, ST_GeomFromWKT('LINESTRING (1 0, 0 0)') AS b")
            .select(stp.ST_OrderingEquals("a", "b"))
        )
        actual_result = df.collect()[0][0]
        assert(not actual_result)

    # aggregates
    def test_ST_Envelope_Aggr(self):
        df = (
            self.spark
            .sql("SELECT explode(array(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0))) AS geom")
            .select(sta.ST_Envelope_Aggr("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
        assert(actual_result == expected_result)


    def test_ST_Union_Aggr(self):
        df = (
            self.spark
            .sql("SELECT explode(array(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GeomFromWKT('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'))) AS geom")
            .select(sta.ST_Union_Aggr("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((1 0, 0 0, 0 1, 1 1, 2 1, 2 0, 1 0))"
        assert(actual_result == expected_result)

    def test_ST_Intersection_Aggr(self):
        df = (
            self.spark
            .sql("SELECT explode(array(ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))'), ST_GeomFromWKT('POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))'))) AS geom")
            .select(sta.ST_Intersection_Aggr("geom"))
        )
        actual_result = df.collect()[0][0].wkt
        expected_result = "POLYGON ((2 0, 1 0, 1 1, 2 1, 2 0))"
        assert(actual_result == expected_result)
