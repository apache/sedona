/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.sql

import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.sql.utils.Adapter
import org.locationtech.jts.geom.{Geometry, LineString}

class constructorTestScala extends TestBaseScala {

  import sparkSession.implicits._

  describe("Sedona-SQL Constructor Test") {
    it("Passed ST_Point") {

      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)

      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      assert(pointDf.count() == 1000)

    }

    it("Passed ST_Point by double") {
      val pointDf = sparkSession.sql("SELECT ST_Point(double(1.2345), 2.3456)")
      assert(pointDf.count() == 1)
    }

    it("Passed ST_Point null safety") {
      val pointDf = sparkSession.sql("SELECT ST_Point(null, null)")
      assert(pointDf.count() == 1)
    }

    it("Passed ST_PointZ") {
      val pointDf = sparkSession.sql("SELECT ST_PointZ(1.2345, 2.3456, 3.4567)")
      assert(pointDf.count() == 1)
    }

    it("Passed ST_PointZ null safety") {
      val pointDf = sparkSession.sql("SELECT ST_PointZ(null, null, null)")
      assert(pointDf.count() == 1)
    }

    it("Passed ST_PolygonFromEnvelope") {
      val polygonDF = sparkSession.sql("select ST_PolygonFromEnvelope(double(1.234),double(2.234),double(3.345),double(3.345))")
      assert(polygonDF.count() == 1)
    }

    it("Passed ST_PointFromText") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")

      var pointDf = sparkSession.sql("select ST_PointFromText(concat(_c0,',',_c1),',') as arealandmark from pointtable")
      assert(pointDf.count() == 121960)
    }

    it("Passed ST_GeomFromWKT") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWkt(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 100)
      val nullGeom = sparkSession.sql("select ST_GeomFromWKT(null)")
      assert(nullGeom.first().isNullAt(0))
      // Fail on wrong input type
      intercept[Exception] {
        sparkSession.sql("SELECT ST_GeomFromWKT(0)").collect()
      }
    }

    it("Passed ST_GeomFromWKT invalid input") {
      // Fail on non wkt strings
      val thrown = intercept[Exception] {
        sparkSession.sql("SELECT ST_GeomFromWKT('not wkt')").collect()
      }
      assert(thrown.getMessage == "Unknown geometry type: NOT (line 1)")
    }

    it("Passed ST_LineFromText") {
      val geometryDf = Seq("Linestring(1 2, 3 4)").map(wkt => Tuple1(wkt)).toDF("geom")
      geometryDf.createOrReplaceTempView("linetable")
      var lineDf = sparkSession.sql("select ST_LineFromText(linetable.geom) from linetable")
      assert(lineDf.count() == 1)
    }

    it("Passed ST_GeomFromWKT 3D") {
      val geometryDf = Seq(
        "Point(21 52 87)",
        "Polygon((0 0 1, 0 1 1, 1 1 1, 1 0 1, 0 0 1))",
        "Linestring(0 0 1, 1 1 2, 1 0 3)",
        "MULTIPOINT ((10 40 66), (40 30 77), (20 20 88), (30 10 99))",
        "MULTIPOLYGON (((30 20 11, 45 40 11, 10 40 11, 30 20 11)), ((15 5 11, 40 10 11, 10 20 11, 5 10 11, 15 5 11)))",
        "MULTILINESTRING ((10 10 11, 20 20 11, 10 40 11), (40 40 11, 30 30 11, 40 20 11, 30 10 11))",
        "MULTIPOLYGON (((40 40 11, 20 45 11, 45 30 11, 40 40 11)), ((20 35 11, 10 30 11, 10 10 11, 30 5 11, 45 20 11, 20 35 11), (30 20 11, 20 15 11, 20 25 11, 30 20 11)))",
        "POLYGON((0 0 11, 0 5 11, 5 5 11, 5 0 11, 0 0 11), (1 1 11, 2 1 11, 2 2 11, 1 2 11, 1 1 11))"
      ).map(wkt => Tuple1(wkt)).toDF("geom")

      geometryDf.createOrReplaceTempView("geometrytable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWkt(geometrytable.geom) from geometrytable")
      assert(polygonDf.count() == 8)
    }

    it("Passed ST_GeomFromText") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromText(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 100)
      val nullGeom = sparkSession.sql("select ST_GeomFromText(null)")
      assert(nullGeom.first().isNullAt(0))
      // Fail on wrong input type
      intercept[Exception] {
        sparkSession.sql("SELECT ST_GeomFromText(0)").collect()
      }
    }

    it("Passed ST_GeomFromWKT multipolygon read as polygon bug") {
      val multipolygon =
        """'MULTIPOLYGON (((-97.143362 27.84948, -97.14051 27.849375, -97.13742 27.849375, -97.13647 27.851056, -97.136945 27.853788, -97.138728 27.855784, -97.141223 27.853158, -97.143362 27.84948)),
            ((-97.131954 27.894443, -97.131716 27.896018, -97.1212014 27.8937854, -97.113415 27.892132, -97.110206 27.890662, -97.110206 27.889191, -97.1104204 27.8890963, -97.112107 27.888351, -97.114247 27.886985, -97.11591 27.88583, -97.11698 27.885515, -97.1173397 27.8855439, -97.118287 27.88562, -97.1180218 27.8875761, -97.117931 27.888246, -97.118287 27.890662, -97.1187136 27.8907652, -97.125269 27.8923509, -97.129577 27.893393, -97.131954 27.894443)),
            ((-97.150493 27.874905, -97.150849 27.875851, -97.149067 27.877531, -97.146927 27.878267, -97.14467 27.880893, -97.143362 27.881523, -97.142055 27.881103, -97.14158 27.879527, -97.142412 27.878582, -97.143719 27.878372, -97.145383 27.876586, -97.147403 27.875641, -97.149423 27.874905, -97.150493 27.874905)),
            ((-97.277888 27.915016, -97.280673 27.916797, -97.277151 27.918885, -97.270902 27.924911, -97.268651 27.926005, -97.269181 27.927081, -97.266121 27.927604, -97.257 27.927545, -97.231476 27.927514, -97.230477 27.953057, -97.229337 27.954497, -97.227041 27.955697, -97.226871 27.956842, -97.227509 27.984419, -97.240412 27.991214, -97.249501 27.99119, -97.249808 28.00156, -97.249403 28.008394, -97.249352 28.015777, -97.246465 28.020607, -97.217976 28.006301, -97.214474 28.011763, -97.187227 27.974459, -97.185218 27.975922, -97.18445 27.978007, -97.18181 27.979354, -97.181351 27.980691, -97.179325 27.979937, -97.177906 27.981857, -97.180899 27.981241, -97.180019 27.983282, -97.177081 27.985091, -97.177068 27.987313, -97.175036 27.98849, -97.174192 27.991763, -97.175233 27.993887, -97.173479 27.996164, -97.170119 27.997582, -97.168713 27.996343, -97.165283 27.992159, -97.10562 27.956867, -97.098551 27.965841, -97.099903 27.963404, -97.095463 27.960391, -97.094531 27.960663, -97.095924 27.961514, -97.093476 27.964801, -97.096948 27.967277, -97.092467 27.964277, -97.093252 27.964707, -97.095581 27.961632, -97.0942561 27.9607887, -97.120902 27.923113, -97.129276 27.918726, -97.131359 27.916602, -97.131241 27.914922, -97.129933 27.912192, -97.131359 27.907046, -97.132342 27.905581, -97.133023 27.90253, -97.1338567 27.9011057, -97.136312 27.90439, -97.137131 27.902861, -97.138515 27.900826, -97.136995 27.899203, -97.13535 27.8978363, -97.135638 27.896964, -97.135671 27.896907, -97.1375368 27.8936862, -97.138133 27.892657, -97.141342 27.88667, -97.144432 27.883099, -97.147641 27.88646, -97.148829 27.8852, -97.1490339 27.884976, -97.151396 27.882393, -97.154652 27.876796, -97.1546812 27.8767587, -97.154642 27.877004, -97.161216 27.880646, -97.161268 27.880676, -97.162722 27.881532, -97.163644 27.882081, -97.164889 27.880906, -97.166941 27.879639, -97.170125 27.881547, -97.16912 27.882906, -97.167873 27.884568, -97.170064 27.885862, -97.181393 27.892565, -97.183688 27.889517, -97.197275 27.89754, -97.195021 27.900546, -97.197251 27.902384, -97.200396 27.906587, -97.195212 27.91354, -97.203996 27.913649, -97.20666 27.919866, -97.21019 27.918115, -97.217076 27.915506, -97.223506 27.912777, -97.220665 27.90783, -97.232103 27.908176, -97.232165 27.90647, -97.233391 27.906212, -97.233185 27.892319, -97.233476 27.89176, -97.234908 27.892503, -97.248829 27.899804, -97.277888 27.915016)))'"""
      val wkt = sparkSession.sql(
        s"""
           |SELECT ST_GeomFromWkt($multipolygon)
           |""".stripMargin)
      assert(wkt.first().getAs[Geometry](0).getGeometryType === "MultiPolygon")
    }

    it("Passed ST_GeomFromWKB") {
      // UTF-8 encoded WKB String
      val polygonWkbDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWkbGeometryInputLocation)
      polygonWkbDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql("select ST_GeomFromWKB(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 100)
      // RAW binary array
      val wkbSeq = Seq[Array[Byte]](Array[Byte](1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65))
      val rawWkbDf = wkbSeq.toDF("wkb")
      rawWkbDf.createOrReplaceTempView("rawWKBTable")
      val geometries = sparkSession.sql("SELECT ST_GeomFromWKB(rawWKBTable.wkb) as countyshape from rawWKBTable")
      val expectedGeom = "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)";
      assert(geometries.first().getAs[Geometry](0).toString.equals(expectedGeom))
      // null input
      val nullGeom = sparkSession.sql("SELECT ST_GeomFromWKB(null)")
      assert(nullGeom.first().isNullAt(0))
      // Fail on wrong input type
      intercept[Exception] {
        sparkSession.sql("SELECT ST_GeomFromWKB(0)").collect()
      }
    }

    it("Passed ST_GeomFromGeoJSON") {
      val polygonJsonDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(geojsonInputLocation)
      polygonJsonDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql("select ST_GeomFromGeoJSON(polygontable._c0) as countyshape from polygontable")
      assert(polygonDf.count() == 1000)
    }

    it("Passed ST_GeomFromGML") {
      val linestring =
        """'<gml:LineString srsName="EPSG:4269">
          |    <gml:coordinates>
          |        -71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932
          |    </gml:coordinates>
          |</gml:LineString>
          |'""".stripMargin
      val gml = sparkSession.sql(
        s"""
           |SELECT ST_GeomFromGML($linestring)
           |""".stripMargin)
      assert(gml.first().getAs[Geometry](0).isInstanceOf[LineString])
    }

    it("Passed ST_GeomFromKML") {
      val linestring =
        """'<LineString>
          |<coordinates>-71.1663,42.2614 -71.1667,42.2616</coordinates>
          |</LineString>
          |'""".stripMargin
      val kml = sparkSession.sql(
        s"""
           |SELECT ST_GeomFromKML($linestring)
           |""".stripMargin)
      assert(kml.first().getAs[Geometry](0).isInstanceOf[LineString])
    }

    it("Passed GeoJsonReader to DataFrame") {
      var spatialRDD = GeoJsonReader.readToGeometryRDD(sparkSession.sparkContext, geojsonInputLocation)
      var spatialDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(spatialDf.count() > 0)
    }

    it("Read shapefile -> DataFrame > RDD -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.columns(1) == "STATEFP")
      var spatialRDD2 = Adapter.toSpatialRdd(df, "geometry")
      Adapter.toDf(spatialRDD2, sparkSession).show(1)
    }

    it("Passed ST_MLineFromText") {
      var mLineDf = sparkSession.sql("select ST_MLineFromText('MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))')")
      assert(mLineDf.count() == 1)
    }
    it("Passed ST_MLineFromText With Srid") {
      var mLineDf = sparkSession.sql("select ST_MLineFromText('MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))',4269)")
      assert(mLineDf.count() == 1)
    }

    it("Passed ST_MPolyFromText") {
      var mLineDf = sparkSession.sql("select ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))')")
      assert(mLineDf.count() == 1)
    }

    it("Passed ST_MPolyFromText With Srid") {
      var mLineDf = sparkSession.sql("select ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))',4269)")
      assert(mLineDf.count() == 1)
    }
  }
}
