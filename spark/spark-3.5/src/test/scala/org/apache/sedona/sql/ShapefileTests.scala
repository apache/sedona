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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{DateType, DecimalType, LongType, StringType, StructField, StructType}
import org.locationtech.jts.geom.{Geometry, MultiPolygon, Point, Polygon}
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files

class ShapefileTests extends TestBaseScala with BeforeAndAfterAll {
  val temporaryLocation: String = resourceFolder + "shapefiles/tmp"

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteDirectory(new File(temporaryLocation))
    Files.createDirectory(new File(temporaryLocation).toPath)
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(new File(temporaryLocation))

  describe("Shapefile read tests") {
    it("read gis_osm_pois_free_1") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/gis_osm_pois_free_1")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "osm_id").get.dataType == StringType)
      assert(schema.find(_.name == "code").get.dataType == LongType)
      assert(schema.find(_.name == "fclass").get.dataType == StringType)
      assert(schema.find(_.name == "name").get.dataType == StringType)
      assert(schema.length == 5)
      assert(shapefileDf.count == 12873)

      shapefileDf.collect().foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        assert(geom.getSRID == 4326)
        assert(row.getAs[String]("osm_id").nonEmpty)
        assert(row.getAs[Long]("code") > 0)
        assert(row.getAs[String]("fclass").nonEmpty)
        assert(row.getAs[String]("name") != null)
      }

      // with projection, selecting geometry and attribute fields
      shapefileDf.select("geometry", "code").take(10).foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        assert(row.getAs[Long]("code") > 0)
      }

      // with projection, selecting geometry fields
      shapefileDf.select("geometry").take(10).foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
      }

      // with projection, selecting attribute fields
      shapefileDf.select("code", "osm_id").take(10).foreach { row =>
        assert(row.getAs[Long]("code") > 0)
        assert(row.getAs[String]("osm_id").nonEmpty)
      }

      // with transformation
      shapefileDf
        .selectExpr("ST_Buffer(geometry, 0.001) AS geom", "code", "osm_id as id")
        .take(10)
        .foreach { row =>
          assert(row.getAs[Geometry]("geom").isInstanceOf[Polygon])
          assert(row.getAs[Long]("code") > 0)
          assert(row.getAs[String]("id").nonEmpty)
        }
    }

    it("read dbf") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/dbf")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "STATEFP").get.dataType == StringType)
      assert(schema.find(_.name == "COUNTYFP").get.dataType == StringType)
      assert(schema.find(_.name == "COUNTYNS").get.dataType == StringType)
      assert(schema.find(_.name == "AFFGEOID").get.dataType == StringType)
      assert(schema.find(_.name == "GEOID").get.dataType == StringType)
      assert(schema.find(_.name == "NAME").get.dataType == StringType)
      assert(schema.find(_.name == "LSAD").get.dataType == StringType)
      assert(schema.find(_.name == "ALAND").get.dataType == LongType)
      assert(schema.find(_.name == "AWATER").get.dataType == LongType)
      assert(schema.length == 10)
      assert(shapefileDf.count() == 3220)

      shapefileDf.collect().foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.getSRID == 0)
        assert(geom.isInstanceOf[Polygon] || geom.isInstanceOf[MultiPolygon])
        assert(row.getAs[String]("STATEFP").nonEmpty)
        assert(row.getAs[String]("COUNTYFP").nonEmpty)
        assert(row.getAs[String]("COUNTYNS").nonEmpty)
        assert(row.getAs[String]("AFFGEOID").nonEmpty)
        assert(row.getAs[String]("GEOID").nonEmpty)
        assert(row.getAs[String]("NAME").nonEmpty)
        assert(row.getAs[String]("LSAD").nonEmpty)
        assert(row.getAs[Long]("ALAND") > 0)
        assert(row.getAs[Long]("AWATER") >= 0)
      }
    }

    it("read multipleshapefiles") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/multipleshapefiles")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "STATEFP").get.dataType == StringType)
      assert(schema.find(_.name == "COUNTYFP").get.dataType == StringType)
      assert(schema.find(_.name == "COUNTYNS").get.dataType == StringType)
      assert(schema.find(_.name == "AFFGEOID").get.dataType == StringType)
      assert(schema.find(_.name == "GEOID").get.dataType == StringType)
      assert(schema.find(_.name == "NAME").get.dataType == StringType)
      assert(schema.find(_.name == "LSAD").get.dataType == StringType)
      assert(schema.find(_.name == "ALAND").get.dataType == LongType)
      assert(schema.find(_.name == "AWATER").get.dataType == LongType)
      assert(schema.length == 10)
      assert(shapefileDf.count() == 3220)
    }

    it("read missing") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/missing")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "id").get.dataType == LongType)
      assert(schema.find(_.name == "a").get.dataType == StringType)
      assert(schema.find(_.name == "b").get.dataType == StringType)
      assert(schema.find(_.name == "c").get.dataType == StringType)
      assert(schema.find(_.name == "d").get.dataType == StringType)
      assert(schema.find(_.name == "e").get.dataType == StringType)
      assert(schema.length == 7)
      val rows = shapefileDf.collect()
      assert(rows.length == 3)
      rows.foreach { row =>
        val a = row.getAs[String]("a")
        val b = row.getAs[String]("b")
        val c = row.getAs[String]("c")
        val d = row.getAs[String]("d")
        val e = row.getAs[String]("e")
        if (a.isEmpty) {
          assert(b == "First")
          assert(c == "field")
          assert(d == "is")
          assert(e == "empty")
        } else if (e.isEmpty) {
          assert(a == "Last")
          assert(b == "field")
          assert(c == "is")
          assert(d == "empty")
        } else {
          assert(a == "Are")
          assert(b == "fields")
          assert(c == "are")
          assert(d == "not")
          assert(e == "empty")
        }
      }
    }

    it("read unsupported") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/unsupported")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "ID").get.dataType == StringType)
      assert(schema.find(_.name == "LOD").get.dataType == LongType)
      assert(schema.find(_.name == "Parent_ID").get.dataType == StringType)
      assert(schema.length == 4)
      val rows = shapefileDf.collect()
      assert(rows.length == 20)
      var nonNullLods = 0
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry") == null)
        assert(row.getAs[String]("ID").nonEmpty)
        val lodIndex = row.fieldIndex("LOD")
        if (!row.isNullAt(lodIndex)) {
          assert(row.getAs[Long]("LOD") == 2)
          nonNullLods += 1
        }
        assert(row.getAs[String]("Parent_ID").nonEmpty)
      }
      assert(nonNullLods == 17)
    }

    it("read bad_shx") {
      var shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/bad_shx")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "field_1").get.dataType == LongType)
      var rows = shapefileDf.collect()
      assert(rows.length == 2)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        if (geom == null) {
          assert(row.getAs[Long]("field_1") == 3)
        } else {
          assert(geom.isInstanceOf[Point])
          assert(row.getAs[Long]("field_1") == 2)
        }
      }

      // Copy the .shp and .dbf files to temporary location, and read the same shapefiles without .shx
      FileUtils.cleanDirectory(new File(temporaryLocation))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/bad_shx/bad_shx.shp"),
        new File(temporaryLocation + "/bad_shx.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/bad_shx/bad_shx.dbf"),
        new File(temporaryLocation + "/bad_shx.dbf"))
      shapefileDf = sparkSession.read
        .format("shapefile")
        .load(temporaryLocation)
      rows = shapefileDf.collect()
      assert(rows.length == 2)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        if (geom == null) {
          assert(row.getAs[Long]("field_1") == 3)
        } else {
          assert(geom.isInstanceOf[Point])
          assert(row.getAs[Long]("field_1") == 2)
        }
      }
    }

    it("read contains_null_geom") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/contains_null_geom")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "fInt").get.dataType == LongType)
      assert(schema.find(_.name == "fFloat").get.dataType.isInstanceOf[DecimalType])
      assert(schema.find(_.name == "fString").get.dataType == StringType)
      assert(schema.length == 4)
      val rows = shapefileDf.collect()
      assert(rows.length == 10)
      rows.foreach { row =>
        val fInt = row.getAs[Long]("fInt")
        val fFloat = row.getAs[java.math.BigDecimal]("fFloat").doubleValue()
        val fString = row.getAs[String]("fString")
        val geom = row.getAs[Geometry]("geometry")
        if (fInt == 2 || fInt == 5) {
          assert(geom == null)
        } else {
          assert(geom.isInstanceOf[Point])
          assert(geom.getCoordinate.x == fInt)
          assert(geom.getCoordinate.y == fInt)
        }
        assert(Math.abs(fFloat - 3.14159 * fInt) < 1e-4)
        assert(fString == s"str_$fInt")
      }
    }

    it("read test_datatypes") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/datatypes")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "id").get.dataType == LongType)
      assert(schema.find(_.name == "aInt").get.dataType == LongType)
      assert(schema.find(_.name == "aUnicode").get.dataType == StringType)
      assert(schema.find(_.name == "aDecimal").get.dataType.isInstanceOf[DecimalType])
      assert(schema.find(_.name == "aDecimal2").get.dataType.isInstanceOf[DecimalType])
      assert(schema.find(_.name == "aDate").get.dataType == DateType)
      assert(schema.length == 7)

      val rows = shapefileDf.collect()
      assert(rows.length == 9)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        assert(geom.getSRID == 4269)
        val idIndex = row.fieldIndex("id")
        if (row.isNullAt(idIndex)) {
          assert(row.isNullAt(row.fieldIndex("aInt")))
          assert(row.getAs[String]("aUnicode").isEmpty)
          assert(row.isNullAt(row.fieldIndex("aDecimal")))
          assert(row.isNullAt(row.fieldIndex("aDecimal2")))
          assert(row.isNullAt(row.fieldIndex("aDate")))
        } else {
          val id = row.getLong(idIndex)
          assert(row.getAs[Long]("aInt") == id)
          assert(row.getAs[String]("aUnicode") == s"测试$id")
          if (id < 10) {
            val decimal = row.getDecimal(row.fieldIndex("aDecimal")).doubleValue()
            assert((decimal * 10).toInt == id * 10 + id)
            assert(row.isNullAt(row.fieldIndex("aDecimal2")))
            assert(row.getAs[java.sql.Date]("aDate").toString == s"202$id-0$id-0$id")
          } else {
            assert(row.isNullAt(row.fieldIndex("aDecimal")))
            val decimal = row.getDecimal(row.fieldIndex("aDecimal2")).doubleValue()
            assert((decimal * 100).toInt == id * 100 + id)
            assert(row.isNullAt(row.fieldIndex("aDate")))
          }
        }
      }
    }

    it("read with .shp path specified") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/datatypes/datatypes1.shp")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "id").get.dataType == LongType)
      assert(schema.find(_.name == "aInt").get.dataType == LongType)
      assert(schema.find(_.name == "aUnicode").get.dataType == StringType)
      assert(schema.find(_.name == "aDecimal").get.dataType.isInstanceOf[DecimalType])
      assert(schema.find(_.name == "aDate").get.dataType == DateType)
      assert(schema.length == 6)

      val rows = shapefileDf.collect()
      assert(rows.length == 5)
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        val idIndex = row.fieldIndex("id")
        if (row.isNullAt(idIndex)) {
          assert(row.isNullAt(row.fieldIndex("aInt")))
          assert(row.getAs[String]("aUnicode").isEmpty)
          assert(row.isNullAt(row.fieldIndex("aDecimal")))
          assert(row.isNullAt(row.fieldIndex("aDate")))
        } else {
          val id = row.getLong(idIndex)
          assert(row.getAs[Long]("aInt") == id)
          assert(row.getAs[String]("aUnicode") == s"测试$id")
          val decimal = row.getDecimal(row.fieldIndex("aDecimal")).doubleValue()
          assert((decimal * 10).toInt == id * 10 + id)
          assert(row.getAs[java.sql.Date]("aDate").toString == s"202$id-0$id-0$id")
        }
      }
    }

    it("read with glob path specified") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/datatypes/datatypes2.*")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "id").get.dataType == LongType)
      assert(schema.find(_.name == "aInt").get.dataType == LongType)
      assert(schema.find(_.name == "aUnicode").get.dataType == StringType)
      assert(schema.find(_.name == "aDecimal2").get.dataType.isInstanceOf[DecimalType])
      assert(schema.length == 5)

      val rows = shapefileDf.collect()
      assert(rows.length == 4)
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        val id = row.getAs[Long]("id")
        assert(row.getAs[Long]("aInt") == id)
        assert(row.getAs[String]("aUnicode") == s"测试$id")
        val decimal = row.getDecimal(row.fieldIndex("aDecimal2")).doubleValue()
        assert((decimal * 100).toInt == id * 100 + id)
      }
    }

    it("read without shx") {
      FileUtils.cleanDirectory(new File(temporaryLocation))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/gis_osm_pois_free_1/gis_osm_pois_free_1.shp"),
        new File(temporaryLocation + "/gis_osm_pois_free_1.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/gis_osm_pois_free_1/gis_osm_pois_free_1.dbf"),
        new File(temporaryLocation + "/gis_osm_pois_free_1.dbf"))

      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(temporaryLocation)
      val rows = shapefileDf.collect()
      assert(rows.length == 12873)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        assert(geom.getSRID == 0)
        assert(row.getAs[String]("osm_id").nonEmpty)
        assert(row.getAs[Long]("code") > 0)
        assert(row.getAs[String]("fclass").nonEmpty)
        assert(row.getAs[String]("name") != null)
      }
    }

    it("read without dbf") {
      FileUtils.cleanDirectory(new File(temporaryLocation))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/gis_osm_pois_free_1/gis_osm_pois_free_1.shp"),
        new File(temporaryLocation + "/gis_osm_pois_free_1.shp"))
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(temporaryLocation)
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.length == 1)

      val rows = shapefileDf.collect()
      assert(rows.length == 12873)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
      }
    }

    it("read without shp") {
      FileUtils.cleanDirectory(new File(temporaryLocation))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/gis_osm_pois_free_1/gis_osm_pois_free_1.dbf"),
        new File(temporaryLocation + "/gis_osm_pois_free_1.dbf"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/gis_osm_pois_free_1/gis_osm_pois_free_1.shx"),
        new File(temporaryLocation + "/gis_osm_pois_free_1.shx"))
      intercept[Exception] {
        sparkSession.read
          .format("shapefile")
          .load(temporaryLocation)
          .count()
      }

      intercept[Exception] {
        sparkSession.read
          .format("shapefile")
          .load(resourceFolder + "shapefiles/gis_osm_pois_free_1/gis_osm_pois_free_1.shx")
          .count()
      }
    }

    it("read directory containing missing .shp files") {
      FileUtils.cleanDirectory(new File(temporaryLocation))
      // Missing .shp file for datatypes1
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes1.dbf"),
        new File(temporaryLocation + "/datatypes1.dbf"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.shp"),
        new File(temporaryLocation + "/datatypes2.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.dbf"),
        new File(temporaryLocation + "/datatypes2.dbf"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.cpg"),
        new File(temporaryLocation + "/datatypes2.cpg"))

      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(temporaryLocation)
      val rows = shapefileDf.collect()
      assert(rows.length == 4)
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        val id = row.getAs[Long]("id")
        assert(row.getAs[Long]("aInt") == id)
        assert(row.getAs[String]("aUnicode") == s"测试$id")
        val decimal = row.getDecimal(row.fieldIndex("aDecimal2")).doubleValue()
        assert((decimal * 100).toInt == id * 100 + id)
      }
    }

    it("read partitioned directory") {
      FileUtils.cleanDirectory(new File(temporaryLocation))
      Files.createDirectory(new File(temporaryLocation + "/part=1").toPath)
      Files.createDirectory(new File(temporaryLocation + "/part=2").toPath)
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes1.shp"),
        new File(temporaryLocation + "/part=1/datatypes1.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes1.dbf"),
        new File(temporaryLocation + "/part=1/datatypes1.dbf"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes1.cpg"),
        new File(temporaryLocation + "/part=1/datatypes1.cpg"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.shp"),
        new File(temporaryLocation + "/part=2/datatypes2.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.dbf"),
        new File(temporaryLocation + "/part=2/datatypes2.dbf"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.cpg"),
        new File(temporaryLocation + "/part=2/datatypes2.cpg"))

      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(temporaryLocation)
        .select("part", "id", "aInt", "aUnicode", "geometry")
      var rows = shapefileDf.collect()
      assert(rows.length == 9)
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        val id = row.getAs[Long]("id")
        assert(row.getAs[Long]("aInt") == id)
        if (id < 10) {
          assert(row.getAs[Int]("part") == 1)
        } else {
          assert(row.getAs[Int]("part") == 2)
        }
        if (id > 0) {
          assert(row.getAs[String]("aUnicode") == s"测试$id")
        }
      }

      // Using partition filters
      rows = shapefileDf.where("part = 2").collect()
      assert(rows.length == 4)
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        assert(row.getAs[Int]("part") == 2)
        val id = row.getAs[Long]("id")
        assert(id > 10)
        assert(row.getAs[Long]("aInt") == id)
        assert(row.getAs[String]("aUnicode") == s"测试$id")
      }
    }

    it("read with recursiveFileLookup") {
      FileUtils.cleanDirectory(new File(temporaryLocation))
      Files.createDirectory(new File(temporaryLocation + "/part1").toPath)
      Files.createDirectory(new File(temporaryLocation + "/part2").toPath)
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes1.shp"),
        new File(temporaryLocation + "/part1/datatypes1.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes1.dbf"),
        new File(temporaryLocation + "/part1/datatypes1.dbf"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes1.cpg"),
        new File(temporaryLocation + "/part1/datatypes1.cpg"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.shp"),
        new File(temporaryLocation + "/part2/datatypes2.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.dbf"),
        new File(temporaryLocation + "/part2/datatypes2.dbf"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.cpg"),
        new File(temporaryLocation + "/part2/datatypes2.cpg"))

      val shapefileDf = sparkSession.read
        .format("shapefile")
        .option("recursiveFileLookup", "true")
        .load(temporaryLocation)
        .select("id", "aInt", "aUnicode", "geometry")
      val rows = shapefileDf.collect()
      assert(rows.length == 9)
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        val id = row.getAs[Long]("id")
        assert(row.getAs[Long]("aInt") == id)
        if (id > 0) {
          assert(row.getAs[String]("aUnicode") == s"测试$id")
        }
      }
    }

    it("read with custom geometry column name") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .option("geometry.name", "geom")
        .load(resourceFolder + "shapefiles/gis_osm_pois_free_1")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geom").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "osm_id").get.dataType == StringType)
      assert(schema.find(_.name == "code").get.dataType == LongType)
      assert(schema.find(_.name == "fclass").get.dataType == StringType)
      assert(schema.find(_.name == "name").get.dataType == StringType)
      assert(schema.length == 5)
      val rows = shapefileDf.collect()
      assert(rows.length == 12873)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geom")
        assert(geom.isInstanceOf[Point])
        assert(row.getAs[String]("osm_id").nonEmpty)
        assert(row.getAs[Long]("code") > 0)
        assert(row.getAs[String]("fclass").nonEmpty)
        assert(row.getAs[String]("name") != null)
      }

      val exception = intercept[Exception] {
        sparkSession.read
          .format("shapefile")
          .option("geometry.name", "osm_id")
          .load(resourceFolder + "shapefiles/gis_osm_pois_free_1")
      }
      assert(
        exception.getMessage.contains(
          "osm_id is reserved for geometry but appears in non-spatial attributes"))
    }

    it("read with shape key column") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .option("key.name", "fid")
        .load(resourceFolder + "shapefiles/datatypes")
        .select("id", "fid", "geometry", "aUnicode")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "geometry").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "id").get.dataType == LongType)
      assert(schema.find(_.name == "fid").get.dataType == LongType)
      assert(schema.find(_.name == "aUnicode").get.dataType == StringType)
      val rows = shapefileDf.collect()
      assert(rows.length == 9)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        val id = row.getAs[Long]("id")
        if (id > 0) {
          assert(row.getAs[Long]("fid") == id % 10)
          assert(row.getAs[String]("aUnicode") == s"测试$id")
        } else {
          assert(row.getAs[Long]("fid") == 5)
        }
      }
    }

    it("read with both custom geometry column and shape key column") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .option("geometry.name", "g")
        .option("key.name", "fid")
        .load(resourceFolder + "shapefiles/datatypes")
        .select("id", "fid", "g", "aUnicode")
      val schema = shapefileDf.schema
      assert(schema.find(_.name == "g").get.dataType == GeometryUDT)
      assert(schema.find(_.name == "id").get.dataType == LongType)
      assert(schema.find(_.name == "fid").get.dataType == LongType)
      assert(schema.find(_.name == "aUnicode").get.dataType == StringType)
      val rows = shapefileDf.collect()
      assert(rows.length == 9)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("g")
        assert(geom.isInstanceOf[Point])
        val id = row.getAs[Long]("id")
        if (id > 0) {
          assert(row.getAs[Long]("fid") == id % 10)
          assert(row.getAs[String]("aUnicode") == s"测试$id")
        } else {
          assert(row.getAs[Long]("fid") == 5)
        }
      }
    }

    it("read with invalid shape key column") {
      val exception = intercept[Exception] {
        sparkSession.read
          .format("shapefile")
          .option("geometry.name", "g")
          .option("key.name", "aDate")
          .load(resourceFolder + "shapefiles/datatypes")
      }
      assert(
        exception.getMessage.contains(
          "aDate is reserved for shape key but appears in non-spatial attributes"))

      val exception2 = intercept[Exception] {
        sparkSession.read
          .format("shapefile")
          .option("geometry.name", "g")
          .option("key.name", "g")
          .load(resourceFolder + "shapefiles/datatypes")
      }
      assert(exception2.getMessage.contains("geometry.name and key.name cannot be the same"))
    }

    it("read with custom charset") {
      FileUtils.cleanDirectory(new File(temporaryLocation))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.shp"),
        new File(temporaryLocation + "/datatypes2.shp"))
      FileUtils.copyFile(
        new File(resourceFolder + "shapefiles/datatypes/datatypes2.dbf"),
        new File(temporaryLocation + "/datatypes2.dbf"))

      val shapefileDf = sparkSession.read
        .format("shapefile")
        .option("charset", "GB2312")
        .load(temporaryLocation)
      val rows = shapefileDf.collect()
      assert(rows.length == 4)
      rows.foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
        val id = row.getAs[Long]("id")
        assert(row.getAs[Long]("aInt") == id)
        assert(row.getAs[String]("aUnicode") == s"测试$id")
        val decimal = row.getDecimal(row.fieldIndex("aDecimal2")).doubleValue()
        assert((decimal * 100).toInt == id * 100 + id)
      }
    }

    it("read with custom schema") {
      val customSchema = StructType(
        Seq(
          StructField("osm_id", StringType),
          StructField("code2", LongType),
          StructField("geometry", GeometryUDT)))
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .schema(customSchema)
        .load(resourceFolder + "shapefiles/gis_osm_pois_free_1")
      assert(shapefileDf.schema == customSchema)
      val rows = shapefileDf.collect()
      assert(rows.length == 12873)
      rows.foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.isInstanceOf[Point])
        assert(row.getAs[String]("osm_id").nonEmpty)
        assert(row.isNullAt(row.fieldIndex("code2")))
      }
    }
  }
}
