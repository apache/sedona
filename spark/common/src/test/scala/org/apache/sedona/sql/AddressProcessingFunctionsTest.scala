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

import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ExpandAddress, ParseAddress}
import org.apache.spark.sql.{Row, functions => f}
import org.scalatest.BeforeAndAfterEach
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.collection.mutable

class AddressProcessingFunctionsTest extends TestBaseScala with BeforeAndAfterEach {
  private val logger = LoggerFactory.getLogger(getClass)
  var clearedLibPostal = false

  def clearLibpostalDataDir(): String = {
    val dir = SedonaConf.fromActiveSession().getLibPostalDataDir
    if (dir != null && dir.nonEmpty) {
      try {
        Files
          .walk(Paths.get(dir))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(path => Files.deleteIfExists(path))
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to clear libpostal data directory: ${e.getMessage}")
      }
    }
    dir
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (!clearedLibPostal) {
      clearedLibPostal = true
      clearLibpostalDataDir()
    }
  }

  describe("ExpandAddress") {
    it("should return expected normalized forms") {
      val resultDf = sparkSession
        .sql(
          "SELECT ExpandAddress('781 Franklin Ave Crown Heights Brooklyn NY 11216 USA') as normalized")
      resultDf.write
        .format("noop")
        .mode("overwrite")
        .save() // Tests a deserialization step that collect does not
      val result = resultDf.collect
        .take(1)(0)(0)
        .asInstanceOf[mutable.Seq[String]]
      assert(result.contains("781 franklin avenue crown heights brooklyn new york 11216 usa"))
    }

    it("should return null for null input") {
      val result = sparkSession.sql("SELECT ExpandAddress(NULL) as normalized").collect()
      assert(result.head.get(0) == null)
    }

    it("should work when chained with explode") {
      val result = sparkSession
        .sql("SELECT Explode(ExpandAddress('781 Franklin Ave Crown Heights Brooklyn NY 11216 USA')) as normalized")
        .collect()
        .map(_.getString(0))
      assert(result.contains("781 franklin avenue crown heights brooklyn new york 11216 usa"))
    }
  }

  describe("ParseAddress") {
    it("should return expected label/value pairs") {
      val resultDf = sparkSession
        .sql(
          "SELECT ParseAddress('781 Franklin Ave Crown Heights Brooklyn NY 11216 USA') as parsed")
      resultDf.write
        .format("noop")
        .mode("overwrite")
        .save() // Tests a deserialization step that collect does not
      val result = resultDf
        .take(1)(0)(0)
        .asInstanceOf[mutable.Seq[Row]]
        .map(row => row.getAs[String]("label") -> row.getAs[String]("value"))
        .toMap

      assert(result.contains("road"))
      assert(result("road") == "franklin ave")
      assert(result.contains("city_district"))
      assert(result("city_district") == "brooklyn")
      assert(result.contains("house_number"))
      assert(result("house_number") == "781")
      assert(result.contains("postcode"))
      assert(result("postcode") == "11216")
      assert(result.contains("country"))
      assert(result("country").toLowerCase.contains("usa"))
      assert(result.contains("suburb"))
      assert(result("suburb").toLowerCase.contains("crown heights"))
    }

    it("should return null for null input") {
      val result = sparkSession.sql("SELECT ParseAddress(NULL) as parsed").collect()
      assert(result.head.get(0) == null)
    }

    it("should should work when chained with explode") {
      val result = sparkSession
        .sql("SELECT Explode(ParseAddress('781 Franklin Ave Crown Heights Brooklyn NY 11216 USA')) as parsed")
        .collect()
        .map(row => row.getAs[Row]("parsed"))
        .map(row => row.getAs[String]("label") -> row.getAs[String]("value"))
        .toMap

      assert(result.contains("road"))
      assert(result("road") == "franklin ave")
      assert(result.contains("city_district"))
      assert(result("city_district") == "brooklyn")
      assert(result.contains("house_number"))
      assert(result("house_number") == "781")
      assert(result.contains("postcode"))
      assert(result("postcode") == "11216")
      assert(result.contains("country"))
      assert(result("country").toLowerCase.contains("usa"))
      assert(result.contains("suburb"))
      assert(result("suburb").toLowerCase.contains("crown heights"))
    }
  }

  describe("DataFrame API") {
    it("should return expected normalized forms using ExpandAddress") {
      val result = sparkSession
        .range(1)
        .select(ExpandAddress(f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA"))
          .alias("address"))
        .take(1)(0)(0)
        .asInstanceOf[mutable.Seq[String]]
      assert(result.contains("781 franklin avenue crown heights brooklyn new york 11216 usa"))
    }

    it("should return expected label/value pairs using ParseAddress") {
      val result = sparkSession
        .range(1)
        .select(ParseAddress(f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA")))
        .take(1)(0)(0)
        .asInstanceOf[mutable.Seq[Row]]
        .map(row => row.getAs[String]("label") -> row.getAs[String]("value"))
        .toMap

      assert(result.contains("road"))
      assert(result("road") == "franklin ave")
      assert(result.contains("city_district"))
      assert(result("city_district") == "brooklyn")
      assert(result.contains("house_number"))
      assert(result("house_number") == "781")
      assert(result.contains("postcode"))
      assert(result("postcode") == "11216")
      assert(result.contains("country"))
      assert(result("country").toLowerCase.contains("usa"))
      assert(result.contains("suburb"))
      assert(result("suburb").toLowerCase.contains("crown heights"))
    }
  }

  describe("Codegen") {
    it("should return the same values as non-codegen for ExpandAddress and ParseAddress") {
      val expected = sparkSession
        .range(1)
        .select(
          ExpandAddress(f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA")).alias(
            "expanded"),
          ParseAddress(f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA")).alias(
            "parsed"))
        .collect()
      sparkSession.conf.set("spark.sql.codegen.factoryMode", "CODEGEN_ONLY")
      val actual = sparkSession
        .range(1)
        .withColumn("address", f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA"))
        .cache() // Cache to ensure codegen is triggered, no constant folding
        .select(
          ExpandAddress(f.col("address")).alias("expanded"),
          ParseAddress(f.col("address")).alias("parsed"))
        .collect()

      sparkSession.catalog.clearCache()
      assert(expected sameElements actual)
    }

    it(
      "should return the same values as non-codegen for ExpandAddress and ParseAddress for null input") {
      val expected = sparkSession
        .range(1)
        .select(
          ExpandAddress(f.lit(null)).alias("expanded"),
          ParseAddress(f.lit(null)).alias("parsed"))
        .collect()
      sparkSession.conf.set("spark.sql.codegen.factoryMode", "CODEGEN_ONLY")
      val actual = sparkSession
        .range(1)
        .withColumn("address", f.lit(null))
        .cache() // Cache to ensure codegen is triggered, no constant folding
        .select(
          ExpandAddress(f.col("address")).alias("expanded"),
          ParseAddress(f.col("address")).alias("parsed"))
        .collect()

      sparkSession.catalog.clearCache()
      assert(expected sameElements actual)
    }

    it("should return the same values as non-code-gen case with explode") {
      // debugging reveals this goes through codegen, but not sure why
      val codeGenResult = sparkSession
        .range(1)
        .select(
          f.explode(ExpandAddress(f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA")))
            .alias("expanded"))
        .withColumn(
          "parsed",
          f.explode(ParseAddress(f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA"))))
        .collect()
      val nonCodeGenResult = sparkSession
        .range(1)
        .withColumn("address", f.lit("781 Franklin Ave Crown Heights Brooklyn NY 11216 USA"))
        .cache() // For some reason with the cache and explode we don't get code gen
        .withColumn("expanded", f.explode(ExpandAddress(f.col("address"))))
        .withColumn("parsed", f.explode(ParseAddress(f.col("address"))))
        .drop("id", "address")
        .collect()

      sparkSession.catalog.clearCache()
      assert(codeGenResult sameElements nonCodeGenResult)
    }
  }
}
