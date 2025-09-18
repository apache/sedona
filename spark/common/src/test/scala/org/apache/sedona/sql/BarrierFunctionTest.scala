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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, BooleanType}
import org.apache.spark.sql.functions.expr
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, be}
import org.scalatest.prop.TableDrivenPropertyChecks
import scala.collection.JavaConverters._

class BarrierFunctionTest extends TestBaseScala with TableDrivenPropertyChecks {

  describe("Barrier Function Test") {

    it("should evaluate simple comparison expressions") {
      // Create test data
      val testDf = sparkSession
        .createDataFrame(
          Seq(Row(1, 4.5, 5), Row(2, 3.0, 3), Row(3, 5.0, 4), Row(4, 2.0, 2)).asJava,
          StructType(
            Seq(
              StructField("id", IntegerType, false),
              StructField("rating", DoubleType, false),
              StructField("stars", IntegerType, false))))

      testDf.createOrReplaceTempView("test_table")

      // Test simple greater than
      val result1 =
        sparkSession.sql("""SELECT id, barrier('rating > 4.0', 'rating', rating) as result
           FROM test_table""")
      val expected1 = Seq(true, false, true, false)
      result1.collect().map(_.getBoolean(1)) should be(expected1)

      // Test AND condition
      val result2 = sparkSession.sql("""SELECT id, barrier('rating > 4.0 AND stars >= 4',
                              'rating', rating,
                              'stars', stars) as result
           FROM test_table""")
      val expected2 = Seq(true, false, true, false)
      result2.collect().map(_.getBoolean(1)) should be(expected2)

      // Test OR condition
      val result3 = sparkSession.sql("""SELECT id, barrier('rating < 3.0 OR stars >= 5',
                              'rating', rating,
                              'stars', stars) as result
           FROM test_table""")
      val expected3 = Seq(true, false, false, true)
      result3.collect().map(_.getBoolean(1)) should be(expected3)
    }

    it("should handle different comparison operators") {
      val testDf = sparkSession
        .createDataFrame(
          Seq(Row(1, 10, 10), Row(2, 20, 30), Row(3, 15, 15), Row(4, 25, 20)).asJava,
          StructType(
            Seq(
              StructField("id", IntegerType, false),
              StructField("val1", IntegerType, false),
              StructField("val2", IntegerType, false))))

      testDf.createOrReplaceTempView("test_table")

      // Test equals
      val result1 = sparkSession.sql("""SELECT id, barrier('val1 = val2',
                              'val1', val1,
                              'val2', val2) as result
           FROM test_table""")
      val expected1 = Seq(true, false, true, false)
      result1.collect().map(_.getBoolean(1)) should be(expected1)

      // Test not equals
      val result2 = sparkSession.sql("""SELECT id, barrier('val1 != val2',
                              'val1', val1,
                              'val2', val2) as result
           FROM test_table""")
      val expected2 = Seq(false, true, false, true)
      result2.collect().map(_.getBoolean(1)) should be(expected2)

      // Test less than or equal
      val result3 = sparkSession.sql("""SELECT id, barrier('val1 <= val2',
                              'val1', val1,
                              'val2', val2) as result
           FROM test_table""")
      val expected3 = Seq(true, true, true, false)
      result3.collect().map(_.getBoolean(1)) should be(expected3)
    }

    it("should handle NOT operator") {
      val testDf = sparkSession
        .createDataFrame(
          Seq(Row(1, true), Row(2, false), Row(3, true), Row(4, false)).asJava,
          StructType(
            Seq(StructField("id", IntegerType, false), StructField("flag", BooleanType, false))))

      testDf.createOrReplaceTempView("test_table")

      val result = sparkSession.sql("""SELECT id, barrier('NOT flag', 'flag', flag) as result
           FROM test_table""")
      val expected = Seq(false, true, false, true)
      result.collect().map(_.getBoolean(1)) should be(expected)
    }

    it("should handle parentheses for grouping") {
      val testDf = sparkSession
        .createDataFrame(
          Seq(
            Row(1, 10, 20, 30),
            Row(2, 15, 25, 35),
            Row(3, 5, 15, 25),
            Row(4, 20, 10, 5)).asJava,
          StructType(
            Seq(
              StructField("id", IntegerType, false),
              StructField("a", IntegerType, false),
              StructField("b", IntegerType, false),
              StructField("c", IntegerType, false))))

      testDf.createOrReplaceTempView("test_table")

      // Test with parentheses
      val result =
        sparkSession.sql("""SELECT id, barrier('(a < b AND b < c) OR (a > b AND b > c)',
                             'a', a, 'b', b, 'c', c) as result
           FROM test_table""")
      val expected = Seq(true, true, true, true)
      result.collect().map(_.getBoolean(1)) should be(expected)
    }

    it("should handle string comparisons") {
      val testDf = sparkSession
        .createDataFrame(
          Seq(
            Row(1, "apple", "banana"),
            Row(2, "zebra", "apple"),
            Row(3, "cat", "cat"),
            Row(4, "dog", "cat")).asJava,
          StructType(
            Seq(
              StructField("id", IntegerType, false),
              StructField("str1", StringType, false),
              StructField("str2", StringType, false))))

      testDf.createOrReplaceTempView("test_table")

      val result = sparkSession.sql("""SELECT id, barrier('str1 < str2',
                             'str1', str1,
                             'str2', str2) as result
           FROM test_table""")
      val expected = Seq(true, false, false, false)
      result.collect().map(_.getBoolean(1)) should be(expected)
    }

    it("should handle null values") {
      val testDf = sparkSession
        .createDataFrame(
          Seq(Row(1, 10, 20), Row(2, null, 20), Row(3, 10, null), Row(4, null, null)).asJava,
          StructType(
            Seq(
              StructField("id", IntegerType, false),
              StructField("val1", IntegerType, true),
              StructField("val2", IntegerType, true))))

      testDf.createOrReplaceTempView("test_table")

      // Test null equality comparisons
      val resultEq = sparkSession.sql("""SELECT id, barrier('val1 = val2',
                             'val1', val1,
                             'val2', val2) as result
           FROM test_table""")
      val expectedEq = Seq(false, false, false, true)
      resultEq.collect().map(_.getBoolean(1)) should be(expectedEq)

      // Test null inequality comparisons
      val resultNe = sparkSession.sql("""SELECT id, barrier('val1 != val2',
                             'val1', val1,
                             'val2', val2) as result
           FROM test_table""")
      val expectedNe = Seq(true, true, true, false)
      resultNe.collect().map(_.getBoolean(1)) should be(expectedNe)

      // Test null with <= operator
      val resultLe = sparkSession.sql("""SELECT id, barrier('val1 <= val2',
                             'val1', val1,
                             'val2', val2) as result
           FROM test_table""")
      val expectedLe = Seq(true, false, false, true)
      resultLe.collect().map(_.getBoolean(1)) should be(expectedLe)
    }
  }

  describe("Barrier Function with KNN Spatial Join Tests") {
    it("should prevent filter pushdown in KNN joins with complex conditions") {
      // Create test data similar to KNN test patterns
      val queries = sparkSession
        .createDataFrame(
          Seq(
            Row(1, 1.0, 1.0, 5), // high rating query
            Row(2, 2.0, 2.0, 3), // low rating query
            Row(3, 3.0, 3.0, 4) // medium rating query
          ).asJava,
          StructType(
            Seq(
              StructField("q_id", IntegerType, false),
              StructField("q_x", DoubleType, false),
              StructField("q_y", DoubleType, false),
              StructField("q_rating", IntegerType, false))))

      val objects = sparkSession
        .createDataFrame(
          Seq(
            Row(1, 0.9, 1.1, 2), // close to query 1, low rating
            Row(2, 1.1, 0.9, 5), // close to query 1, high rating
            Row(3, 2.1, 1.9, 3), // close to query 2, medium rating
            Row(4, 2.9, 3.1, 4), // close to query 3, good rating
            Row(5, 1.5, 1.5, 1), // between queries, very low rating
            Row(6, 0.5, 0.5, 5) // close to query 1, high rating
          ).asJava,
          StructType(
            Seq(
              StructField("o_id", IntegerType, false),
              StructField("o_x", DoubleType, false),
              StructField("o_y", DoubleType, false),
              StructField("o_rating", IntegerType, false))))

      // Add geometry columns
      val queriesWithGeom = queries.withColumn("q_geom", expr("ST_Point(q_x, q_y)"))
      val objectsWithGeom = objects.withColumn("o_geom", expr("ST_Point(o_x, o_y)"))

      queriesWithGeom.createOrReplaceTempView("test_queries")
      objectsWithGeom.createOrReplaceTempView("test_objects")

      // Test 1: KNN join with barrier to prevent early filtering
      // This should find K=2 nearest neighbors first, then apply rating filter
      val resultWithBarrier = sparkSession.sql("""
        SELECT q.q_id, o.o_id, o.o_rating
        FROM test_queries q
        JOIN test_objects o
        ON ST_KNN(q.q_geom, o.o_geom, 2, false)
        WHERE barrier('q_rating >= 4 AND o_rating >= 3',
                     'q_rating', q.q_rating,
                     'o_rating', o.o_rating)
      """)

      val matches = resultWithBarrier.collect().sortBy(r => (r.getInt(0), r.getInt(1)))

      // Should find pairs where both query and object have good ratings
      // from the 2 nearest neighbors per query
      matches.length should be > 0
      matches.foreach { row =>
        val qRating = row.getInt(2) // This is o_rating in the select
        qRating should be >= 3
      }
    }

    it("should demonstrate different semantics with and without barrier in KNN joins") {
      // Create a scenario where barrier changes the result
      val hotels = sparkSession
        .createDataFrame(
          Seq(
            Row(1, 0.0, 0.0, 5), // luxury hotel at origin
            Row(2, 1.0, 1.0, 2) // budget hotel nearby
          ).asJava,
          StructType(
            Seq(
              StructField("h_id", IntegerType, false),
              StructField("h_x", DoubleType, false),
              StructField("h_y", DoubleType, false),
              StructField("h_stars", IntegerType, false))))

      val restaurants = sparkSession
        .createDataFrame(
          Seq(
            Row(1, 0.1, 0.1, 2.0), // close to luxury hotel, poor rating
            Row(2, 0.2, 0.2, 3.0), // close to luxury hotel, ok rating
            Row(3, 0.9, 0.9, 5.0), // close to budget hotel, excellent rating
            Row(4, 5.0, 5.0, 5.0) // far away, excellent rating
          ).asJava,
          StructType(
            Seq(
              StructField("r_id", IntegerType, false),
              StructField("r_x", DoubleType, false),
              StructField("r_y", DoubleType, false),
              StructField("r_rating", DoubleType, false))))

      val hotelsWithGeom = hotels.withColumn("h_geom", expr("ST_Point(h_x, h_y)"))
      val restaurantsWithGeom = restaurants.withColumn("r_geom", expr("ST_Point(r_x, r_y)"))

      hotelsWithGeom.createOrReplaceTempView("knn_hotels")
      restaurantsWithGeom.createOrReplaceTempView("knn_restaurants")

      // Query 1: WITHOUT barrier - filter might get pushed down
      // This could filter high-rated restaurants BEFORE finding nearest
      val withoutBarrier = sparkSession.sql("""
        SELECT h.h_id, r.r_id, r.r_rating
        FROM knn_hotels h
        JOIN knn_restaurants r
        ON ST_KNN(h.h_geom, r.r_geom, 2, false)
        WHERE h.h_stars >= 4 AND r.r_rating >= 4.0
      """)

      // Query 2: WITH barrier - ensures KNN completes before filtering
      // This finds 2 nearest restaurants FIRST, then filters
      val withBarrier = sparkSession.sql("""
        SELECT h.h_id, r.r_id, r.r_rating
        FROM knn_hotels h
        JOIN knn_restaurants r
        ON ST_KNN(h.h_geom, r.r_geom, 2, false)
        WHERE barrier('h_stars >= 4 AND r_rating >= 4.0',
                     'h_stars', h.h_stars,
                     'r_rating', r.r_rating)
      """)

      val resultsWithoutBarrier = withoutBarrier.collect()
      val resultsWithBarrier = withBarrier.collect()

      // The key difference: barrier should return fewer/different results
      resultsWithoutBarrier.length should be(2)

      // Expected: withBarrier should find 0 results (nearest 1,2 don't meet rating filter)
      resultsWithBarrier.length should be(0)
    }

    it("should work with inequality conditions in KNN barrier functions") {
      // Test adapted from KNN suite inequality tests
      val points1 = sparkSession
        .createDataFrame(
          Seq(Row(1, 1.0, 1.0), Row(2, 2.0, 2.0), Row(3, 3.0, 3.0)).asJava,
          StructType(
            Seq(
              StructField("id", IntegerType, false),
              StructField("x", DoubleType, false),
              StructField("y", DoubleType, false))))

      val points2 = sparkSession
        .createDataFrame(
          Seq(
            Row(1, 1.1, 1.1),
            Row(3, 3.1, 3.1),
            Row(6, 6.0, 6.0),
            Row(13, 13.0, 13.0),
            Row(16, 16.0, 16.0)).asJava,
          StructType(
            Seq(
              StructField("id", IntegerType, false),
              StructField("x", DoubleType, false),
              StructField("y", DoubleType, false))))

      val points1WithGeom = points1.withColumn("geom", expr("ST_Point(x, y)"))
      val points2WithGeom = points2.withColumn("geom", expr("ST_Point(x, y)"))

      points1WithGeom.createOrReplaceTempView("knn_points1")
      points2WithGeom.createOrReplaceTempView("knn_points2")

      // Test inequality condition with barrier
      val result = sparkSession.sql("""
        SELECT p1.id, p2.id
        FROM knn_points1 p1
        JOIN knn_points2 p2
        ON ST_KNN(p1.geom, p2.geom, 3, false)
        WHERE barrier('p1_id < p2_id',
                     'p1_id', p1.id,
                     'p2_id', p2.id)
      """)

      val matches = result.collect().sortBy(r => (r.getInt(0), r.getInt(1)))

      // Should only include pairs where p1.id < p2.id
      matches.foreach { row =>
        val p1Id = row.getInt(0)
        val p2Id = row.getInt(1)
        p1Id should be < p2Id
      }
    }

    it("should handle complex boolean expressions in KNN barrier scenarios") {
      // Create test data for complex conditions
      val venues = sparkSession
        .createDataFrame(
          Seq(
            Row(1, 0.0, 0.0, "restaurant", 4.5, true),
            Row(2, 0.1, 0.1, "cafe", 3.8, false),
            Row(3, 0.2, 0.2, "restaurant", 4.2, true),
            Row(4, 1.0, 1.0, "bar", 4.0, false)).asJava,
          StructType(
            Seq(
              StructField("v_id", IntegerType, false),
              StructField("v_x", DoubleType, false),
              StructField("v_y", DoubleType, false),
              StructField("v_type", StringType, false),
              StructField("v_rating", DoubleType, false),
              StructField("v_open_late", BooleanType, false))))

      val users = sparkSession
        .createDataFrame(
          Seq(Row(1, 0.05, 0.05, 25, true), Row(2, 0.5, 0.5, 35, false)).asJava,
          StructType(
            Seq(
              StructField("u_id", IntegerType, false),
              StructField("u_x", DoubleType, false),
              StructField("u_y", DoubleType, false),
              StructField("u_age", IntegerType, false),
              StructField("u_night_owl", BooleanType, false))))

      val venuesWithGeom = venues.withColumn("v_geom", expr("ST_Point(v_x, v_y)"))
      val usersWithGeom = users.withColumn("u_geom", expr("ST_Point(u_x, u_y)"))

      venuesWithGeom.createOrReplaceTempView("knn_venues")
      usersWithGeom.createOrReplaceTempView("knn_users")

      // Complex barrier condition: young night owls want open restaurants with good ratings
      val result = sparkSession.sql("""
        SELECT u.u_id, v.v_id, v.v_type, v.v_rating
        FROM knn_users u
        JOIN knn_venues v
        ON ST_KNN(u.u_geom, v.v_geom, 3, false)
        WHERE barrier('(u_age < 30 AND u_night_owl AND v_open_late) AND (v_type = "restaurant" AND v_rating > 4.0)',
                     'u_age', u.u_age,
                     'u_night_owl', u.u_night_owl,
                     'v_open_late', v.v_open_late,
                     'v_type', v.v_type,
                     'v_rating', v.v_rating)
      """)

      val matches = result.collect()

      // Should only match young night owls with open restaurants with good ratings
      matches.foreach { row =>
        val vType = row.getString(2)
        val vRating = row.getDouble(3)
        vType should be("restaurant")
        vRating should be > 4.0
      }
    }
  }
}
