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
package org.apache.sedona.core

import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{DistanceMetric, GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialPartitioning.QuadTreeRTPartitioner
import org.apache.sedona.core.spatialRDD.{PointRDD, SpatialRDD}
import org.apache.sedona.sql.TestBaseScala
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.WKTReader
import org.scalatest.prop.TableDrivenPropertyChecks.{Table, forAll}
import org.scalatest.prop.TableFor7

import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.Using

class KnnJoinQueryTest extends TestBaseScala {

  val testRootPath: String = System.getProperty("user.dir") + "/src/test/resources/knn/"

  case class KnnTestCase(
      id: Int,
      desc: String,
      p: Int,
      k: Int,
      objectLocation: String,
      queryLocation: String,
      resultLocation: String)

  // Function to read and parse the external file
  def readKnnTestCases(filePath: String): Seq[KnnTestCase] = {
    val lines = Using(Source.fromFile(filePath)) { source =>
      source
        .getLines()
        .drop(1) // Drop the first line
        .filterNot(line => line.trim.startsWith("#")) // Skip lines starting with #
        .toList
    }
    lines match {
      case scala.util.Success(l) =>
        l.map { line =>
          val Array(id, desc, p, k, objectLocation, queryLocation, resultLocation) =
            line.split(",")
          KnnTestCase(
            id.toInt,
            desc,
            p.toInt,
            k.toInt,
            objectLocation,
            queryLocation,
            resultLocation)
        }
      case scala.util.Failure(exception) =>
        println(s"Error reading file: ${exception.getMessage}")
        Seq.empty
    }
  }

  // All test cases for KNN
  val knnTestCasesFilePath: String = testRootPath + "all-test-cases-knn.csv"
  val knnTestCasesList: Seq[KnnTestCase] = readKnnTestCases(knnTestCasesFilePath)
  val knnTestCases: TableFor7[Int, String, Int, Int, String, String, String] = Table(
    ("id", "desc", "p", "k", "objectLocation", "queryLocation", "resultLocation"),
    knnTestCasesList.map(tc =>
      (
        tc.id,
        "knn_" + tc.desc,
        tc.p,
        tc.k,
        tc.objectLocation,
        tc.queryLocation,
        tc.resultLocation)): _*)

  forAll(knnTestCases) {
    (
        id: Int,
        desc: String,
        p: Int,
        k: Int,
        objectLocation: String,
        queryLocation: String,
        resultLocation: String) =>
      it(s"$id - $desc: p=$p, k=$k") {
        val objectRDD =
          new PointRDD(sc, testRootPath + objectLocation, 0, FileDataSplitter.CSV, true, p)
        val queryRDD =
          new PointRDD(sc, testRootPath + queryLocation, 0, FileDataSplitter.CSV, true, p)

        // analyze the both RDDs to get the statistics (e.g., boundary)
        objectRDD.analyze()
        queryRDD.analyze()

        // expand the boundary for partition to include both RDDs
        objectRDD.boundaryEnvelope.expandToInclude(queryRDD.boundaryEnvelope)

        // set the number of neighbors to be found
        objectRDD.setNeighborSampleNumber(k)

        // use modified quadtree partitioning, as it is an exact algorithm
        objectRDD.spatialPartitioning(GridType.QUADTREE_RTREE)
        queryRDD.spatialPartitioning(
          objectRDD.getPartitioner.asInstanceOf[QuadTreeRTPartitioner].nonOverlappedPartitioner())

        objectRDD.buildIndex(IndexType.RTREE, true)

        // Custom ordering for Point based on coordinates
        implicit val pointOrdering: Ordering[Point] = (p1: Point, p2: Point) => {
          val cmp = p1.getX.compare(p2.getX)
          if (cmp != 0) cmp else p1.getY.compare(p2.getY)
        }

        val knnOutputs = JoinQuery
          .KNNJoinQuery(objectRDD, queryRDD, IndexType.RTREE, k, DistanceMetric.EUCLIDEAN)
          .collect()
          .asScala
          .toList

        val sortedKnnOutputs = knnOutputs.sortBy { case (queryPoint, _) => queryPoint }

        val output = sortedKnnOutputs.map { case (queryPoint, neighbors) =>
          val sortedNeighbors = neighbors.asScala.toList.sorted
          val neighborsString = sortedNeighbors.mkString(",")
          s"$queryPoint,$neighborsString\n"
        }.mkString

        val expectedOutput =
          new String(Files.readAllBytes(Paths.get(testRootPath + resultLocation)))
        print(output)
        assert(expectedOutput == output)
      }
  }

  for (metric <- Seq(DistanceMetric.HAVERSINE, DistanceMetric.SPHEROID);
    broadcastQuerySide <- Seq(false, true)) {
    it(
      s"broadcast KNN join with $metric skips empty geometries, " +
        s"broadcastQuerySide=$broadcastQuerySide") {
      val reader = new WKTReader()
      def toSpatialRDD(wkts: Seq[String]): SpatialRDD[Geometry] = {
        val rdd = new SpatialRDD[Geometry]
        rdd.setRawSpatialRDD(sc.parallelize(wkts.map(wkt => reader.read(wkt)), 2).toJavaRDD())
        rdd.analyze()
        rdd
      }
      val queryRDD = toSpatialRDD(Seq("POINT (0 0)", "POLYGON EMPTY", "POINT (10 10)"))
      val objectRDD =
        toSpatialRDD(Seq("POINT (0.1 0.1)", "POLYGON EMPTY", "POINT (9 9)", "POINT (5 5)"))
      if (broadcastQuerySide) {
        // An index on the object side makes knnJoin broadcast the query side instead
        objectRDD.buildIndex(IndexType.RTREE, false)
      }
      val joinParams = new JoinParams(true, null, IndexType.RTREE, null, 2, metric)
      val pairs = JoinQuery
        .knnJoin(queryRDD, objectRDD, joinParams, false, true)
        .collect()
        .asScala
        .map(pair => (pair._1.toText, pair._2.toText))
        .sorted
      // Empty query geometries get no neighbors and empty objects are never neighbors
      assert(
        pairs == Seq(
          ("POINT (0 0)", "POINT (0.1 0.1)"),
          ("POINT (0 0)", "POINT (5 5)"),
          ("POINT (10 10)", "POINT (5 5)"),
          ("POINT (10 10)", "POINT (9 9)")))
    }
  }
}
