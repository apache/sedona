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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.geography.{Constructors => GeographyConstructors}
import org.apache.sedona.common.raster.RasterConstructors
import org.apache.sedona.core.enums.IndexType
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.utils.RasterSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, Expression, GenericInternalRow, Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.{ST_Buffer, ST_Transform}
import org.apache.spark.sql.sedona_sql.expressions.raster.RS_Envelope
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType, IntegerType}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Native spatial join execution, including computed keys and retained row ownership. */
class NativeJoinExecutionSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var session: SparkSession = _
  private var ownedContext = false
  private var previous: Option[SparkSession] = None
  private val nativeType = GeometryType("ANY")

  override def beforeAll(): Unit = {
    super.beforeAll()
    previous = SparkSession.getActiveSession
    val borrowed = previous.orElse(SparkSession.getDefaultSession)
    ownedContext = borrowed.isEmpty
    session = borrowed.map(_.newSession()).getOrElse {
      SedonaContext
        .builder()
        .master("local[2]")
        .appName(getClass.getSimpleName)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    }
    SparkSession.setActiveSession(session)
    session.conf.set("spark.sql.geospatial.enabled", "true")
    session.conf.set("spark.sql.adaptive.enabled", "false")
    session.conf.set("sedona.join.numpartition", "2")
    SedonaContext.create(session)
  }

  override def afterAll(): Unit = {
    try {
      if (ownedContext && session != null) session.stop()
      previous match {
        case Some(value) => SparkSession.setActiveSession(value)
        case None => SparkSession.clearActiveSession()
      }
    } finally super.afterAll()
  }

  private def point(x: Double, y: Double, srid: Int = 4326): Geometry =
    new GeometryFactory(new PrecisionModel(), srid).createPoint(new Coordinate(x, y))

  private def scan(
      name: String,
      dataType: DataType,
      values: Seq[(Int, Any)]): LocalTableScanExec = {
    val attributes = Seq(
      AttributeReference(name + "Id", IntegerType, nullable = false)(),
      AttributeReference(name + "Shape", dataType, nullable = true)())
    val rows = values.map { case (id, shape) => new GenericInternalRow(Array[Any](id, shape)) }
    LocalTableScanExec(attributes, rows, None)
  }

  private def geometryScan(
      name: String,
      values: Seq[(Int, Geometry)],
      dataType: DataType = nativeType): LocalTableScanExec =
    scan(
      name,
      dataType,
      values.map { case (id, shape) =>
        (id, SpatialTypeSupport.serializeGeometry(shape, dataType))
      })

  // Two spatial partitions need at least four input records for partition sampling.
  private def matchingPoints(firstId: Int): Seq[(Int, Geometry)] =
    (0 until 4).map(i => (firstId + i, point(1 + 20 * i, 2)))

  private val matchingPointIds = (0 until 4).map(i => (1 + i, 10 + i)).toSet
  private val allRasterIds = (for (left <- 1 to 4; right <- 10 to 13) yield (left, right)).toSet

  private def shape(plan: SparkPlan): Expression =
    if (SpatialTypeSupport.isNativeGeometry(plan.output(1).dataType) ||
      SpatialTypeSupport.isNativeGeography(plan.output(1).dataType)) {
      new RejectLegacyJoinInput(plan.output(1))
    } else plan.output(1)

  private def ids(rows: Array[InternalRow]): Set[(Int, Int)] = {
    val pairs = rows.map(row => (row.getInt(0), row.getInt(2))).toSet
    assert(rows.length == pairs.size, "Spatial join duplicated an input pair")
    pairs
  }

  private def broadcast(
      left: SparkPlan,
      right: SparkPlan,
      raster: Boolean = false,
      geography: Boolean = false,
      distance: Option[Expression] = None,
      predicate: SpatialPredicate = SpatialPredicate.INTERSECTS): BroadcastIndexJoinExec = {
    val index =
      SpatialIndexExec(left, shape(left), IndexType.RTREE, raster, false, distance, geography)
    BroadcastIndexJoinExec(
      index,
      right,
      shape(right),
      LeftSide,
      LeftSide,
      Inner,
      predicate,
      distance = distance,
      geographyShape = geography)
  }

  test("partitioned range and distance joins bypass native input byte adapters") {
    val left = geometryScan("left", matchingPoints(1))
    val right = geometryScan("right", matchingPoints(10))
    val range = RangeJoinExec(left, right, shape(left), shape(right), SpatialPredicate.INTERSECTS)
    assert(ids(range.executeCollect()) == matchingPointIds)
    val distance = DistanceJoinExec(
      left,
      right,
      shape(left),
      shape(right),
      Literal(0d),
      true,
      SpatialPredicate.INTERSECTS,
      false)
    assert(ids(distance.executeCollect()) == matchingPointIds)
  }

  test("broadcast build and stream paths bypass adapters for relation and distance joins") {
    val left = geometryScan("left", Seq(1 -> point(1, 2)))
    val right = geometryScan("right", Seq(10 -> point(1, 2)))
    assert(ids(broadcast(left, right).executeCollect()) == Set((1, 10)))
    assert(
      ids(broadcast(left, right, distance = Some(Literal(0d))).executeCollect()) == Set((1, 10)))
  }

  test("computed join shapes bypass serialization in partitioned and broadcast execution") {
    val left = geometryScan("left", matchingPoints(1))
    val right = geometryScan("right", matchingPoints(10))
    def buffer(plan: SparkPlan): Expression =
      new RejectComputedJoinSerialization(ST_Buffer(Seq(shape(plan), Literal(0.1d))))
    def transform(plan: SparkPlan): Expression = new RejectComputedJoinSerialization(
      ST_Transform(
        Seq(shape(plan), Literal("EPSG:4326"), Literal("EPSG:3857")),
        false,
        "",
        "",
        ""))
    val raster = RasterConstructors.makeEmptyRaster(1, "D", 2, 2, 0d, 3d, 1d, -1d, 0d, 0d, 4326)
    val rasterLeft =
      scan("rasterLeft", RasterUDT(), (1 to 4).map(i => i -> RasterSerializer.serialize(raster)))
    val rasterRight =
      scan(
        "rasterRight",
        RasterUDT(),
        (10 to 13).map(i => i -> RasterSerializer.serialize(raster)))
    def envelope(plan: SparkPlan): Expression =
      new RejectComputedJoinSerialization(RS_Envelope(Seq(shape(plan))))
    val scenarios = Seq(
      (left, right, buffer(left), buffer(right), matchingPointIds),
      (left, right, transform(left), transform(right), matchingPointIds),
      (rasterLeft, rasterRight, envelope(rasterLeft), envelope(rasterRight), allRasterIds))
    scenarios.foreach { case (build, stream, buildShape, streamShape, expected) =>
      assert(
        ids(RangeJoinExec(build, stream, buildShape, streamShape, SpatialPredicate.INTERSECTS)
          .executeCollect()) == expected)
      assert(
        ids(
          DistanceJoinExec(
            build,
            stream,
            buildShape,
            streamShape,
            Literal(0d),
            true,
            SpatialPredicate.INTERSECTS,
            false).executeCollect()) == expected)
      Seq(None, Some(Literal(0d))).foreach { distance =>
        val index = SpatialIndexExec(build, buildShape, IndexType.RTREE, false, false, distance)
        val join = BroadcastIndexJoinExec(
          index,
          stream,
          streamShape,
          LeftSide,
          LeftSide,
          Inner,
          SpatialPredicate.INTERSECTS,
          distance = distance)
        assert(ids(join.executeCollect()) == expected)
      }
    }
  }

  test("native null shapes preserve RDD row payloads and broadcast outer output") {
    val left = geometryScan("left", Seq(0 -> null, 1 -> point(1, 2)))
    val right = geometryScan("right", Seq(10 -> point(1, 2)))
    val operator =
      RangeJoinExec(left, right, shape(left), shape(right), SpatialPredicate.INTERSECTS)
    val bound = BindReferences.bindReference(shape(left), left.output)
    val raw = left.execute().map(_.asInstanceOf[UnsafeRow])
    val shapes = operator.toSpatialRDD(raw, bound).rawSpatialRDD.collect()
    val placeholders = shapes.toArray.toSeq.map(_.asInstanceOf[Geometry])
    assert(
      placeholders.exists(g => g.isEmpty && g.getUserData.asInstanceOf[UnsafeRow].getInt(0) == 0))
    val index = SpatialIndexExec(right, shape(right), IndexType.RTREE, false, false)
    val outer = BroadcastIndexJoinExec(
      left,
      index,
      shape(left),
      RightSide,
      LeftSide,
      LeftOuter,
      SpatialPredicate.INTERSECTS)
    val result = outer
      .executeCollect()
      .map(r => (r.getInt(0), if (r.isNullAt(2)) None else Some(r.getInt(2))))
      .toSet
    assert(result == Set((0, None), (1, Some(10))))
  }

  test(
    "projected raster joins use direct geometry conversion in partitioned and broadcast paths") {
    val raster = RasterConstructors.makeEmptyRaster(
      1,
      "D",
      2,
      2,
      0d,
      222650d,
      111325d,
      -111325d,
      0d,
      0d,
      3857)
    val left =
      scan("raster", RasterUDT(), (1 to 4).map(i => i -> RasterSerializer.serialize(raster)))
    val right =
      geometryScan(
        "geometry",
        (10 to 13).map(i => i -> point(111319.49079327357, 111325.1428663851, 3857)))
    assert(
      ids(RangeJoinExec(left, right, shape(left), shape(right), SpatialPredicate.INTERSECTS)
        .executeCollect()) == allRasterIds)
    assert(ids(broadcast(left, right, raster = true).executeCollect()) == allRasterIds)
    assert(
      ids(broadcast(left, right, raster = true, distance = Some(Literal(100d)))
        .executeCollect()) == allRasterIds)
    assert(
      ids(
        DistanceJoinExec(
          left,
          right,
          shape(left),
          shape(right),
          Literal(100d),
          true,
          SpatialPredicate.INTERSECTS,
          false).executeCollect()) == allRasterIds)
  }

  test("geography broadcast refinement retains S2 antimeridian containment") {
    val geographyType = GeographyType(4326)
    def value(wkt: String): Any = SpatialTypeSupport.serializeGeography(
      GeographyConstructors.geogFromWKT(wkt, 4326),
      geographyType)
    val left = scan(
      "left",
      geographyType,
      Seq(1 -> value("POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))")))
    val right = scan("right", geographyType, Seq(10 -> value("POINT (179 0)")))
    assert(
      ids(broadcast(left, right, geography = true, predicate = SpatialPredicate.CONTAINS)
        .executeCollect()) == Set((1, 10)))
  }

  test("explicit geometry UDT inputs retain partitioned and broadcast behavior") {
    val left = geometryScan("left", matchingPoints(1), GeometryUDT())
    val right = geometryScan("right", matchingPoints(10), GeometryUDT())
    assert(
      ids(RangeJoinExec(left, right, shape(left), shape(right), SpatialPredicate.INTERSECTS)
        .executeCollect()) == matchingPointIds)
    assert(ids(broadcast(left, right).executeCollect()) == matchingPointIds)
  }
}
