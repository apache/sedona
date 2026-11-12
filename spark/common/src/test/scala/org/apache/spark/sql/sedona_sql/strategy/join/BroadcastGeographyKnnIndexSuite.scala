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

import org.apache.sedona.common.S2Geography.Geography
import org.apache.sedona.common.geography.{Constructors, Functions, GeographyKnnIndex}
import org.apache.sedona.common.geography.GeographyKnnIndex.Neighbor
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/**
 * How the rows of a geography KNN join travel between the driver and the executors. The search
 * itself is covered by GeographyKnnIndexTest in the common module.
 */
class BroadcastGeographyKnnIndexSuite extends AnyFunSuite {

  private def geography(wkt: String): Geography = Constructors.geogFromWKT(wkt, 4326)

  private def row(id: Int): UnsafeRow =
    UnsafeProjection.create(Array[DataType](IntegerType)).apply(InternalRow(id)).copy()

  private def ids(neighbors: java.util.List[Neighbor[UnsafeRow]]): Seq[Int] =
    neighbors.asScala.map(_.getItem.getInt(0)).toSeq

  test("an input is only created for a non-empty geography") {
    assert(GeographyKnnInput.from(null, row(0)).isEmpty)
    assert(GeographyKnnInput.from(geography("POINT EMPTY"), row(0)).isEmpty)
    val input = GeographyKnnInput.from(geography("POINT(1 2)"), row(7)).get
    assert(input.srid === 4326)
    assert(Functions.distance(input.geography, geography("POINT(1 2)")) === 0.0)
    assert(input.row.getInt(0) === 7)
  }

  for ((name, serializer) <- Seq[(String, Serializer)](
      ("Java", new JavaSerializer(new SparkConf())),
      ("Kryo", new KryoSerializer(new SparkConf())))) {

    test(s"the broadcast index can be searched after $name serialization") {
      val inputs = Seq(
        "POINT(3 0)",
        "POLYGON((1 -1, 2 -1, 2 1, 1 1, 1 -1))",
        "POINT(-2 0)").zipWithIndex.flatMap { case (wkt, id) =>
        GeographyKnnInput.from(geography(wkt), row(id))
      }.toArray
      val instance = serializer.newInstance()
      val copy = instance.deserialize[BroadcastGeographyKnnIndex](
        instance.serialize(new BroadcastGeographyKnnIndex(inputs)))
      val neighbors = copy.index.nearest(geography("POINT(0 0)"), 2, false)
      assert(ids(neighbors) === Seq(1, 2))
    }

    test(s"the neighbours of a query can be merged after $name serialization") {
      // The per-partition neighbours of a broadcast query are shuffled before they are merged.
      val index = GeographyKnnIndex
        .builder[UnsafeRow]()
        .add(geography("POINT(3 0)"), row(0))
        .add(geography("POINT(1 0)"), row(1))
        .build()
      val neighbors = index.nearest(geography("POINT(0 0)"), 2, false)
      val instance = serializer.newInstance()
      val copy = instance.deserialize[java.util.List[Neighbor[UnsafeRow]]](
        instance.serialize[java.util.List[Neighbor[UnsafeRow]]](neighbors))
      val other = java.util.Collections.singletonList(new Neighbor[UnsafeRow](0.0, row(2)))
      assert(ids(GeographyKnnIndex.merge(copy, other, 2, false)) === Seq(2, 1))
      assert(copy.asScala.map(_.getDistance) === neighbors.asScala.map(_.getDistance))
    }
  }
}
