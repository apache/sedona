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
package org.apache.sedona.sql.datasources.spider

import org.apache.sedona.common.spider.Generator
import org.apache.sedona.common.spider.GeneratorFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.util.AffineTransformation

class SpiderPartitionReader(partition: SpiderPartition) extends PartitionReader[InternalRow] {
  private val random: java.util.Random = new java.util.Random(partition.seed)
  private val affine: Option[AffineTransformation] = partition.transform.toJTS
  private val generator: Generator =
    GeneratorFactory.create(partition.distribution, random, partition.opts)
  private var count: Long = 0
  private var currentGeometry: Geometry = _

  override def next(): Boolean = {
    if (count < partition.numRows) {
      val geom = generator.next()
      currentGeometry = affine.map(_.transform(geom)).getOrElse(geom)
      count += 1
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    InternalRow(partition.startIndex + count, GeometryUDT.serialize(currentGeometry))
  }

  override def close(): Unit = {}
}
