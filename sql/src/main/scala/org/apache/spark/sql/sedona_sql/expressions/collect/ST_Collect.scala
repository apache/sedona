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
package org.apache.spark.sql.sedona_sql.expressions.collect

import org.apache.sedona.core.geometryObjects.Circle
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

import org.apache.spark.sql.catalyst.expressions.Expression

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.types.{ArrayType, _}
import org.locationtech.jts.geom._
import scala.collection.JavaConverters._

case class ST_Collect(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback {
  assert(inputExpressions.length >= 1)
  private val geomFactory = new GeometryFactory()

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val firstElement = inputExpressions.head

    firstElement.dataType match {
      case ArrayType(elementType, _) =>
        elementType match {
          case _: GeometryUDT =>
            val data = firstElement.eval(input).asInstanceOf[ArrayData]
            val numElements = data.numElements()
            val geomElements = (0 until numElements)
              .map(element => data.getArray(element))
              .filter(_ != null)
              .map(_.toGeometry)

            createMultiGeometry(geomElements)
          case _ => emptyCollection
        }
      case _ =>
        val geomElements =
          inputExpressions.map(_.toGeometry(input)).filter(_ != null)
        val length = geomElements.length
        if (length > 1) createMultiGeometry(geomElements)
        else if (length == 1)
          createMultiGeometryFromOneElement(
            geomElements.head
          ).toGenericArrayData
        else emptyCollection
    }
  }

  private def createMultiGeometry(geomElements: Seq[Geometry]) =
    geomFactory.buildGeometry(geomElements.asJava).toGenericArrayData

  private def emptyCollection =
    geomFactory.createGeometryCollection().toGenericArrayData

  private def createMultiGeometryFromOneElement(geom: Geometry): Geometry = {
    geom match {
      case circle: Circle                 => geomFactory.createGeometryCollection(Array(circle))
      case collection: GeometryCollection => collection
      case string: LineString =>
        geomFactory.createMultiLineString(Array(string))
      case point: Point     => geomFactory.createMultiPoint(Array(point))
      case polygon: Polygon => geomFactory.createMultiPolygon(Array(polygon))
      case _                => geomFactory.createGeometryCollection()
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

}
