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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.collect.GeomType.GeomTypeVal
import org.apache.spark.sql.sedona_sql.expressions.implicits.{GeometryEnhancer, InputExpressionEnhancer}
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, Point, Polygon}

import java.util

object GeomType extends Enumeration(1) {
  case class GeomTypeVal(empty: Function[Geometry, Geometry], multi: Function[util.List[Geometry], Geometry]) extends super.Val {}
  import scala.language.implicitConversions
  implicit def valueToGeomTypeVal(x: Value): GeomTypeVal = x.asInstanceOf[GeomTypeVal]

  def getGeometryType(geometry: Geometry): GeomTypeVal = {
    (geometry: Geometry) match {
      case (geometry: GeometryCollection) =>
        GeomType.apply(Range(0, geometry.getNumGeometries).map(i => geometry.getGeometryN(i)).map(geom => getGeometryType(geom).id).reduce(scala.math.max))
      case (geometry: Point) => point
      case (geometry: LineString) => line
      case (geometry: Polygon) => polygon
    }
  }

  val point = GeomTypeVal(geom => geom.getFactory.createMultiPoint(), geoms => geoms.get(0).getFactory().createMultiPoint(geoms.toArray(Array[Point]())))
  val line = GeomTypeVal(geom => geom.getFactory.createMultiLineString(), geoms => geoms.get(0).getFactory().createMultiLineString(geoms.toArray(Array[LineString]())))
  val polygon = GeomTypeVal(geom => geom.getFactory.createMultiPolygon(), geoms => geoms.get(0).getFactory().createMultiPolygon(geoms.toArray(Array[Polygon]())))
}

case class ST_CollectionExtract(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }

  override def nullable: Boolean = true

  def nullSafeEval(geometry: Geometry, geomType: GeomTypeVal): GenericArrayData = {
    val geometries : util.ArrayList[Geometry] = new util.ArrayList[Geometry]()
    filterGeometry(geometries, geometry, geomType);

    if (geometries.isEmpty()) {
      geomType.empty(geometry).toGenericArrayData
    }
    else{
      geomType.multi(geometries).toGenericArrayData
    }
  }

  def filterGeometry(geometries: util.ArrayList[Geometry], geometry: Geometry, geomType: GeomTypeVal):Unit = {
    (geometry: Geometry) match {
      case (geometry: GeometryCollection) =>
        Range(0, geometry.getNumGeometries).map(i => geometry.getGeometryN(i)).foreach(geom => filterGeometry(geometries, geom, geomType))
      case (geometry: Geometry) => {
        if(geomType==GeomType.getGeometryType(geometry))
          geometries.add(geometry)
      }

    }
  }
  override def eval(input: InternalRow): Any = {

    val geometry = inputExpressions.head.toGeometry(input)
    val geomType = if (inputExpressions.length == 2) {
      GeomType.apply(inputExpressions(1).eval(input).asInstanceOf[Int])
    } else {
      GeomType.getGeometryType(geometry)
    }

    (geometry) match {
      case (geometry: Geometry) => nullSafeEval(geometry, geomType)
      case _ => null
    }
  }
}
