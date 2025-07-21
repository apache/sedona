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
package org.apache.sedona

import SedonaGeospatialLibrary.BACKTICKS_PATTERN
import org.apache.iceberg.expressions.{Expressions => IcebergExpressions}
import org.apache.iceberg.spark.geo.spi.GeospatialLibrary
import org.apache.iceberg.{Geography, expressions}
import org.apache.sedona.common.geometryObjects.{Geography => SedonaGeography}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.execution.datasources.{PushableColumn, PushableColumnBase}
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions._
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.Geometry

import java.util.regex.Pattern

class SedonaGeospatialLibrary extends GeospatialLibrary {
  override def getGeometryType: DataType = GeometryUDT

  override def getGeographyType: DataType = GeographyUDT

  override def fromGeometry(geometry: Geometry): AnyRef = GeometryUDT.serialize(geometry)

  override def toGeometry(datum: Any): Geometry = GeometryUDT.deserialize(datum)

  override def fromGeography(geography: Geography): AnyRef =
    GeographyUDT.serialize(new SedonaGeography(geography.geometry()))

  override def toGeography(datum: Any): Geography = {
    val sedonaGeography = GeographyUDT.deserialize(datum)
    new Geography(sedonaGeography.getGeometry)
  }

  override def isSpatialFilter(expression: Expression): Boolean = expression match {
    case pred: ST_Predicate => !pred.isInstanceOf[ST_Disjoint]
    case _ => false
  }

  override def translateToIceberg(expression: Expression): expressions.Expression = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = true)
    expression match {
      case ST_Intersects(_) | ST_Contains(_) | ST_Covers(_) | ST_Within(_) | ST_CoveredBy(_) =>
        val icebergExpr = {
          for ((name, value) <- resolveNameAndLiteral(expression.children, pushableColumn))
            yield IcebergExpressions.stIntersects(unquote(name), GeometryUDT.deserialize(value))
        }
        icebergExpr.orNull
      case _ => null
    }
  }

  private def unquote(attributeName: String) = {
    val matcher = BACKTICKS_PATTERN.matcher(attributeName)
    matcher.replaceAll("$2")
  }

  private def resolveNameAndLiteral(
      expressions: Seq[Expression],
      pushableColumn: PushableColumnBase): Option[(String, Any)] = {
    expressions match {
      case Seq(pushableColumn(name), Literal(v, _)) => Some(name, v)
      case Seq(Literal(v, _), pushableColumn(name)) => Some(name, v)
      case _ => None
    }
  }
}

object SedonaGeospatialLibrary {
  private val BACKTICKS_PATTERN = Pattern.compile("""([`])(.|$)""")
}
