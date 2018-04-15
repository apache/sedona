/**
  * FILE: Catalog
  * Copyright (c) 2015 - 2018 GeoSpark Development Team
  *
  * MIT License
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package org.datasyslab.geosparksql.UDF

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.geosparksql.expressions._

import scala.reflect.runtime.{universe => ru}

object Catalog {
  val expressions:Seq[FunctionBuilder] = Seq(
    ST_PointFromText,
    ST_PolygonFromText,
    ST_LineStringFromText,
    ST_GeomFromWKT,
    ST_GeomFromWKB,
    ST_GeomFromGeoJSON,
    ST_Circle,
    ST_Point,
    ST_PolygonFromEnvelope,
    ST_Contains,
    ST_Intersects,
    ST_Within,
    ST_Distance,
    ST_ConvexHull,
    ST_Envelope,
    ST_Length,
    ST_Area,
    ST_Centroid,
    ST_Transform,
    ST_Intersection
  )

  val aggregateExpressions:Seq[UserDefinedAggregateFunction] = Seq(
    new ST_Union_Aggr,
    new ST_Envelope_Aggr
  )
}
