/**
  * FILE: UdfRegistrator
  * PATH: org.datasyslab.geosparksql.UDF.UdfRegistrator
  * Copyright (c) GeoSpark Development Team
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

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.geosparksql.expressions._

object UdfRegistrator {
  def resigterConstructors(sparkSession: SparkSession): Unit =
  {
    sparkSession.sessionState.functionRegistry.registerFunction("ST_PointFromText", ST_PointFromText)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_PolygonFromText", ST_PolygonFromText)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_LineStringFromText", ST_LineStringFromText)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_GeomFromWKT", ST_GeomFromWKT)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_GeomFromGeoJSON", ST_GeomFromGeoJSON)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Circle", ST_Circle)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Point", ST_Point)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_PolygonFromEnvelope", ST_PolygonFromEnvelope)
  }

  def registerPredicates(sparkSession: SparkSession): Unit =
  {
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Contains", ST_Contains)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Intersects", ST_Intersects)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Within", ST_Within)
  }

  def registerFunctions(sparkSession: SparkSession): Unit =
  {
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Distance", ST_Distance)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_ConvexHull", ST_ConvexHull)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Envelope", ST_Envelope)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Length", ST_Length)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Area", ST_Area)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Centroid", ST_Centroid)
    sparkSession.sessionState.functionRegistry.registerFunction("ST_Transform", ST_Transform)
  }

  def registerAggregateFunctions(sparkSession: SparkSession): Unit =
  {
    sparkSession.udf.register("ST_Envelope_Aggr", new ST_Envelope_Aggr)
    sparkSession.udf.register("ST_Union_Aggr", new ST_Union_Aggr)
  }

  def dropConstructors(sparkSession: SparkSession): Unit =
  {
    sparkSession.sessionState.functionRegistry.dropFunction("ST_PointFromText")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_PolygonFromText")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_LineStringFromText")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_GeomFromWKT")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_GeomFromGeoJSON")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Circle")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Point")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_PolygonFromEnvelope")
  }

  def dropPredicates(sparkSession: SparkSession): Unit =
  {
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Contains")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Intersects")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Within")
  }

  def dropFunctions(sparkSession: SparkSession): Unit =
  {
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Distance")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_ConvexHull")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Envelope")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Length")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Area")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Centroid")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Transform")
  }

  def dropAggregateFunctions(sparkSession: SparkSession): Unit =
  {
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Envelope_Aggr")
    sparkSession.sessionState.functionRegistry.dropFunction("ST_Union_Aggr")
  }

  def registerAll(sqlContext: SQLContext): Unit =
  {
    resigterConstructors(sqlContext.sparkSession)
    registerPredicates(sqlContext.sparkSession)
    registerFunctions(sqlContext.sparkSession)
    registerAggregateFunctions(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit =
  {
    resigterConstructors(sparkSession)
    registerPredicates(sparkSession)
    registerFunctions(sparkSession)
    registerAggregateFunctions(sparkSession)
  }

  def dropAll(sparkSession: SparkSession): Unit =
  {
    dropConstructors(sparkSession)
    dropPredicates(sparkSession)
    dropFunctions(sparkSession)
    dropAggregateFunctions(sparkSession)
  }
}
