/*
 * FILE: UdfRegistrator.scala
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
 *
 */
package org.datasyslab.geosparksql.UDF

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.geosparksql.expressions._
import org.apache.spark.sql.{SQLContext, SparkSession}

object UdfRegistrator {
  def resigterConstructors(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_PointFromText", ST_PointFromText)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_PolygonFromText", ST_PolygonFromText)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_LineStringFromText", ST_LineStringFromText)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_GeomFromWKT", ST_GeomFromWKT)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_GeomFromGeoJSON", ST_GeomFromGeoJSON)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Circle", ST_Circle)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Point", ST_Point)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_PolygonFromEnvelope", ST_PolygonFromEnvelope)
  }

  def registerPredicates(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Contains", ST_Contains)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Intersects", ST_Intersects)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Within", ST_Within)
  }

  def registerFunctions(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Distance", ST_Distance)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_ConvexHull", ST_ConvexHull)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Envelope", ST_Envelope)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Length", ST_Length)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Area", ST_Area)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Centroid", ST_Centroid)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Transform", ST_Transform)
    sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction("ST_Intersection", ST_Intersection)
  }

  def registerAggregateFunctions(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("ST_Envelope_Aggr", new ST_Envelope_Aggr)
    sparkSession.udf.register("ST_Union_Aggr", new ST_Union_Aggr)
  }

  def dropConstructors(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_PointFromText"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_PolygonFromText"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_LineStringFromText"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_GeomFromWKT"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_GeomFromGeoJSON"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Circle"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Point"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_PolygonFromEnvelope"))
  }

  def dropPredicates(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Contains"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Intersects"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Within"))
  }

  def dropFunctions(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Distance"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_ConvexHull"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Envelope"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Length"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Area"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Centroid"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Transform"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Intersection"))
  }

  def dropAggregateFunctions(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Envelope_Aggr"))
    sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier("ST_Union_Aggr"))
  }

  def registerAll(sqlContext: SQLContext): Unit = {
    resigterConstructors(sqlContext.sparkSession)
    registerPredicates(sqlContext.sparkSession)
    registerFunctions(sqlContext.sparkSession)
    registerAggregateFunctions(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    resigterConstructors(sparkSession)
    registerPredicates(sparkSession)
    registerFunctions(sparkSession)
    registerAggregateFunctions(sparkSession)
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    dropConstructors(sparkSession)
    dropPredicates(sparkSession)
    dropFunctions(sparkSession)
    dropAggregateFunctions(sparkSession)
  }
}
