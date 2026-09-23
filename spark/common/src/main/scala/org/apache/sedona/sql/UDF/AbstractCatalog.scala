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
package org.apache.sedona.sql.UDF

import org.apache.spark.sql.{SQLContext, SparkSession, functions}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.sedona_sql.expressions.{ST_Envelope_Aggr, ST_Intersection_Aggr, ST_Union_Aggr}
import org.locationtech.jts.geom.Geometry

import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport

import java.util.Locale
import scala.reflect.ClassTag
import scala.util.Try

abstract class AbstractCatalog {

  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)

  val expressions: Seq[FunctionDescription]

  val aggregateExpressions: Seq[Aggregator[Geometry, _, _]]

  private val sparkNativeFunctions =
    Set("st_asbinary", "st_geomfromwkb", "st_geogfromwkb", "st_srid", "st_setsrid")

  private def registeredExpressions: Seq[FunctionDescription] =
    if (SpatialTypeSupport.usesNativeTypes) {
      expressions.filterNot { case (identifier, _, _) =>
        sparkNativeFunctions.contains(identifier.funcName.toLowerCase(Locale.ROOT))
      }
    } else {
      expressions
    }

  private def registryIdentifier(identifier: FunctionIdentifier): FunctionIdentifier =
    if (SpatialTypeSupport.usesNativeTypes) {
      FunctionIdentifier(identifier.funcName, Some("builtin"), Some("system"))
    } else {
      identifier
    }

  protected def function[T <: Expression: ClassTag](defaultArgs: Any*): FunctionDescription = {
    val classTag = implicitly[ClassTag[T]]
    val constructor = classTag.runtimeClass.getConstructor(classOf[Seq[Expression]])
    val functionName = classTag.runtimeClass.getSimpleName
    val functionIdentifier = FunctionIdentifier(functionName)
    val expressionInfo = new ExpressionInfo(
      classTag.runtimeClass.getCanonicalName,
      functionIdentifier.database.orNull,
      functionName)

    def functionBuilder(expressions: Seq[Expression]): T = {
      val expr = constructor.newInstance(expressions).asInstanceOf[T]
      expr match {
        case e: ExpectsInputTypes =>
          val numParameters = e.inputTypes.size
          val numArguments = expressions.size
          if (numParameters == numArguments || numParameters == expr.children.size) expr
          else {
            val numUnspecifiedArgs = numParameters - numArguments
            if (numUnspecifiedArgs > 0) {
              if (numUnspecifiedArgs <= defaultArgs.size) {
                val args =
                  expressions ++ defaultArgs.takeRight(numUnspecifiedArgs).map(Literal(_))
                constructor.newInstance(args).asInstanceOf[T]
              } else {
                throw new IllegalArgumentException(s"function $functionName takes at least " +
                  s"${numParameters - defaultArgs.size} argument(s), $numArguments argument(s) specified")
              }
            } else {
              throw new IllegalArgumentException(
                s"function $functionName takes at most " +
                  s"$numParameters argument(s), $numArguments argument(s) specified")
            }
          }
        case _ => expr
      }
    }

    (functionIdentifier, expressionInfo, functionBuilder)
  }

  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    val registry = sparkSession.sessionState.functionRegistry
    registeredExpressions.foreach { case (identifier, expressionInfo, functionBuilder) =>
      val functionIdentifier = registryIdentifier(identifier)
      val shouldRegister = registry.lookupFunction(functionIdentifier) match {
        case Some(existingInfo) =>
          // Skip if Sedona already registered this function (e.g., SedonaContext.create called
          // twice). Overwrite if it's a Spark native function (e.g., Spark 4.1's ST_GeomFromWKB).
          // All expressions registered here live under org.apache.spark.sql.sedona_sql; the
          // org.apache.sedona prefix is kept defensively for future classes.
          !existingInfo.getClassName.startsWith("org.apache.sedona.") &&
          !existingInfo.getClassName.startsWith("org.apache.spark.sql.sedona_sql.")
        case None => true
      }
      if (shouldRegister) {
        val builder = SpatialTypeSupport.adaptFunction(functionBuilder)
        registry.registerFunction(functionIdentifier, expressionInfo, builder)
        FunctionRegistry.builtin.registerFunction(functionIdentifier, expressionInfo, builder)
      }
    }
    aggregateExpressions.foreach { f =>
      registerAggregateFunction(sparkSession, f.getClass.getSimpleName, f)
    }
    // Register aliases for *_Aggr functions with *_Agg suffix
    registerAggregateFunction(sparkSession, "ST_Envelope_Agg", new ST_Envelope_Aggr)
    registerAggregateFunction(sparkSession, "ST_Intersection_Agg", new ST_Intersection_Aggr)
    registerAggregateFunction(sparkSession, "ST_Union_Agg", new ST_Union_Aggr())
  }

  // Builds the catalyst aggregate for a UDAF so the FunctionRegistry.builtin entry can be the
  // real implementation rather than a non-invocable placeholder. The required Spark API is
  // package-private in Scala (public in bytecode) and moved between versions, hence reflection:
  // Spark 3.5 exposes UserDefinedAggregator.scalaAggregator(children), Spark 4.x the
  // ScalaAggregator companion's apply(udaf, children).
  private lazy val builtinAggregateBuilder
      : Option[(UserDefinedFunction, Seq[Expression]) => Expression] = {
    val spark3Builder = Try {
      val method = Class
        .forName("org.apache.spark.sql.expressions.UserDefinedAggregator")
        .getMethod("scalaAggregator", classOf[Seq[Expression]])
      (udaf: UserDefinedFunction, children: Seq[Expression]) =>
        method.invoke(udaf, children).asInstanceOf[Expression]
    }
    val spark4Builder = Try {
      val companionClass =
        Class.forName("org.apache.spark.sql.execution.aggregate.ScalaAggregator$")
      val companion = companionClass.getField("MODULE$").get(null)
      val method = companionClass.getMethods
        .find(m =>
          m.getName == "apply" && m.getParameterCount == 2 &&
            m.getParameterTypes()(0).getSimpleName == "UserDefinedAggregator")
        .get
      (udaf: UserDefinedFunction, children: Seq[Expression]) =>
        method.invoke(companion, udaf, children).asInstanceOf[Expression]
    }
    spark3Builder.orElse(spark4Builder).toOption
  }

  private def registerAggregateFunction(
      sparkSession: SparkSession,
      functionName: String,
      aggregator: Aggregator[Geometry, _, _]): Unit = {
    val functionIdentifier = registryIdentifier(FunctionIdentifier(functionName))
    val registry = sparkSession.sessionState.functionRegistry
    val udaf = functions.udaf(aggregator)
    // A session created before this JVM's first SedonaContext.create cloned
    // FunctionRegistry.builtin while it still held the non-invocable placeholder for this
    // aggregate, so mere existence in the session registry does not mean the real UDAF is
    // there (GH-3044). Probe the registered builder: the placeholder throws on any
    // invocation, a real entry builds an expression.
    val isInvocable = registry.functionExists(functionIdentifier) &&
      Try(registry.lookupFunction(functionIdentifier, Seq(Literal(null)))).isSuccess
    val builtinBuilder: FunctionBuilder = builtinAggregateBuilder match {
      case Some(build) => children => build(udaf, children)
      case None =>
        _ =>
          throw new UnsupportedOperationException(
            s"Aggregate function $functionName cannot be used as a regular function")
    }
    val builder = SpatialTypeSupport.adaptFunction(builtinBuilder)
    val info = new ExpressionInfo(aggregator.getClass.getCanonicalName, null, functionName)
    if (!isInvocable) {
      if (SpatialTypeSupport.usesNativeTypes) {
        // Register in the same builtin namespace as scalar extensions. A temporary UDAF would
        // otherwise live in system.session and bypass the native spatial type adapter.
        registry.registerFunction(functionIdentifier, info, builder)
      } else {
        sparkSession.udf.register(functionName, udaf)
      }
    }
    if (!FunctionRegistry.builtin.functionExists(functionIdentifier)) {
      FunctionRegistry.builtin.registerFunction(functionIdentifier, info, builder)
    }
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    registeredExpressions.foreach { case (functionIdentifier, _, _) =>
      sparkSession.sessionState.functionRegistry.dropFunction(
        registryIdentifier(functionIdentifier))
    }
    aggregateExpressions.foreach(f =>
      sparkSession.sessionState.functionRegistry.dropFunction(
        registryIdentifier(FunctionIdentifier(f.getClass.getSimpleName))))
    // Drop aliases for *_Aggr functions
    Seq("ST_Envelope_Agg", "ST_Intersection_Agg", "ST_Union_Agg").foreach { aliasName =>
      sparkSession.sessionState.functionRegistry.dropFunction(
        registryIdentifier(FunctionIdentifier(aliasName)))
    }
  }
}
