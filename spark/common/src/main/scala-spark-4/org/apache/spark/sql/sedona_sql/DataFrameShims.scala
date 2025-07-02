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
package org.apache.spark.sql.sedona_sql

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.types.StructType

object DataFrameShims {

  private[sedona_sql] def wrapExpression[E <: Expression: ClassTag](args: Any*): Column = {
    wrapVarArgExpression[E](args)
  }

  private[sedona_sql] def wrapVarArgExpression[E <: Expression: ClassTag](arg: Seq[Any]): Column = {
    val runtimeClass = implicitly[ClassTag[E]].runtimeClass

    val colArgs = arg.map(_ match {
      case c: Column => c
      case s: String => Column(s)
      case x: Any => lit(x)
      case null => lit(null)
    })
    Column.fn(runtimeClass.getSimpleName(), colArgs :_*)
  }

  private[sedona_sql] def wrapAggregator[A <: UserDefinedAggregateFunction: ClassTag](arg: Any*): Column = {
    val runtimeClass = implicitly[ClassTag[A]].runtimeClass
    val colArgs = arg.map(_ match {
      case c: Column => c
      case s: String => Column(s)
      case x: Any => lit(x)
      case null => lit(null)
    })
    Column.fn(runtimeClass.getSimpleName(), colArgs :_*)
  }

  private[sedona_sql] def createDataFrame(
      sparkSession: SparkSession,
      rdd: RDD[InternalRow],
      schema: StructType): DataFrame = {
    sparkSession.asInstanceOf[ClassicSparkSession].internalCreateDataFrame(rdd, schema)
  }
}
