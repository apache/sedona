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

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.{SQLContext, SparkSession}

object UdfRegistrator {

  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    Catalog.expressions.foreach(f=>FunctionRegistry.builtin.registerFunction(f.getClass.getSimpleName.dropRight(1),f))
    Catalog.aggregateExpressions.foreach(f=>sparkSession.udf.register(f.getClass.getSimpleName,f))
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    Catalog.expressions.foreach(f=>FunctionRegistry.builtin.dropFunction(f.getClass.getSimpleName.dropRight(1)))
    Catalog.aggregateExpressions.foreach(f=>FunctionRegistry.builtin.dropFunction((f.getClass.getSimpleName)))
  }
}