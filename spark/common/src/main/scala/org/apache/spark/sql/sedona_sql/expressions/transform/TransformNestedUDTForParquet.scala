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
package org.apache.spark.sql.sedona_sql.expressions.transform

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._

/**
 * Expression that transforms nested GeometryUDT to BinaryType to work around SPARK-48942.
 *
 * This expression recursively transforms the schema of nested structures containing GeometryUDT,
 * converting them to BinaryType for compatibility with Spark's vectorized Parquet reader.
 */
case class TransformNestedUDTForParquet(child: Expression) extends UnaryExpression {

  override def dataType: DataType = transformDataType(child.dataType)

  override def nullable: Boolean = child.nullable

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    // The actual data remains the same - only the schema changes
    value
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Generate code that passes through the value unchanged
    val childGen = child.genCode(ctx)
    ExprCode(code = childGen.code, isNull = childGen.isNull, value = childGen.value)
  }

  override protected def withNewChildInternal(
      newChild: Expression): TransformNestedUDTForParquet =
    copy(child = newChild)

  private def transformDataType(dataType: DataType): DataType = {
    dataType match {
      case ArrayType(elementType, containsNull) =>
        ArrayType(transformNestedType(elementType), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(transformNestedType(keyType), transformNestedType(valueType), valueContainsNull)
      case StructType(fields) =>
        StructType(
          fields.map(field => field.copy(dataType = transformNestedType(field.dataType))))
      case other => other
    }
  }

  private def transformNestedType(dataType: DataType): DataType = {
    dataType match {
      case _: GeometryUDT => BinaryType
      case ArrayType(elementType, containsNull) =>
        ArrayType(transformNestedType(elementType), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(transformNestedType(keyType), transformNestedType(valueType), valueContainsNull)
      case StructType(fields) =>
        StructType(
          fields.map(field => field.copy(dataType = transformNestedType(field.dataType))))
      case udt: UserDefinedType[_] => transformNestedType(udt.sqlType)
      case other => other
    }
  }
}
