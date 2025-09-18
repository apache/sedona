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
package org.apache.spark.sql.sedona_sql.utils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._

/**
 * Utility object to handle SPARK-48942 fix for nested GeometryUDT in Parquet files.
 *
 * This provides helper methods to transform schemas and handle the type mismatch between
 * GeometryUDT and BinaryType in nested structures.
 */
object ParquetNestedUDTFix {

  /**
   * Transforms a DataFrame's schema to use BinaryType instead of nested GeometryUDT to avoid
   * SPARK-48942 when writing to Parquet.
   */
  def transformSchemaForParquetWrite(schema: StructType): StructType = {
    StructType(schema.fields.map(field =>
      field.copy(dataType = transformDataTypeForParquet(field.dataType))))
  }

  /**
   * Recursively transforms data types, converting GeometryUDT to BinaryType in nested contexts.
   */
  private def transformDataTypeForParquet(dataType: DataType): DataType = {
    dataType match {
      // Don't transform top-level GeometryUDT, only nested ones
      case ArrayType(elementType, containsNull) =>
        ArrayType(transformNestedDataType(elementType), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(
          transformNestedDataType(keyType),
          transformNestedDataType(valueType),
          valueContainsNull)
      case StructType(fields) =>
        StructType(
          fields.map(field => field.copy(dataType = transformNestedDataType(field.dataType))))
      case other => other
    }
  }

  /**
   * Transforms nested data types, converting GeometryUDT to BinaryType.
   */
  private def transformNestedDataType(dataType: DataType): DataType = {
    dataType match {
      case _: GeometryUDT => BinaryType
      case ArrayType(elementType, containsNull) =>
        ArrayType(transformNestedDataType(elementType), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(
          transformNestedDataType(keyType),
          transformNestedDataType(valueType),
          valueContainsNull)
      case StructType(fields) =>
        StructType(
          fields.map(field => field.copy(dataType = transformNestedDataType(field.dataType))))
      case udt: UserDefinedType[_] => transformNestedDataType(udt.sqlType)
      case other => other
    }
  }

  /**
   * Checks if a schema contains nested GeometryUDT that would trigger SPARK-48942.
   */
  def hasNestedGeometryUDT(schema: StructType): Boolean = {
    schema.fields.exists(field => hasNestedGeometryUDTInType(field.dataType, isTopLevel = true))
  }

  private def hasNestedGeometryUDTInType(dataType: DataType, isTopLevel: Boolean): Boolean = {
    dataType match {
      case _: GeometryUDT => !isTopLevel
      case ArrayType(elementType, _) =>
        hasNestedGeometryUDTInType(elementType, isTopLevel = false)
      case MapType(keyType, valueType, _) =>
        hasNestedGeometryUDTInType(keyType, isTopLevel = false) ||
        hasNestedGeometryUDTInType(valueType, isTopLevel = false)
      case StructType(fields) =>
        fields.exists(field => hasNestedGeometryUDTInType(field.dataType, isTopLevel = false))
      case _ => false
    }
  }

  /**
   * Expression that transforms GeometryUDT to BinaryType for nested structures. This can be used
   * in DataFrame transformations.
   */
  case class TransformNestedUDTExpression(child: Expression) extends UnaryExpression {
    override def dataType: DataType = transformDataTypeForParquet(child.dataType)

    override def eval(input: InternalRow): Any = {
      val value = child.eval(input)
      // For now, return the value as-is since the serialization will handle it
      value
    }

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      child.genCode(ctx)
    }

    override protected def withNewChildInternal(
        newChild: Expression): TransformNestedUDTExpression =
      copy(child = newChild)
  }
}
