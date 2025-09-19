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
package org.apache.spark.sql.sedona_sql.UDT

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._

/**
 * Catalyst rule that automatically transforms schemas with nested GeometryUDT to prevent
 * SPARK-48942 errors in Parquet reading.
 *
 * This rule detects LogicalRelations that use ParquetFileFormat and have nested GeometryUDT in
 * their schema, then transforms the schema to use BinaryType instead.
 */
class TransformNestedUDTParquet(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case lr @ LogicalRelation(relation: HadoopFsRelation, _, _, _)
          if relation.fileFormat.isInstanceOf[ParquetFileFormat] && hasNestedGeometryUDT(
            lr.schema) =>
        // Transform the schema to use BinaryType for nested GeometryUDT
        val transformedSchema = transformSchemaForNestedUDT(lr.schema)

        // Create new AttributeReferences with transformed data types
        val transformedAttributes = transformedSchema.fields.zipWithIndex.map {
          case (field, index) =>
            val originalAttr = lr.output(index)
            AttributeReference(field.name, field.dataType, field.nullable, field.metadata)(
              originalAttr.exprId,
              originalAttr.qualifier)
        }
        lr.copy(output = transformedAttributes)

      case other => other
    }
  }

  private def hasNestedGeometryUDT(schema: StructType): Boolean = {
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
   * Transform a schema to handle nested UDT by processing each top-level field. This preserves
   * top-level GeometryUDT fields while transforming nested ones to BinaryType.
   */
  private def transformSchemaForNestedUDT(schema: StructType): StructType = {
    StructType(
      schema.fields.map(field => field.copy(dataType = transformTopLevelUDT(field.dataType))))
  }

  /**
   * Transform a top-level field's data type, preserving GeometryUDT at the top level but
   * converting nested GeometryUDT to BinaryType.
   */
  private def transformTopLevelUDT(dataType: DataType): DataType = {
    dataType match {
      case ArrayType(elementType, containsNull) =>
        ArrayType(transformNestedUDTToBinary(elementType), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(
          transformNestedUDTToBinary(keyType),
          transformNestedUDTToBinary(valueType),
          valueContainsNull)
      case StructType(fields) =>
        StructType(
          fields.map(field => field.copy(dataType = transformNestedUDTToBinary(field.dataType))))
      case _: GeometryUDT => dataType // Preserve top-level GeometryUDT
      case other => other
    }
  }

  /**
   * Recursively transform nested data types, converting ALL GeometryUDT to BinaryType. This is
   * used for nested structures where GeometryUDT must be converted.
   */
  private def transformNestedUDTToBinary(dataType: DataType): DataType = {
    dataType match {
      case _: GeometryUDT => BinaryType
      case ArrayType(elementType, containsNull) =>
        ArrayType(transformNestedUDTToBinary(elementType), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(
          transformNestedUDTToBinary(keyType),
          transformNestedUDTToBinary(valueType),
          valueContainsNull)
      case StructType(fields) =>
        StructType(
          fields.map(field => field.copy(dataType = transformNestedUDTToBinary(field.dataType))))
      case udt: UserDefinedType[_] => transformNestedUDTToBinary(udt.sqlType)
      case other => other
    }
  }
}
