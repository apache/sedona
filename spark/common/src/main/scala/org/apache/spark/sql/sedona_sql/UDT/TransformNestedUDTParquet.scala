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
      case lr: LogicalRelation
          if lr.relation.isInstanceOf[HadoopFsRelation] &&
            lr.relation
              .asInstanceOf[HadoopFsRelation]
              .fileFormat
              .isInstanceOf[ParquetFileFormat] &&
            hasNestedGeometryUDT(lr.schema) =>
        val relation = lr.relation.asInstanceOf[HadoopFsRelation]

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

  /**
   * Checks if a schema contains nested GeometryUDT fields, meaning GeometryUDT instances that are
   * inside arrays, maps, or structs (i.e., not at the top level of the schema). Top-level
   * GeometryUDT fields (fields whose type is GeometryUDT directly in the StructType) are NOT
   * considered nested and do not trigger the transformation. This distinction is important
   * because the SPARK-48942 Parquet bug only affects nested UDTs, not top-level ones. Therefore,
   * this method returns true only if a GeometryUDT is found inside a container type, ensuring
   * that only affected fields are transformed.
   */
  private def hasNestedGeometryUDT(schema: StructType): Boolean = {
    schema.fields.exists(field => hasNestedGeometryUDTInType(field.dataType, isTopLevel = true))
  }

  /**
   * Recursively check if a data type contains nested GeometryUDT.
   * @param dataType
   *   the data type to check
   * @param isTopLevel
   *   true if this is a top-level field, false if nested inside a container
   * @return
   *   true if nested GeometryUDT is found, false otherwise
   */
  private def hasNestedGeometryUDTInType(dataType: DataType, isTopLevel: Boolean): Boolean = {
    dataType match {
      case _: GeometryUDT => !isTopLevel // GeometryUDT is "nested" only if NOT at top level
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
   * Transform a schema to handle nested UDT by processing each field. This preserves top-level
   * GeometryUDT fields while transforming nested ones to BinaryType.
   */
  private def transformSchemaForNestedUDT(schema: StructType): StructType = {
    StructType(schema.fields.map(field =>
      field.copy(dataType = transformDataType(field.dataType, isTopLevel = true))))
  }

  /**
   * Recursively transform data types based on nesting level.
   * @param dataType
   *   the data type to transform
   * @param isTopLevel
   *   true if this is a top-level field (preserves GeometryUDT), false if nested (converts
   *   GeometryUDT to BinaryType)
   * @return
   *   transformed data type
   */
  private def transformDataType(dataType: DataType, isTopLevel: Boolean): DataType = {
    dataType match {
      case _: GeometryUDT =>
        if (isTopLevel) dataType else BinaryType // Preserve at top-level, convert if nested

      case ArrayType(elementType, containsNull) =>
        ArrayType(transformDataType(elementType, isTopLevel = false), containsNull)

      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(
          transformDataType(keyType, isTopLevel = false),
          transformDataType(valueType, isTopLevel = false),
          valueContainsNull)

      case StructType(fields) =>
        StructType(fields.map(field =>
          field.copy(dataType = transformDataType(field.dataType, isTopLevel = false))))

      case udt: UserDefinedType[_] if !isTopLevel =>
        transformDataType(udt.sqlType, isTopLevel = false)

      case other => other
    }
  }
}
