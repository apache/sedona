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
package org.apache.spark.sql.execution.datasources.geoparquet.internal

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.types.{DataType, DecimalType, StructField, StructType}

object QueryExecutionErrors {
  def sparkUpgradeInReadingDatesError(
      format: String,
      config: String,
      option: String): RuntimeException = {
    new RuntimeException(
      s"Inconsistent behavior across version: read ancient datetime. format: $format, config: $config, option: $option")
  }

  def sparkUpgradeInWritingDatesError(format: String, config: String): RuntimeException = {
    new RuntimeException(
      s"Inconsistent behavior across version: write ancient datetime. format: $format, config: $config")
  }

  def failedToMergeIncompatibleSchemasError(
      left: StructType,
      right: StructType,
      e: Throwable): Throwable = {
    new RuntimeException(
      s"Failed to merge incompatible schemas: \nLeft: ${left.treeString}\nRight: ${right.treeString}",
      e)
  }

  def cannotReadFooterForFileError(file: Path, e: Exception): Throwable = {
    new RuntimeException(s"Cannot read footer for file: ${file.toString}.", e)
  }

  def foundDuplicateFieldInFieldIdLookupModeError(
      requiredId: Int,
      matchedFields: String): RuntimeException = {
    new RuntimeException(
      s"Found duplicate field for field id $requiredId in field id lookup mode. " +
        s"Matched fields: $matchedFields")
  }

  def foundDuplicateFieldInCaseInsensitiveModeError(
      requiredFieldName: String,
      matchedOrcFields: String): RuntimeException = {
    new RuntimeException(
      s"Found duplicate field for field name '$requiredFieldName' in case-insensitive mode. " +
        s"Matched fields: $matchedOrcFields")
  }

  def cannotCreateParquetConverterForTypeError(
      t: DecimalType,
      parquetType: String): RuntimeException = {
    cannotCreateParquetConverterForDecimalTypeError(t, parquetType)
  }

  def cannotCreateParquetConverterForDecimalTypeError(
      t: DecimalType,
      parquetType: String): RuntimeException = {
    new RuntimeException(
      s"Cannot create Parquet converter for DecimalType(${t.precision}, ${t.scale}) " +
        s"with Parquet type: $parquetType.")
  }

  def cannotCreateParquetConverterForDataTypeError(
      t: DataType,
      parquetType: String): RuntimeException = {
    new RuntimeException(
      s"Cannot create Parquet converter for data type: ${t.catalogString} with Parquet type: $parquetType.")
  }

  def failedMergingSchemaError(
      leftSchema: StructType,
      rightSchema: StructType,
      e: SparkException): Throwable = {
    new RuntimeException(
      s"Failed to merge schema: \nLeft: ${leftSchema.treeString}\nRight: ${rightSchema.treeString}",
      e)
  }
}

object QueryCompilationErrors {
  def failedToParseExistenceDefaultAsLiteral(
      fieldName: String,
      defaultValue: String): Throwable = {
    new RuntimeException(
      s"Failed to parse existence default value for field '$fieldName': '$defaultValue'.")
  }

  def illegalParquetTypeError(parquetType: String): Throwable = {
    new RuntimeException(s"Illegal Parquet type: $parquetType.")
  }

  def parquetTypeUnsupportedYetError(parquetType: String): Throwable = {
    new RuntimeException(s"Parquet type $parquetType is not supported yet")
  }

  def unrecognizedParquetTypeError(field: String): Throwable = {
    new RuntimeException(s"Unrecognized Parquet type for field: $field.")
  }

  def cannotConvertDataTypeToParquetTypeError(field: StructField): Throwable = {
    new RuntimeException(
      s"Cannot convert data type '${field.dataType.catalogString}' to Parquet type for field: ${field.name}.")
  }
}
