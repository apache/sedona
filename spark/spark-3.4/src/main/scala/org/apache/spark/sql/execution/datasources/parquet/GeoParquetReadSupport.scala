/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._

import java.time.ZoneId
import java.util.{Locale, Map => JMap}
import scala.collection.JavaConverters._

/**
 * A Parquet [[ReadSupport]] implementation for reading Parquet records as Catalyst
 * [[InternalRow]]s.
 *
 * The API interface of [[ReadSupport]] is a little bit over complicated because of historical
 * reasons.  In older versions of parquet-mr (say 1.6.0rc3 and prior), [[ReadSupport]] need to be
 * instantiated and initialized twice on both driver side and executor side.  The [[init()]] method
 * is for driver side initialization, while [[prepareForRead()]] is for executor side.  However,
 * starting from parquet-mr 1.6.0, it's no longer the case, and [[ReadSupport]] is only instantiated
 * and initialized on executor side.  So, theoretically, now it's totally fine to combine these two
 * methods into a single initialization method.  The only reason (I could think of) to still have
 * them here is for parquet-mr API backwards-compatibility.
 *
 * Due to this reason, we no longer rely on [[ReadContext]] to pass requested schema from [[init()]]
 * to [[prepareForRead()]], but use a private `var` for simplicity.
 */
class GeoParquetReadSupport (override val convertTz: Option[ZoneId],
                             enableVectorizedReader: Boolean,
                             datetimeRebaseMode: LegacyBehaviorPolicy.Value,
                             int96RebaseMode: LegacyBehaviorPolicy.Value)
  extends ParquetReadSupport with Logging {
  private var catalystRequestedSchema: StructType = _

  /**
   * Called on executor side before [[prepareForRead()]] and instantiating actual Parquet record
   * readers.  Responsible for figuring out Parquet requested schema used for column pruning.
   */
  override def init(context: InitContext): ReadContext = {
    val conf = context.getConfiguration
    catalystRequestedSchema = {
      val schemaString = conf.get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA)
      assert(schemaString != null, "Parquet requested schema not set.")
      StructType.fromString(schemaString)
    }

    val caseSensitive = conf.getBoolean(SQLConf.CASE_SENSITIVE.key,
      SQLConf.CASE_SENSITIVE.defaultValue.get)
    val schemaPruningEnabled = conf.getBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.defaultValue.get)
    val useFieldId = conf.getBoolean(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key,
      SQLConf.PARQUET_FIELD_ID_READ_ENABLED.defaultValue.get)
    val parquetFileSchema = context.getFileSchema
    val parquetClippedSchema = ParquetReadSupport.clipParquetSchema(parquetFileSchema,
      catalystRequestedSchema, caseSensitive, useFieldId)

    // We pass two schema to ParquetRecordMaterializer:
    // - parquetRequestedSchema: the schema of the file data we want to read
    // - catalystRequestedSchema: the schema of the rows we want to return
    // The reader is responsible for reconciling the differences between the two.
    val parquetRequestedSchema = if (schemaPruningEnabled && !enableVectorizedReader) {
      // Parquet-MR reader requires that parquetRequestedSchema include only those fields present
      // in the underlying parquetFileSchema. Therefore, we intersect the parquetClippedSchema
      // with the parquetFileSchema
      GeoParquetReadSupport.intersectParquetGroups(parquetClippedSchema, parquetFileSchema)
        .map(groupType => new MessageType(groupType.getName, groupType.getFields))
        .getOrElse(ParquetSchemaConverter.EMPTY_MESSAGE)
    } else {
      // Spark's vectorized reader only support atomic types currently. It also skip fields
      // in parquetRequestedSchema which are not present in the file.
      parquetClippedSchema
    }
    logDebug(
      s"""Going to read the following fields from the Parquet file with the following schema:
         |Parquet file schema:
         |$parquetFileSchema
         |Parquet clipped schema:
         |$parquetClippedSchema
         |Parquet requested schema:
         |$parquetRequestedSchema
         |Catalyst requested schema:
         |${catalystRequestedSchema.treeString}
       """.stripMargin)
    new ReadContext(parquetRequestedSchema, Map.empty[String, String].asJava)
  }
  /**
   * Called on executor side after [[init()]], before instantiating actual Parquet record readers.
   * Responsible for instantiating [[RecordMaterializer]], which is used for converting Parquet
   * records to Catalyst [[InternalRow]]s.
   */
  override def prepareForRead(
                               conf: Configuration,
                               keyValueMetaData: JMap[String, String],
                               fileSchema: MessageType,
                               readContext: ReadContext): RecordMaterializer[InternalRow] = {
    val parquetRequestedSchema = readContext.getRequestedSchema
    new GeoParquetRecordMaterializer(
      parquetRequestedSchema,
      GeoParquetReadSupport.expandUDT(catalystRequestedSchema),
      new GeoParquetToSparkSchemaConverter(keyValueMetaData, conf),
      convertTz,
      datetimeRebaseMode,
      int96RebaseMode)
  }
}

object GeoParquetReadSupport extends Logging {

  /**
   * Computes the structural intersection between two Parquet group types.
   * This is used to create a requestedSchema for ReadContext of Parquet-MR reader.
   * Parquet-MR reader does not support the nested field access to non-existent field
   * while parquet library does support to read the non-existent field by regular field access.
   */
  private def intersectParquetGroups(
                                      groupType1: GroupType, groupType2: GroupType): Option[GroupType] = {
    val fields =
      groupType1.getFields.asScala
        .filter(field => groupType2.containsField(field.getName))
        .flatMap {
          case field1: GroupType =>
            val field2 = groupType2.getType(field1.getName)
            if (field2.isPrimitive) {
              None
            } else {
              intersectParquetGroups(field1, field2.asGroupType)
            }
          case field1 => Some(field1)
        }

    if (fields.nonEmpty) {
      Some(groupType1.withNewFields(fields.asJava))
    } else {
      None
    }
  }

  def expandUDT(schema: StructType): StructType = {
    def expand(dataType: DataType): DataType = {
      dataType match {
        case t: ArrayType =>
          t.copy(elementType = expand(t.elementType))

        case t: MapType =>
          t.copy(
            keyType = expand(t.keyType),
            valueType = expand(t.valueType))

        case t: StructType =>
          val expandedFields = t.fields.map(f => f.copy(dataType = expand(f.dataType)))
          t.copy(fields = expandedFields)

        // Don't expand GeometryUDT types. We'll treat geometry columns specially in
        // GeoParquetRowConverter
        case t: GeometryUDT => t

        case t: UserDefinedType[_] =>
          t.sqlType

        case t =>
          t
      }
    }

    expand(schema).asInstanceOf[StructType]
  }
}
