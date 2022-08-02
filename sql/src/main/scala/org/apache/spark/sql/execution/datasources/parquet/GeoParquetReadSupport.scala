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
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

import java.time.ZoneId
import java.util
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
class GeoParquetReadSupport (
                              override val convertTz: Option[ZoneId],
                              enableVectorizedReader: Boolean,
                              datetimeRebaseSpec: RebaseSpec,
                              int96RebaseSpec: RebaseSpec)
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

    val parquetRequestedSchema = ParquetReadSupport.getRequestedSchema(
      context.getFileSchema, catalystRequestedSchema, conf, enableVectorizedReader)
    new ReadContext(parquetRequestedSchema, new util.HashMap[String, String]())
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
      ParquetReadSupport.expandUDT(catalystRequestedSchema),
      new GeoParquetToSparkSchemaConverter(conf),
      convertTz,
      datetimeRebaseSpec,
      int96RebaseSpec)
  }
}

object GeoParquetReadSupport extends Logging {
  private def clipParquetType(
                               parquetType: Type,
                               catalystType: DataType,
                               caseSensitive: Boolean,
                               useFieldId: Boolean): Type = {
    val newParquetType = catalystType match {
      case t: ArrayType if !isPrimitiveCatalystType(t.elementType) =>
        clipParquetListType(parquetType.asGroupType(), t.elementType, caseSensitive, useFieldId)

      case t: MapType
        if !isPrimitiveCatalystType(t.keyType) ||
          !isPrimitiveCatalystType(t.valueType) =>
        clipParquetMapType(
          parquetType.asGroupType(), t.keyType, t.valueType, caseSensitive, useFieldId)

      case t: StructType =>
        clipParquetGroup(parquetType.asGroupType(), t, caseSensitive, useFieldId)

      case _ =>
        parquetType
    }

    if (useFieldId && parquetType.getId != null) {
      newParquetType.withId(parquetType.getId.intValue())
    } else {
      newParquetType
    }
  }

  /**
   * Whether a Catalyst [[DataType]] is primitive.  Primitive [[DataType]] is not equivalent to
   * [[AtomicType]].  For example, [[CalendarIntervalType]] is primitive, but it's not an
   * [[AtomicType]].
   */
  private def isPrimitiveCatalystType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType | _: MapType | _: StructType => false
      case _ => true
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[ArrayType]].  The element type
   * of the [[ArrayType]] should also be a nested type, namely an [[ArrayType]], a [[MapType]], or a
   * [[StructType]].
   */
  private def clipParquetListType(
                                   parquetList: GroupType,
                                   elementType: DataType,
                                   caseSensitive: Boolean,
                                   useFieldId: Boolean): Type = {
    assert(!isPrimitiveCatalystType(elementType))
    if (parquetList.getLogicalTypeAnnotation == null &&
      parquetList.isRepetition(Repetition.REPEATED)) {
      clipParquetType(parquetList, elementType, caseSensitive, useFieldId)
    } else {
      assert(
        parquetList.getLogicalTypeAnnotation.isInstanceOf[ListLogicalTypeAnnotation],
        "Invalid Parquet schema. " +
          "Logical type annotation of annotated Parquet lists must be ListLogicalTypeAnnotation: " +
          parquetList.toString)

      assert(
        parquetList.getFieldCount == 1 && parquetList.getType(0).isRepetition(Repetition.REPEATED),
        "Invalid Parquet schema. " +
          "LIST-annotated group should only have exactly one repeated field: " +
          parquetList)

      assert(!parquetList.getType(0).isPrimitive)

      val repeatedGroup = parquetList.getType(0).asGroupType()
      if (
        repeatedGroup.getFieldCount > 1 ||
          repeatedGroup.getName == "array" ||
          repeatedGroup.getName == parquetList.getName + "_tuple"
      ) {
        Types
          .buildGroup(parquetList.getRepetition)
          .as(LogicalTypeAnnotation.listType())
          .addField(clipParquetType(repeatedGroup, elementType, caseSensitive, useFieldId))
          .named(parquetList.getName)
      } else {
        val newRepeatedGroup = Types
          .repeatedGroup()
          .addField(
            clipParquetType(
              repeatedGroup.getType(0), elementType, caseSensitive, useFieldId))
          .named(repeatedGroup.getName)

        val newElementType = if (useFieldId && repeatedGroup.getId != null) {
          newRepeatedGroup.withId(repeatedGroup.getId.intValue())
        } else {
          newRepeatedGroup
        }
        Types
          .buildGroup(parquetList.getRepetition)
          .as(LogicalTypeAnnotation.listType())
          .addField(newElementType)
          .named(parquetList.getName)
      }
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[MapType]].  Either key type or
   * value type of the [[MapType]] must be a nested type, namely an [[ArrayType]], a [[MapType]], or
   * a [[StructType]].
   */
  private def clipParquetMapType(
                                  parquetMap: GroupType,
                                  keyType: DataType,
                                  valueType: DataType,
                                  caseSensitive: Boolean,
                                  useFieldId: Boolean): GroupType = {
    assert(!isPrimitiveCatalystType(keyType) || !isPrimitiveCatalystType(valueType))

    val repeatedGroup = parquetMap.getType(0).asGroupType()
    val parquetKeyType = repeatedGroup.getType(0)
    val parquetValueType = repeatedGroup.getType(1)

    val clippedRepeatedGroup = {
      val newRepeatedGroup = Types
        .repeatedGroup()
        .as(repeatedGroup.getLogicalTypeAnnotation)
        .addField(clipParquetType(parquetKeyType, keyType, caseSensitive, useFieldId))
        .addField(clipParquetType(parquetValueType, valueType, caseSensitive, useFieldId))
        .named(repeatedGroup.getName)
      if (useFieldId && repeatedGroup.getId != null) {
        newRepeatedGroup.withId(repeatedGroup.getId.intValue())
      } else {
        newRepeatedGroup
      }
    }

    Types
      .buildGroup(parquetMap.getRepetition)
      .as(parquetMap.getLogicalTypeAnnotation)
      .addField(clippedRepeatedGroup)
      .named(parquetMap.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return A clipped [[GroupType]], which has at least one field.
   * @note Parquet doesn't allow creating empty [[GroupType]] instances except for empty
   *       [[MessageType]].  Because it's legal to construct an empty requested schema for column
   *       pruning.
   */
  private def clipParquetGroup(
                                parquetRecord: GroupType,
                                structType: StructType,
                                caseSensitive: Boolean,
                                useFieldId: Boolean): GroupType = {
    val clippedParquetFields =
      clipParquetGroupFields(parquetRecord, structType, caseSensitive, useFieldId)
    Types
      .buildGroup(parquetRecord.getRepetition)
      .as(parquetRecord.getLogicalTypeAnnotation)
      .addFields(clippedParquetFields: _*)
      .named(parquetRecord.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return A list of clipped [[GroupType]] fields, which can be empty.
   */
  private def clipParquetGroupFields(
                                      parquetRecord: GroupType,
                                      structType: StructType,
                                      caseSensitive: Boolean,
                                      useFieldId: Boolean): Seq[Type] = {
    val toParquet = new SparkToGeoParquetSchemaConverter(
      writeLegacyParquetFormat = false, useFieldId = useFieldId)
    lazy val caseSensitiveParquetFieldMap =
      parquetRecord.getFields.asScala.map(f => f.getName -> f).toMap
    lazy val caseInsensitiveParquetFieldMap =
      parquetRecord.getFields.asScala.groupBy(_.getName.toLowerCase(Locale.ROOT))
    lazy val idToParquetFieldMap =
      parquetRecord.getFields.asScala.filter(_.getId != null).groupBy(f => f.getId.intValue())

    def matchCaseSensitiveField(f: StructField): Type = {
      caseSensitiveParquetFieldMap
        .get(f.name)
        .map(clipParquetType(_, f.dataType, caseSensitive, useFieldId))
        .getOrElse(toParquet.convertField(f))
    }

    def matchCaseInsensitiveField(f: StructField): Type = {
      caseInsensitiveParquetFieldMap
        .get(f.name.toLowerCase(Locale.ROOT))
        .map { parquetTypes =>
          if (parquetTypes.size > 1) {
            val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
            throw QueryExecutionErrors.foundDuplicateFieldInCaseInsensitiveModeError(
              f.name, parquetTypesString)
          } else {
            clipParquetType(parquetTypes.head, f.dataType, caseSensitive, useFieldId)
          }
        }.getOrElse(toParquet.convertField(f))
    }

    def matchIdField(f: StructField): Type = {
      val fieldId = ParquetUtils.getFieldId(f)
      idToParquetFieldMap
        .get(fieldId)
        .map { parquetTypes =>
          if (parquetTypes.size > 1) {
            val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
            throw QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError(
              fieldId, parquetTypesString)
          } else {
            clipParquetType(parquetTypes.head, f.dataType, caseSensitive, useFieldId)
          }
        }.getOrElse {
        toParquet.convertField(f.copy(name = ParquetReadSupport.generateFakeColumnName))
      }
    }

    val shouldMatchById = useFieldId && ParquetUtils.hasFieldIds(structType)
    structType.map { f =>
      if (shouldMatchById && ParquetUtils.hasFieldId(f)) {
        matchIdField(f)
      } else if (caseSensitive) {
        matchCaseSensitiveField(f)
      } else {
        matchCaseInsensitiveField(f)
      }
    }
  }
}
