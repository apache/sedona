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
import org.apache.parquet.io.{ColumnIO, GroupColumnIO, PrimitiveColumnIO}
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._
import org.apache.parquet.schema._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

import java.util.Locale

/**
 * This converter class is used to convert Parquet [[MessageType]] to Spark SQL [[StructType]]
 * (via the `convert` method) as well as [[ParquetColumn]] (via the `convertParquetColumn`
 * method). The latter contains richer information about the Parquet type, including its
 * associated repetition & definition level, column path, column descriptor etc.
 *
 * Parquet format backwards-compatibility rules are respected when converting Parquet
 * [[MessageType]] schemas.
 *
 * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 *
 * @param assumeBinaryIsString Whether unannotated BINARY fields should be assumed to be Spark SQL
 *        [[StringType]] fields.
 * @param assumeInt96IsTimestamp Whether unannotated INT96 fields should be assumed to be Spark SQL
 *        [[TimestampType]] fields.
 * @param caseSensitive Whether use case sensitive analysis when comparing Spark catalyst read
 *                      schema with Parquet schema
 */
class GeoParquetToSparkSchemaConverter(
                                        assumeBinaryIsString: Boolean = SQLConf.PARQUET_BINARY_AS_STRING.defaultValue.get,
                                        assumeInt96IsTimestamp: Boolean = SQLConf.PARQUET_INT96_AS_TIMESTAMP.defaultValue.get,
                                        caseSensitive: Boolean = SQLConf.CASE_SENSITIVE.defaultValue.get)
  extends ParquetToSparkSchemaConverter {

  def this(conf: SQLConf) = this(
    assumeBinaryIsString = conf.isParquetBinaryAsString,
    assumeInt96IsTimestamp = conf.isParquetINT96AsTimestamp,
    caseSensitive = conf.caseSensitiveAnalysis)

  def this(conf: Configuration) = this(
    assumeBinaryIsString = conf.get(SQLConf.PARQUET_BINARY_AS_STRING.key).toBoolean,
    assumeInt96IsTimestamp = conf.get(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key).toBoolean,
    caseSensitive = conf.get(SQLConf.CASE_SENSITIVE.key).toBoolean)


  /**
   * Converts Parquet [[MessageType]] `parquetSchema` to a Spark SQL [[StructType]].
   */
  override def convert(parquetSchema: MessageType): StructType = super.convert(parquetSchema)

  private def convertInternal(
                               groupColumn: GroupColumnIO,
                               sparkReadSchema: Option[StructType] = None): ParquetColumn = {
    val schemaMapOpt = sparkReadSchema.map { schema =>
      schema.map(f => normalizeFieldName(f.name) -> f).toMap
    }

    val converted = (0 until groupColumn.getChildrenCount).map { i =>
      val field = groupColumn.getChild(i)
      val fieldFromReadSchema = schemaMapOpt.flatMap { schemaMap =>
        schemaMap.get(normalizeFieldName(field.getName))
      }
      var fieldReadType = fieldFromReadSchema.map(_.dataType)

      if (field.getType.getRepetition == REPEATED) {
        fieldReadType = fieldReadType.flatMap {
          case at: ArrayType => Some(at.elementType)
          case _ =>
            throw QueryCompilationErrors.illegalParquetTypeError(groupColumn.toString)
        }
      }

      val convertedField = convertField(field, fieldReadType)
      val fieldName = fieldFromReadSchema.map(_.name).getOrElse(field.getType.getName)

      field.getType.getRepetition match {
        case OPTIONAL | REQUIRED =>
          val nullable = field.getType.getRepetition == OPTIONAL
          (StructField(fieldName, convertedField.sparkType, nullable = nullable),
            convertedField)

        case REPEATED =>
          val arrayType = ArrayType(convertedField.sparkType, containsNull = false)
          (StructField(fieldName, arrayType, nullable = false),
            ParquetColumn(arrayType, None, convertedField.repetitionLevel - 1,
              convertedField.definitionLevel - 1, required = true, convertedField.path,
              Seq(convertedField.copy(required = true))))
      }
    }

    ParquetColumn(StructType(converted.map(_._1)), groupColumn, converted.map(_._2))
  }

  private def normalizeFieldName(name: String): String =
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)

  /**
   * Converts a Parquet [[Type]] to a [[ParquetColumn]] which wraps a Spark SQL [[DataType]] with
   * additional information such as the Parquet column's repetition & definition level, column
   * path, column descriptor etc.
   */
  override def convertField(
                             field: ColumnIO,
                             sparkReadType: Option[DataType] = None): ParquetColumn = field match {
    case primitiveColumn: PrimitiveColumnIO => convertPrimitiveField(primitiveColumn, sparkReadType)
    case groupColumn: GroupColumnIO => convertGroupField(groupColumn, sparkReadType)
  }

  private def convertPrimitiveField(
                                     primitiveColumn: PrimitiveColumnIO,
                                     sparkReadType: Option[DataType] = None): ParquetColumn = {
    val parquetType = primitiveColumn.getType.asPrimitiveType()
    val typeAnnotation = primitiveColumn.getType.getLogicalTypeAnnotation
    val typeName = primitiveColumn.getPrimitive
    val fieldName = primitiveColumn.getName

    def typeString =
      if (typeAnnotation == null) s"$typeName" else s"$typeName ($typeAnnotation)"

    def typeNotImplemented() =
      throw QueryCompilationErrors.parquetTypeUnsupportedYetError(typeString)

    def illegalType() =
      throw QueryCompilationErrors.illegalParquetTypeError(typeString)

    def makeDecimalType(maxPrecision: Int = -1): DecimalType = {
      val decimalLogicalTypeAnnotation = typeAnnotation
        .asInstanceOf[DecimalLogicalTypeAnnotation]
      val precision = decimalLogicalTypeAnnotation.getPrecision
      val scale = decimalLogicalTypeAnnotation.getScale

      ParquetSchemaConverter.checkConversionRequirement(
        maxPrecision == -1 || 1 <= precision && precision <= maxPrecision,
        s"Invalid decimal precision: $typeName cannot store $precision digits (max $maxPrecision)")

      DecimalType(precision, scale)
    }

    val sparkType = sparkReadType.getOrElse(typeName match {
      case BOOLEAN => BooleanType

      case FLOAT => FloatType

      case DOUBLE => DoubleType

      case INT32 =>
        typeAnnotation match {
          case intTypeAnnotation: IntLogicalTypeAnnotation if intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 8 => ByteType
              case 16 => ShortType
              case 32 => IntegerType
              case _ => illegalType()
            }
          case null => IntegerType
          case _: DateLogicalTypeAnnotation => DateType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType(Decimal.MAX_INT_DIGITS)
          case intTypeAnnotation: IntLogicalTypeAnnotation if !intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 8 => ShortType
              case 16 => IntegerType
              case 32 => LongType
              case _ => illegalType()
            }
          case t: TimestampLogicalTypeAnnotation if t.getUnit == TimeUnit.MILLIS =>
            typeNotImplemented()
          case _ => illegalType()
        }

      case INT64 =>
        typeAnnotation match {
          case intTypeAnnotation: IntLogicalTypeAnnotation if intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 64 => LongType
              case _ => illegalType()
            }
          case null => LongType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType(Decimal.MAX_LONG_DIGITS)
          case intTypeAnnotation: IntLogicalTypeAnnotation if !intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              // The precision to hold the largest unsigned long is:
              // `java.lang.Long.toUnsignedString(-1).length` = 20
              case 64 => DecimalType(20, 0)
              case _ => illegalType()
            }
          case timestamp: TimestampLogicalTypeAnnotation
            if timestamp.getUnit == TimeUnit.MICROS || timestamp.getUnit == TimeUnit.MILLIS =>
            if (timestamp.isAdjustedToUTC) {
              TimestampType
            } else {
              // SPARK-38829: Remove TimestampNTZ type support in Parquet for Spark 3.3
              if (Utils.isTesting) TimestampNTZType else TimestampType
            }
          case _ => illegalType()
        }

      case INT96 =>
        ParquetSchemaConverter.checkConversionRequirement(
          assumeInt96IsTimestamp,
          "INT96 is not supported unless it's interpreted as timestamp. " +
            s"Please try to set ${SQLConf.PARQUET_INT96_AS_TIMESTAMP.key} to true.")
        TimestampType

      case BINARY =>
        typeAnnotation match {
          case _: StringLogicalTypeAnnotation | _: EnumLogicalTypeAnnotation |
               _: JsonLogicalTypeAnnotation => StringType
          case null if GeoParquetSchemaConverter.checkGeomFieldName(fieldName) => GeometryUDT
          case null if assumeBinaryIsString => StringType
          case null => BinaryType
          case _: BsonLogicalTypeAnnotation => BinaryType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType()
          case _ => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        typeAnnotation match {
          case _: DecimalLogicalTypeAnnotation =>
            makeDecimalType(Decimal.maxPrecisionForBytes(parquetType.getTypeLength))
          case _: IntervalLogicalTypeAnnotation => typeNotImplemented()
          case _ => illegalType()
        }

      case _ => illegalType()
    })

    ParquetColumn(sparkType, primitiveColumn)
  }

  private def convertGroupField(
                                 groupColumn: GroupColumnIO,
                                 sparkReadType: Option[DataType] = None): ParquetColumn = {
    val field = groupColumn.getType.asGroupType()
    Option(field.getLogicalTypeAnnotation).fold(
      convertInternal(groupColumn, sparkReadType.map(_.asInstanceOf[StructType]))) {

      case _: ListLogicalTypeAnnotation =>
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1, s"Invalid list type $field")
        ParquetSchemaConverter.checkConversionRequirement(
          sparkReadType.forall(_.isInstanceOf[ArrayType]),
          s"Invalid Spark read type: expected $field to be list type but found $sparkReadType")

        val repeated = groupColumn.getChild(0)
        val repeatedType = repeated.getType
        ParquetSchemaConverter.checkConversionRequirement(
          repeatedType.isRepetition(REPEATED), s"Invalid list type $field")
        val sparkReadElementType = sparkReadType.map(_.asInstanceOf[ArrayType].elementType)

        if (isElementType(repeatedType, field.getName)) {
          var converted = convertField(repeated, sparkReadElementType)
          val convertedType = sparkReadElementType.getOrElse(converted.sparkType)

          if (repeatedType.isPrimitive) converted = converted.copy(required = true)

          ParquetColumn(ArrayType(convertedType, containsNull = false),
            groupColumn, Seq(converted))
        } else {
          val element = repeated.asInstanceOf[GroupColumnIO].getChild(0)
          val converted = convertField(element, sparkReadElementType)
          val convertedType = sparkReadElementType.getOrElse(converted.sparkType)
          val optional = element.getType.isRepetition(OPTIONAL)
          ParquetColumn(ArrayType(convertedType, containsNull = optional),
            groupColumn, Seq(converted))
        }

      case _: MapLogicalTypeAnnotation | _: MapKeyValueTypeAnnotation =>
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1 && !field.getType(0).isPrimitive,
          s"Invalid map type: $field")
        ParquetSchemaConverter.checkConversionRequirement(
          sparkReadType.forall(_.isInstanceOf[MapType]),
          s"Invalid Spark read type: expected $field to be map type but found $sparkReadType")

        val keyValue = groupColumn.getChild(0).asInstanceOf[GroupColumnIO]
        val keyValueType = keyValue.getType.asGroupType()
        ParquetSchemaConverter.checkConversionRequirement(
          keyValueType.isRepetition(REPEATED) && keyValueType.getFieldCount == 2,
          s"Invalid map type: $field")

        val key = keyValue.getChild(0)
        val value = keyValue.getChild(1)
        val sparkReadKeyType = sparkReadType.map(_.asInstanceOf[MapType].keyType)
        val sparkReadValueType = sparkReadType.map(_.asInstanceOf[MapType].valueType)
        val convertedKey = convertField(key, sparkReadKeyType)
        val convertedValue = convertField(value, sparkReadValueType)
        val convertedKeyType = sparkReadKeyType.getOrElse(convertedKey.sparkType)
        val convertedValueType = sparkReadValueType.getOrElse(convertedValue.sparkType)
        val valueOptional = value.getType.isRepetition(OPTIONAL)
        ParquetColumn(
          MapType(convertedKeyType, convertedValueType,
            valueContainsNull = valueOptional),
          groupColumn, Seq(convertedKey, convertedValue))
      case _ =>
        throw QueryCompilationErrors.unrecognizedParquetTypeError(field.toString)
    }
  }

  // scalastyle:off
  // Here we implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // scalastyle:on
  override def isElementType(repeatedType: Type, parentName: String): Boolean = super.isElementType(repeatedType, parentName)
}

/**
 * This converter class is used to convert Spark SQL [[StructType]] to Parquet [[MessageType]].
 *
 * @param writeLegacyParquetFormat Whether to use legacy Parquet format compatible with Spark 1.4
 *        and prior versions when converting a Catalyst [[StructType]] to a Parquet [[MessageType]].
 *        When set to false, use standard format defined in parquet-format spec.  This argument only
 *        affects Parquet write path.
 * @param outputTimestampType which parquet timestamp type to use when writing.
 * @param useFieldId whether we should include write field id to Parquet schema. Set this to false
 *        via `spark.sql.parquet.fieldId.write.enabled = false` to disable writing field ids.
 */
class SparkToGeoParquetSchemaConverter(
                                        writeLegacyParquetFormat: Boolean = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get,
                                        outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
                                        SQLConf.ParquetOutputTimestampType.INT96,
                                        useFieldId: Boolean = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.defaultValue.get)
  extends SparkToParquetSchemaConverter {

  def this(conf: SQLConf) = this(
    writeLegacyParquetFormat = conf.writeLegacyParquetFormat,
    outputTimestampType = conf.parquetOutputTimestampType,
    useFieldId = conf.parquetFieldIdWriteEnabled)

  def this(conf: Configuration) = this(
    writeLegacyParquetFormat = conf.get(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key).toBoolean,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.withName(
      conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)),
    useFieldId = conf.get(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key).toBoolean)

  /**
   * Converts a Spark SQL [[StructType]] to a Parquet [[MessageType]].
   */

  override def convert(catalystSchema: StructType): MessageType = {
    Types
      .buildMessage()
      .addFields(catalystSchema.map(convertField): _*)
      .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  }

  /**
   * Converts a Spark SQL [[StructField]] to a Parquet [[Type]].
   */

  override def convertField(field: StructField): Type = {
    val converted = convertField(field, if (field.nullable) OPTIONAL else REQUIRED)
    if (useFieldId && ParquetUtils.hasFieldId(field)) {
      converted.withId(ParquetUtils.getFieldId(field))
    } else {
      converted
    }
  }

  private def convertField(field: StructField, repetition: Type.Repetition): Type = {
    field.dataType match {
      // ===================
      // Simple atomic types
      // ===================

      case BooleanType =>
        Types.primitive(BOOLEAN, repetition).named(field.name)

      case ByteType =>
        Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.intType(8, true)).named(field.name)

      case ShortType =>
        Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.intType(16, true)).named(field.name)

      case IntegerType | _: YearMonthIntervalType =>
        Types.primitive(INT32, repetition).named(field.name)

      case LongType | _: DayTimeIntervalType =>
        Types.primitive(INT64, repetition).named(field.name)

      case FloatType =>
        Types.primitive(FLOAT, repetition).named(field.name)

      case DoubleType =>
        Types.primitive(DOUBLE, repetition).named(field.name)

      case StringType =>
        Types.primitive(BINARY, repetition)
          .as(LogicalTypeAnnotation.stringType()).named(field.name)

      case DateType =>
        Types.primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.dateType()).named(field.name)

      case TimestampType =>
        outputTimestampType match {
          case SQLConf.ParquetOutputTimestampType.INT96 =>
            Types.primitive(INT96, repetition).named(field.name)
          case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS =>
            Types.primitive(INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS)).named(field.name)
          case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
            Types.primitive(INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS)).named(field.name)
        }

      case TimestampNTZType if Utils.isTesting =>
        Types.primitive(INT64, repetition)
          .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS)).named(field.name)
      case BinaryType =>
        Types.primitive(BINARY, repetition).named(field.name)

      // ======================
      // Decimals (legacy mode)
      // ======================

      case DecimalType.Fixed(precision, scale) if writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .length(Decimal.minBytesForPrecision(precision))
          .named(field.name)

      // ========================
      // Decimals (standard mode)
      // ========================

      // Uses INT32 for 1 <= precision <= 9
      case DecimalType.Fixed(precision, scale)
        if precision <= Decimal.MAX_INT_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT32, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .named(field.name)

      // Uses INT64 for 1 <= precision <= 18
      case DecimalType.Fixed(precision, scale)
        if precision <= Decimal.MAX_LONG_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT64, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .named(field.name)

      // Uses FIXED_LEN_BYTE_ARRAY for all other precisions
      case DecimalType.Fixed(precision, scale) if !writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .length(Decimal.minBytesForPrecision(precision))
          .named(field.name)

      // ===================================
      // ArrayType and MapType (legacy mode)
      // ===================================


      case ArrayType(elementType, nullable @ true) if writeLegacyParquetFormat =>
        Types
          .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
          .addField(Types
            .buildGroup(REPEATED)
            // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
            .addField(convertField(StructField("array", elementType, nullable)))
            .named("bag"))
          .named(field.name)


      case ArrayType(elementType, nullable @ false) if writeLegacyParquetFormat =>
        Types
          .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
          // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
          .addField(convertField(StructField("array", elementType, nullable), REPEATED))
          .named(field.name)

      // Spark 1.4.x and prior versions convert MapType into a 3-level group annotated by
      // MAP_KEY_VALUE.  This is covered by `convertGroupField(field: GroupType): DataType`.
      case MapType(keyType, valueType, valueContainsNull) if writeLegacyParquetFormat =>
        ConversionPatterns.mapType(
          repetition,
          field.name,
          convertField(StructField("key", keyType, nullable = false)),
          convertField(StructField("value", valueType, valueContainsNull)))

      // =====================================
      // ArrayType and MapType (standard mode)
      // =====================================

      case ArrayType(elementType, containsNull) if !writeLegacyParquetFormat =>
        Types
          .buildGroup(repetition).as(LogicalTypeAnnotation.listType())
          .addField(
            Types.repeatedGroup()
              .addField(convertField(StructField("element", elementType, containsNull)))
              .named("list"))
          .named(field.name)

      case MapType(keyType, valueType, valueContainsNull) =>
        Types
          .buildGroup(repetition).as(LogicalTypeAnnotation.mapType())
          .addField(
            Types
              .repeatedGroup()
              .addField(convertField(StructField("key", keyType, nullable = false)))
              .addField(convertField(StructField("value", valueType, valueContainsNull)))
              .named("key_value"))
          .named(field.name)

      // ===========
      // Other types
      // ===========

      case StructType(fields) =>
        fields.foldLeft(Types.buildGroup(repetition)) { (builder, field) =>
          builder.addField(convertField(field))
        }.named(field.name)

      case udt: UserDefinedType[_] =>
        convertField(field.copy(dataType = udt.sqlType))

      case _ =>
        throw QueryCompilationErrors.cannotConvertDataTypeToParquetTypeError(field)
    }
  }
}

private[sql] object GeoParquetSchemaConverter {
  def checkGeomFieldName(name: String): Boolean = {
    if (name.equals(GeometryField.getFieldGeometry())) {
      true
    } else false
  }
}
