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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.OriginalType.LIST
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.{GroupType, OriginalType, Type}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{PhysicalByteType, PhysicalShortType}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, CaseInsensitiveMap, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.io.WKBReader

import java.math.{BigDecimal, BigInteger}
import java.time.{ZoneId, ZoneOffset}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A [[ParquetRowConverter]] is used to convert Parquet records into Catalyst [[InternalRow]]s.
 * Since Catalyst `StructType` is also a Parquet record, this converter can be used as root
 * converter. Take the following Parquet type as an example:
 * {{{
 *   message root {
 *     required int32 f1;
 *     optional group f2 {
 *       required double f21;
 *       optional binary f22 (utf8);
 *     }
 *   }
 * }}}
 * 5 converters will be created:
 *
 *   - a root [[ParquetRowConverter]] for [[org.apache.parquet.schema.MessageType]] `root`, which
 *     contains:
 *     - a [[ParquetPrimitiveConverter]] for required
 *       [[org.apache.parquet.schema.OriginalType.INT_32]] field `f1`, and
 *     - a nested [[ParquetRowConverter]] for optional [[GroupType]] `f2`, which contains:
 *       - a [[ParquetPrimitiveConverter]] for required
 *         [[org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE]] field `f21`, and
 *       - a [[ParquetStringConverter]] for optional
 *         [[org.apache.parquet.schema.OriginalType.UTF8]] string field `f22`
 *
 * When used as a root converter, [[NoopUpdater]] should be used since root converters don't have
 * any "parent" container.
 *
 * @param schemaConverter
 *   A utility converter used to convert Parquet types to Catalyst types.
 * @param parquetType
 *   Parquet schema of Parquet records
 * @param catalystType
 *   Spark SQL schema that corresponds to the Parquet record type. User-defined types other than
 *   [[GeometryUDT]] should have been expanded.
 * @param convertTz
 *   the optional time zone to convert to int96 data
 * @param datetimeRebaseSpec
 *   the specification of rebasing date/timestamp from Julian to Proleptic Gregorian calendar:
 *   mode + optional original time zone
 * @param int96RebaseSpec
 *   the specification of rebasing INT96 timestamp from Julian to Proleptic Gregorian calendar
 * @param parameters
 *   Options for reading GeoParquet files. For example, if legacyMode is enabled or not.
 * @param updater
 *   An updater which propagates converted field values to the parent container
 */
private[parquet] class GeoParquetRowConverter(
    schemaConverter: GeoParquetToSparkSchemaConverter,
    parquetType: GroupType,
    catalystType: StructType,
    convertTz: Option[ZoneId],
    datetimeRebaseSpec: RebaseSpec,
    int96RebaseSpec: RebaseSpec,
    parameters: Map[String, String],
    updater: ParentContainerUpdater)
    extends ParquetGroupConverter(updater)
    with Logging {

  assert(
    parquetType.getFieldCount <= catalystType.length,
    s"""Field count of the Parquet schema is greater than the field count of the Catalyst schema:
       |
       |Parquet schema:
       |$parquetType
       |Catalyst schema:
       |${catalystType.prettyJson}
     """.stripMargin)

  assert(
    !catalystType.existsRecursively(t =>
      !t.isInstanceOf[GeometryUDT] && t.isInstanceOf[UserDefinedType[_]]),
    s"""User-defined types in Catalyst schema should have already been expanded:
       |${catalystType.prettyJson}
     """.stripMargin)

  logDebug(s"""Building row converter for the following schema:
       |
       |Parquet form:
       |$parquetType
       |Catalyst form:
       |${catalystType.prettyJson}
     """.stripMargin)

  /**
   * Updater used together with field converters within a [[ParquetRowConverter]]. It propagates
   * converted filed values to the `ordinal`-th cell in `currentRow`.
   */
  private final class RowUpdater(row: InternalRow, ordinal: Int) extends ParentContainerUpdater {
    override def set(value: Any): Unit = row(ordinal) = value
    override def setBoolean(value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(value: Float): Unit = row.setFloat(ordinal, value)
  }

  private[this] val currentRow = new SpecificInternalRow(catalystType.map(_.dataType))

  /**
   * The [[InternalRow]] converted from an entire Parquet record.
   */
  def currentRecord: InternalRow = currentRow

  private val dateRebaseFunc =
    DataSourceUtils.createDateRebaseFuncInRead(datetimeRebaseSpec.mode, "Parquet")

  private val timestampRebaseFunc =
    DataSourceUtils.createTimestampRebaseFuncInRead(datetimeRebaseSpec, "Parquet")

  private val int96RebaseFunc =
    DataSourceUtils.createTimestampRebaseFuncInRead(int96RebaseSpec, "Parquet INT96")

  // Converters for each field.
  private[this] val fieldConverters: Array[Converter with HasParentContainerUpdater] = {
    // (SPARK-31116) Use case insensitive map if spark.sql.caseSensitive is false
    // to prevent throwing IllegalArgumentException when searching catalyst type's field index
    val catalystFieldNameToIndex = if (SQLConf.get.caseSensitiveAnalysis) {
      catalystType.fieldNames.zipWithIndex.toMap
    } else {
      CaseInsensitiveMap(catalystType.fieldNames.zipWithIndex.toMap)
    }
    parquetType.getFields.asScala.map { parquetField =>
      val fieldIndex = catalystFieldNameToIndex(parquetField.getName)
      val catalystField = catalystType(fieldIndex)
      // Converted field value should be set to the `fieldIndex`-th cell of `currentRow`
      newConverter(parquetField, catalystField.dataType, new RowUpdater(currentRow, fieldIndex))
    }.toArray
  }

  // Updaters for each field.
  private[this] val fieldUpdaters: Array[ParentContainerUpdater] = fieldConverters.map(_.updater)

  override def getConverter(fieldIndex: Int): Converter = fieldConverters(fieldIndex)

  override def end(): Unit = {
    var i = 0
    while (i < fieldUpdaters.length) {
      fieldUpdaters(i).end()
      i += 1
    }
    updater.set(currentRow)
  }

  override def start(): Unit = {
    var i = 0
    val numFields = currentRow.numFields
    while (i < numFields) {
      currentRow.setNullAt(i)
      i += 1
    }
    i = 0
    while (i < fieldUpdaters.length) {
      fieldUpdaters(i).start()
      i += 1
    }
  }

  /**
   * Creates a converter for the given Parquet type `parquetType` and Spark SQL data type
   * `catalystType`. Converted values are handled by `updater`.
   */
  private def newConverter(
      parquetType: Type,
      catalystType: DataType,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {

    catalystType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType | BinaryType =>
        new ParquetPrimitiveConverter(updater)

      case GeometryUDT =>
        if (parquetType.isPrimitive) {
          new ParquetPrimitiveConverter(updater) {
            override def addBinary(value: Binary): Unit = {
              val wkbReader = new WKBReader()
              val geom = wkbReader.read(value.getBytes)
              updater.set(GeometryUDT.serialize(geom))
            }
          }
        } else {
          if (GeoParquetUtils.isLegacyMode(parameters)) {
            new ParquetArrayConverter(
              parquetType.asGroupType(),
              ArrayType(ByteType, containsNull = false),
              updater) {
              override def end(): Unit = {
                val wkbReader = new WKBReader()
                val byteArray = currentArray.map(_.asInstanceOf[Byte]).toArray
                val geom = wkbReader.read(byteArray)
                updater.set(GeometryUDT.serialize(geom))
              }
            }
          } else {
            throw new IllegalArgumentException(
              s"Parquet type for geometry column is $parquetType. This parquet file could be written by " +
                "Apache Sedona <= 1.3.1-incubating. Please use option(\"legacyMode\", \"true\") to read this file.")
          }
        }

      case ByteType =>
        new ParquetPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setByte(value.asInstanceOf[PhysicalByteType#InternalType])

          override def addBinary(value: Binary): Unit = {
            val bytes = value.getBytes
            for (b <- bytes) {
              updater.set(b)
            }
          }
        }

      case ShortType =>
        new ParquetPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setShort(value.asInstanceOf[PhysicalShortType#InternalType])
        }

      // For INT32 backed decimals
      case t: DecimalType if parquetType.asPrimitiveType().getPrimitiveTypeName == INT32 =>
        new ParquetIntDictionaryAwareDecimalConverter(t.precision, t.scale, updater)

      // For INT64 backed decimals
      case t: DecimalType if parquetType.asPrimitiveType().getPrimitiveTypeName == INT64 =>
        new ParquetLongDictionaryAwareDecimalConverter(t.precision, t.scale, updater)

      // For BINARY and FIXED_LEN_BYTE_ARRAY backed decimals
      case t: DecimalType
          if parquetType.asPrimitiveType().getPrimitiveTypeName == FIXED_LEN_BYTE_ARRAY ||
            parquetType.asPrimitiveType().getPrimitiveTypeName == BINARY =>
        new ParquetBinaryDictionaryAwareDecimalConverter(t.precision, t.scale, updater)

      case t: DecimalType =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for decimal type ${t.json} whose Parquet type is " +
            s"$parquetType.  Parquet DECIMAL type can only be backed by INT32, INT64, " +
            "FIXED_LEN_BYTE_ARRAY, or BINARY.")

      case StringType =>
        new ParquetStringConverter(updater)

      case TimestampType if parquetType.getOriginalType == OriginalType.TIMESTAMP_MICROS =>
        new ParquetPrimitiveConverter(updater) {
          override def addLong(value: Long): Unit = {
            updater.setLong(timestampRebaseFunc(value))
          }
        }

      case TimestampType if parquetType.getOriginalType == OriginalType.TIMESTAMP_MILLIS =>
        new ParquetPrimitiveConverter(updater) {
          override def addLong(value: Long): Unit = {
            val micros = DateTimeUtils.millisToMicros(value)
            updater.setLong(timestampRebaseFunc(micros))
          }
        }

      // INT96 timestamp doesn't have a logical type, here we check the physical type instead.
      case TimestampType if parquetType.asPrimitiveType().getPrimitiveTypeName == INT96 =>
        new ParquetPrimitiveConverter(updater) {
          // Converts nanosecond timestamps stored as INT96
          override def addBinary(value: Binary): Unit = {
            val julianMicros = ParquetRowConverter.binaryToSQLTimestamp(value)
            val gregorianMicros = int96RebaseFunc(julianMicros)
            val adjTime = convertTz
              .map(DateTimeUtils.convertTz(gregorianMicros, _, ZoneOffset.UTC))
              .getOrElse(gregorianMicros)
            updater.setLong(adjTime)
          }
        }

      case TimestampNTZType
          if canReadAsTimestampNTZ(parquetType) &&
            parquetType.getLogicalTypeAnnotation
              .asInstanceOf[TimestampLogicalTypeAnnotation]
              .getUnit == TimeUnit.MICROS =>
        new ParquetPrimitiveConverter(updater)

      case TimestampNTZType
          if canReadAsTimestampNTZ(parquetType) &&
            parquetType.getLogicalTypeAnnotation
              .asInstanceOf[TimestampLogicalTypeAnnotation]
              .getUnit == TimeUnit.MILLIS =>
        new ParquetPrimitiveConverter(updater) {
          override def addLong(value: Long): Unit = {
            val micros = DateTimeUtils.millisToMicros(value)
            updater.setLong(micros)
          }
        }

      case DateType =>
        new ParquetPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit = {
            updater.set(dateRebaseFunc(value))
          }
        }

      // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
      // annotated by `LIST` or `MAP` should be interpreted as a required list of required
      // elements where the element type is the type of the field.
      case t: ArrayType if parquetType.getOriginalType != LIST =>
        if (parquetType.isPrimitive) {
          new RepeatedPrimitiveConverter(parquetType, t.elementType, updater)
        } else {
          new RepeatedGroupConverter(parquetType, t.elementType, updater)
        }

      case t: ArrayType =>
        new ParquetArrayConverter(parquetType.asGroupType(), t, updater)

      case t: MapType =>
        new ParquetMapConverter(parquetType.asGroupType(), t, updater)

      case t: StructType =>
        val wrappedUpdater = {
          // SPARK-30338: avoid unnecessary InternalRow copying for nested structs:
          // There are two cases to handle here:
          //
          //  1. Parent container is a map or array: we must make a deep copy of the mutable row
          //     because this converter may be invoked multiple times per Parquet input record
          //     (if the map or array contains multiple elements).
          //
          //  2. Parent container is a struct: we don't need to copy the row here because either:
          //
          //     (a) all ancestors are structs and therefore no copying is required because this
          //         converter will only be invoked once per Parquet input record, or
          //     (b) some ancestor is struct that is nested in a map or array and that ancestor's
          //         converter will perform deep-copying (which will recursively copy this row).
          if (updater.isInstanceOf[RowUpdater]) {
            // `updater` is a RowUpdater, implying that the parent container is a struct.
            updater
          } else {
            // `updater` is NOT a RowUpdater, implying that the parent container a map or array.
            new ParentContainerUpdater {
              override def set(value: Any): Unit = {
                updater.set(value.asInstanceOf[SpecificInternalRow].copy()) // deep copy
              }
            }
          }
        }
        new GeoParquetRowConverter(
          schemaConverter,
          parquetType.asGroupType(),
          t,
          convertTz,
          datetimeRebaseSpec,
          int96RebaseSpec,
          parameters,
          wrappedUpdater)

      case t =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type ${t.json} " +
            s"whose Parquet type is $parquetType")
    }
  }

  // Only INT64 column with Timestamp logical annotation `isAdjustedToUTC=false`
  // can be read as Spark's TimestampNTZ type. This is to avoid mistakes in reading the timestamp
  // values.
  private def canReadAsTimestampNTZ(parquetType: Type): Boolean =
    schemaConverter.isTimestampNTZEnabled() &&
      parquetType.asPrimitiveType().getPrimitiveTypeName == INT64 &&
      parquetType.getLogicalTypeAnnotation.isInstanceOf[TimestampLogicalTypeAnnotation] &&
      !parquetType.getLogicalTypeAnnotation
        .asInstanceOf[TimestampLogicalTypeAnnotation]
        .isAdjustedToUTC

  /**
   * Parquet converter for strings. A dictionary is used to minimize string decoding cost.
   */
  private final class ParquetStringConverter(updater: ParentContainerUpdater)
      extends ParquetPrimitiveConverter(updater) {

    private var expandedDictionary: Array[UTF8String] = null

    override def hasDictionarySupport: Boolean = true

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { i =>
        UTF8String.fromBytes(dictionary.decodeToBinary(i).getBytes)
      }
    }

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    override def addBinary(value: Binary): Unit = {
      // The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here we
      // are using `Binary.toByteBuffer.array()` to steal the underlying byte array without copying
      // it.
      val buffer = value.toByteBuffer
      val offset = buffer.arrayOffset() + buffer.position()
      val numBytes = buffer.remaining()
      updater.set(UTF8String.fromBytes(buffer.array(), offset, numBytes))
    }
  }

  /**
   * Parquet converter for fixed-precision decimals.
   */
  private abstract class ParquetDecimalConverter(
      precision: Int,
      scale: Int,
      updater: ParentContainerUpdater)
      extends ParquetPrimitiveConverter(updater) {

    protected var expandedDictionary: Array[Decimal] = _

    override def hasDictionarySupport: Boolean = true

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    // Converts decimals stored as INT32
    override def addInt(value: Int): Unit = {
      addLong(value: Long)
    }

    // Converts decimals stored as INT64
    override def addLong(value: Long): Unit = {
      updater.set(decimalFromLong(value))
    }

    // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
    override def addBinary(value: Binary): Unit = {
      updater.set(decimalFromBinary(value))
    }

    protected def decimalFromLong(value: Long): Decimal = {
      Decimal(value, precision, scale)
    }

    protected def decimalFromBinary(value: Binary): Decimal = {
      if (precision <= Decimal.MAX_LONG_DIGITS) {
        // Constructs a `Decimal` with an unscaled `Long` value if possible.
        val unscaled = ParquetRowConverter.binaryToUnscaledLong(value)
        Decimal(unscaled, precision, scale)
      } else {
        // Otherwise, resorts to an unscaled `BigInteger` instead.
        Decimal(new BigDecimal(new BigInteger(value.getBytes), scale), precision, scale)
      }
    }
  }

  private class ParquetIntDictionaryAwareDecimalConverter(
      precision: Int,
      scale: Int,
      updater: ParentContainerUpdater)
      extends ParquetDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromLong(dictionary.decodeToInt(id).toLong)
      }
    }
  }

  private class ParquetLongDictionaryAwareDecimalConverter(
      precision: Int,
      scale: Int,
      updater: ParentContainerUpdater)
      extends ParquetDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromLong(dictionary.decodeToLong(id))
      }
    }
  }

  private class ParquetBinaryDictionaryAwareDecimalConverter(
      precision: Int,
      scale: Int,
      updater: ParentContainerUpdater)
      extends ParquetDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromBinary(dictionary.decodeToBinary(id))
      }
    }
  }

  /**
   * Parquet converter for arrays. Spark SQL arrays are represented as Parquet lists. Standard
   * Parquet lists are represented as a 3-level group annotated by `LIST`:
   * {{{
   *   <list-repetition> group <name> (LIST) {            <-- parquetSchema points here
   *     repeated group list {
   *       <element-repetition> <element-type> element;
   *     }
   *   }
   * }}}
   * The `parquetSchema` constructor argument points to the outermost group.
   *
   * However, before this representation is standardized, some Parquet libraries/tools also use
   * some non-standard formats to represent list-like structures. Backwards-compatibility rules
   * for handling these cases are described in Parquet format spec.
   *
   * @see
   *   https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
   */
  private class ParquetArrayConverter(
      parquetSchema: GroupType,
      catalystSchema: ArrayType,
      updater: ParentContainerUpdater)
      extends ParquetGroupConverter(updater) {

    protected[this] val currentArray: mutable.ArrayBuffer[Any] = ArrayBuffer.empty[Any]

    private[this] val elementConverter: Converter = {
      val repeatedType = parquetSchema.getType(0)
      val elementType = catalystSchema.elementType

      // At this stage, we're not sure whether the repeated field maps to the element type or is
      // just the syntactic repeated group of the 3-level standard LIST layout. Take the following
      // Parquet LIST-annotated group type as an example:
      //
      //    optional group f (LIST) {
      //      repeated group list {
      //        optional group element {
      //          optional int32 element;
      //        }
      //      }
      //    }
      //
      // This type is ambiguous:
      //
      // 1. When interpreted as a standard 3-level layout, the `list` field is just the syntactic
      //    group, and the entire type should be translated to:
      //
      //      ARRAY<STRUCT<element: INT>>
      //
      // 2. On the other hand, when interpreted as a non-standard 2-level layout, the `list` field
      //    represents the element type, and the entire type should be translated to:
      //
      //      ARRAY<STRUCT<element: STRUCT<element: INT>>>
      //
      // Here we try to convert field `list` into a Catalyst type to see whether the converted type
      // matches the Catalyst array element type. If it doesn't match, then it's case 1; otherwise,
      // it's case 2.
      val guessedElementType = schemaConverter.convertFieldWithGeo(repeatedType)

      if (DataType.equalsIgnoreCompatibleNullability(guessedElementType, elementType)) {
        // If the repeated field corresponds to the element type, creates a new converter using the
        // type of the repeated field.
        newConverter(
          repeatedType,
          elementType,
          new ParentContainerUpdater {
            override def set(value: Any): Unit = currentArray += value
          })
      } else {
        // If the repeated field corresponds to the syntactic group in the standard 3-level Parquet
        // LIST layout, creates a new converter using the only child field of the repeated field.
        assert(!repeatedType.isPrimitive && repeatedType.asGroupType().getFieldCount == 1)
        new ElementConverter(repeatedType.asGroupType().getType(0), elementType)
      }
    }

    override def getConverter(fieldIndex: Int): Converter = elementConverter

    override def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))

    override def start(): Unit = currentArray.clear()

    /** Array element converter */
    private final class ElementConverter(parquetType: Type, catalystType: DataType)
        extends GroupConverter {

      private var currentElement: Any = _

      private[this] val converter =
        newConverter(
          parquetType,
          catalystType,
          new ParentContainerUpdater {
            override def set(value: Any): Unit = currentElement = value
          })

      override def getConverter(fieldIndex: Int): Converter = converter

      override def end(): Unit = currentArray += currentElement

      override def start(): Unit = currentElement = null
    }
  }

  /** Parquet converter for maps */
  private final class ParquetMapConverter(
      parquetType: GroupType,
      catalystType: MapType,
      updater: ParentContainerUpdater)
      extends ParquetGroupConverter(updater) {

    private[this] val currentKeys = ArrayBuffer.empty[Any]
    private[this] val currentValues = ArrayBuffer.empty[Any]

    private[this] val keyValueConverter = {
      val repeatedType = parquetType.getType(0).asGroupType()
      new KeyValueConverter(
        repeatedType.getType(0),
        repeatedType.getType(1),
        catalystType.keyType,
        catalystType.valueType)
    }

    override def getConverter(fieldIndex: Int): Converter = keyValueConverter

    override def end(): Unit = {
      // The parquet map may contains null or duplicated map keys. When it happens, the behavior is
      // undefined.
      // TODO (SPARK-26174): disallow it with a config.
      updater.set(
        new ArrayBasedMapData(
          new GenericArrayData(currentKeys.toArray),
          new GenericArrayData(currentValues.toArray)))
    }

    override def start(): Unit = {
      currentKeys.clear()
      currentValues.clear()
    }

    /** Parquet converter for key-value pairs within the map. */
    private final class KeyValueConverter(
        parquetKeyType: Type,
        parquetValueType: Type,
        catalystKeyType: DataType,
        catalystValueType: DataType)
        extends GroupConverter {

      private var currentKey: Any = _

      private var currentValue: Any = _

      private[this] val converters = Array(
        // Converter for keys
        newConverter(
          parquetKeyType,
          catalystKeyType,
          new ParentContainerUpdater {
            override def set(value: Any): Unit = currentKey = value
          }),

        // Converter for values
        newConverter(
          parquetValueType,
          catalystValueType,
          new ParentContainerUpdater {
            override def set(value: Any): Unit = currentValue = value
          }))

      override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

      override def end(): Unit = {
        currentKeys += currentKey
        currentValues += currentValue
      }

      override def start(): Unit = {
        currentKey = null
        currentValue = null
      }
    }
  }

  private trait RepeatedConverter {
    private[this] val currentArray = ArrayBuffer.empty[Any]

    protected def newArrayUpdater(updater: ParentContainerUpdater) = new ParentContainerUpdater {
      override def start(): Unit = currentArray.clear()
      override def end(): Unit = updater.set(new GenericArrayData(currentArray.toArray))
      override def set(value: Any): Unit = currentArray += value
    }
  }

  /**
   * A primitive converter for converting unannotated repeated primitive values to required arrays
   * of required primitives values.
   */
  private final class RepeatedPrimitiveConverter(
      parquetType: Type,
      catalystType: DataType,
      parentUpdater: ParentContainerUpdater)
      extends PrimitiveConverter
      with RepeatedConverter
      with HasParentContainerUpdater {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private[this] val elementConverter: PrimitiveConverter =
      newConverter(parquetType, catalystType, updater).asPrimitiveConverter()

    override def addBoolean(value: Boolean): Unit = elementConverter.addBoolean(value)
    override def addInt(value: Int): Unit = elementConverter.addInt(value)
    override def addLong(value: Long): Unit = elementConverter.addLong(value)
    override def addFloat(value: Float): Unit = elementConverter.addFloat(value)
    override def addDouble(value: Double): Unit = elementConverter.addDouble(value)
    override def addBinary(value: Binary): Unit = elementConverter.addBinary(value)

    override def setDictionary(dict: Dictionary): Unit = elementConverter.setDictionary(dict)
    override def hasDictionarySupport: Boolean = elementConverter.hasDictionarySupport
    override def addValueFromDictionary(id: Int): Unit =
      elementConverter.addValueFromDictionary(id)
  }

  /**
   * A group converter for converting unannotated repeated group values to required arrays of
   * required struct values.
   */
  private final class RepeatedGroupConverter(
      parquetType: Type,
      catalystType: DataType,
      parentUpdater: ParentContainerUpdater)
      extends GroupConverter
      with HasParentContainerUpdater
      with RepeatedConverter {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private[this] val elementConverter: GroupConverter =
      newConverter(parquetType, catalystType, updater).asGroupConverter()

    override def getConverter(field: Int): Converter = elementConverter.getConverter(field)
    override def end(): Unit = elementConverter.end()
    override def start(): Unit = elementConverter.start()
  }
}
