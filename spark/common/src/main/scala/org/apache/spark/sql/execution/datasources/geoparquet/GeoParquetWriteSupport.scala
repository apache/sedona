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
package org.apache.spark.sql.execution.datasources.geoparquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.sedona.common.utils.GeomUtils
import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetMetaData.{GEOPARQUET_COVERING_KEY, GEOPARQUET_COVERING_MODE_AUTO, GEOPARQUET_COVERING_MODE_KEY, GEOPARQUET_COVERING_MODE_LEGACY, GEOPARQUET_CRS_KEY, GEOPARQUET_VERSION_KEY, VERSION, createCoveringColumnMetadata}
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetWriteSupport.GeometryColumnInfo
import org.apache.spark.sql.execution.datasources.geoparquet.internal.{DataSourceUtils, LegacyBehaviorPolicy, PortableSQLConf}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse
import org.locationtech.jts.geom.Geometry

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A Parquet [[WriteSupport]] implementation that writes Catalyst [[InternalRow]]s as Parquet
 * messages. This class can write Parquet data in two modes:
 *
 *   - Standard mode: Parquet data are written in standard format defined in parquet-format spec.
 *   - Legacy mode: Parquet data are written in legacy format compatible with Spark 1.4 and prior.
 *
 * This behavior can be controlled by SQL option `spark.sql.parquet.writeLegacyFormat`. The value
 * of this option is propagated to this class by the `init()` method and its Hadoop configuration
 * argument.
 */
class GeoParquetWriteSupport extends WriteSupport[InternalRow] with Logging {
  // A `ValueWriter` is responsible for writing a field of an `InternalRow` to the record consumer.
  // Here we are using `SpecializedGetters` rather than `InternalRow` so that we can directly access
  // data in `ArrayData` without the help of `SpecificMutableRow`.
  private type ValueWriter = (SpecializedGetters, Int) => Unit

  // Schema of the `InternalRow`s to be written
  private var schema: StructType = _

  // `ValueWriter`s for all fields of the schema
  private var rootFieldWriters: Array[ValueWriter] = _

  // The Parquet `RecordConsumer` to which all `InternalRow`s are written
  private var recordConsumer: RecordConsumer = _

  // Whether to write data in legacy Parquet format compatible with Spark 1.4 and prior versions
  private var writeLegacyParquetFormat: Boolean = _

  // Which parquet timestamp type to use when writing.
  private var outputTimestampType: PortableSQLConf.ParquetOutputTimestampType.Value = _

  // Reusable byte array used to write timestamps as Parquet INT96 values
  private val timestampBuffer = new Array[Byte](12)

  // Reusable byte array used to write decimal values
  private val decimalBuffer =
    new Array[Byte](Decimal.minBytesForPrecision(DecimalType.MAX_PRECISION))

  private val datetimeRebaseMode =
    LegacyBehaviorPolicy.withName(
      PortableSQLConf.get.getConf(PortableSQLConf.PARQUET_REBASE_MODE_IN_WRITE))

  private val dateRebaseFunc =
    DataSourceUtils.createDateRebaseFuncInWrite(datetimeRebaseMode, "Parquet")

  private val timestampRebaseFunc =
    DataSourceUtils.createTimestampRebaseFuncInWrite(datetimeRebaseMode, "Parquet")

  private val int96RebaseMode =
    LegacyBehaviorPolicy.withName(
      PortableSQLConf.get.getConf(PortableSQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE))

  private val int96RebaseFunc =
    DataSourceUtils.createTimestampRebaseFuncInWrite(int96RebaseMode, "Parquet INT96")

  // A mapping from geometry field ordinal to bounding box. According to the geoparquet specification,
  // "Geometry columns MUST be at the root of the schema", so we don't need to worry about geometry
  // fields in nested structures.
  private val geometryColumnInfoMap: mutable.Map[Int, GeometryColumnInfo] = mutable.Map.empty

  private var geoParquetVersion: Option[String] = None
  private var defaultGeoParquetCrs: Option[JValue] = None
  private val geoParquetColumnCrsMap: mutable.Map[String, Option[JValue]] = mutable.Map.empty
  private val geoParquetColumnCoveringMap: mutable.Map[String, Covering] = mutable.Map.empty
  private val generatedCoveringColumnOrdinals: mutable.Map[Int, Int] = mutable.Map.empty
  private var geoParquetCoveringMode: String = GEOPARQUET_COVERING_MODE_AUTO

  override def init(configuration: Configuration): WriteContext = {
    val schemaString = configuration.get(internal.ParquetWriteSupport.SPARK_ROW_SCHEMA)
    this.schema = StructType.fromString(schemaString)
    this.writeLegacyParquetFormat = {
      // `LenientSQLConf.PARQUET_WRITE_LEGACY_FORMAT` should always be explicitly set in ParquetRelation
      assert(configuration.get(PortableSQLConf.PARQUET_WRITE_LEGACY_FORMAT.key) != null)
      configuration.get(PortableSQLConf.PARQUET_WRITE_LEGACY_FORMAT.key).toBoolean
    }

    this.outputTimestampType = {
      val key = PortableSQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key
      assert(configuration.get(key) != null)
      PortableSQLConf.ParquetOutputTimestampType.withName(configuration.get(key))
    }

    schema.zipWithIndex.foreach { case (field, ordinal) =>
      if (field.dataType == GeometryUDT) {
        geometryColumnInfoMap.getOrElseUpdate(ordinal, new GeometryColumnInfo())
      }
    }

    if (geometryColumnInfoMap.isEmpty) {
      throw new RuntimeException("No geometry column found in the schema")
    }

    geoParquetVersion = configuration.get(GEOPARQUET_VERSION_KEY) match {
      case null => Some(VERSION)
      case version: String => Some(version)
    }
    geoParquetCoveringMode = Option(configuration.get(GEOPARQUET_COVERING_MODE_KEY))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(GEOPARQUET_COVERING_MODE_AUTO)
      .toLowerCase(java.util.Locale.ROOT)
    if (geoParquetCoveringMode != GEOPARQUET_COVERING_MODE_AUTO &&
      geoParquetCoveringMode != GEOPARQUET_COVERING_MODE_LEGACY) {
      throw new IllegalArgumentException(
        s"Invalid value '$geoParquetCoveringMode' for $GEOPARQUET_COVERING_MODE_KEY. " +
          s"Supported values are '$GEOPARQUET_COVERING_MODE_AUTO' and " +
          s"'$GEOPARQUET_COVERING_MODE_LEGACY'.")
    }
    defaultGeoParquetCrs = configuration.get(GEOPARQUET_CRS_KEY) match {
      case null =>
        // If no CRS is specified, we write null to the crs metadata field. This is for compatibility with
        // geopandas 0.10.0 and earlier versions, which requires crs field to be present.
        Some(org.json4s.JNull)
      case "" => None
      case crs: String => Some(parse(crs))
    }
    geometryColumnInfoMap.keys.map(schema(_).name).foreach { name =>
      Option(configuration.get(GEOPARQUET_CRS_KEY + "." + name)).foreach {
        case "" => geoParquetColumnCrsMap.put(name, None)
        case crs: String => geoParquetColumnCrsMap.put(name, Some(parse(crs)))
      }
    }
    Option(configuration.get(GEOPARQUET_COVERING_KEY)).foreach { coveringColumnName =>
      if (geometryColumnInfoMap.size > 1) {
        throw new IllegalArgumentException(
          s"$GEOPARQUET_COVERING_KEY is ambiguous when there are multiple geometry columns." +
            s"Please specify $GEOPARQUET_COVERING_KEY.<columnName> for configured geometry column.")
      }
      val geometryColumnName = schema(geometryColumnInfoMap.keys.head).name
      val covering = createCoveringColumnMetadata(coveringColumnName, schema)
      geoParquetColumnCoveringMap.put(geometryColumnName, covering)
    }
    geometryColumnInfoMap.keys.map(schema(_).name).foreach { name =>
      val perColumnKey = GEOPARQUET_COVERING_KEY + "." + name
      // Skip keys that collide with reserved option keys (e.g. geoparquet.covering.mode)
      if (perColumnKey != GEOPARQUET_COVERING_MODE_KEY) {
        Option(configuration.get(perColumnKey)).foreach { coveringColumnName =>
          val covering = createCoveringColumnMetadata(coveringColumnName, schema)
          geoParquetColumnCoveringMap.put(name, covering)
        }
      }
    }

    maybeAutoGenerateCoveringColumns()

    this.rootFieldWriters = schema.zipWithIndex
      .map { case (field, ordinal) =>
        generatedCoveringColumnOrdinals.get(ordinal) match {
          case Some(geometryOrdinal) => makeGeneratedCoveringWriter(geometryOrdinal)
          case None => makeWriter(field.dataType, Some(ordinal))
        }
      }
      .toArray[ValueWriter]

    val messageType = new internal.SparkToParquetSchemaConverter(configuration).convert(schema)
    val sparkSqlParquetRowMetadata = GeoParquetWriteSupport.getSparkSqlParquetRowMetadata(schema)
    val metadata = Map(
      DataSourceUtils.SPARK_VERSION_METADATA_KEY -> SPARK_VERSION_SHORT,
      internal.ParquetReadSupport.SPARK_METADATA_KEY -> sparkSqlParquetRowMetadata) ++ {
      if (datetimeRebaseMode == LegacyBehaviorPolicy.LEGACY) {
        Some("org.apache.spark.legacyDateTime" -> "")
      } else {
        None
      }
    } ++ {
      if (int96RebaseMode == LegacyBehaviorPolicy.LEGACY) {
        Some("org.apache.spark.legacyINT96" -> "")
      } else {
        None
      }
    }

    logInfo(s"""Initialized Parquet WriteSupport with Catalyst schema:
         |${schema.prettyJson}
         |and corresponding Parquet message type:
         |$messageType
       """.stripMargin)

    new WriteContext(messageType, metadata.asJava)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    this.recordConsumer = recordConsumer
  }

  override def finalizeWrite(): WriteSupport.FinalizedWriteContext = {
    val metadata = new util.HashMap[String, String]()
    if (geometryColumnInfoMap.nonEmpty) {
      val primaryColumnIndex = geometryColumnInfoMap.keys.head
      val primaryColumn = schema.fields(primaryColumnIndex).name
      val columns = geometryColumnInfoMap.map { case (ordinal, columnInfo) =>
        val columnName = schema.fields(ordinal).name
        val geometryTypes = columnInfo.seenGeometryTypes.toSeq
        val bbox = if (geometryTypes.nonEmpty) {
          Seq(
            columnInfo.bbox.minX,
            columnInfo.bbox.minY,
            columnInfo.bbox.maxX,
            columnInfo.bbox.maxY)
        } else Seq(0.0, 0.0, 0.0, 0.0)
        val crs = geoParquetColumnCrsMap.getOrElse(columnName, defaultGeoParquetCrs)
        val covering = geoParquetColumnCoveringMap.get(columnName)
        columnName -> GeometryFieldMetaData("WKB", geometryTypes, bbox, crs, covering)
      }.toMap
      val geoParquetMetadata = GeoParquetMetaData(geoParquetVersion, primaryColumn, columns)
      val geoParquetMetadataJson = GeoParquetMetaData.toJson(geoParquetMetadata)
      metadata.put("geo", geoParquetMetadataJson)
    }
    new FinalizedWriteContext(metadata)
  }

  override def write(row: InternalRow): Unit = {
    consumeMessage {
      writeFields(row, schema, rootFieldWriters)
    }
  }

  private def writeFields(
      row: InternalRow,
      schema: StructType,
      fieldWriters: Array[ValueWriter]): Unit = {
    var i = 0
    while (i < schema.length) {
      generatedCoveringColumnOrdinals.get(i) match {
        case Some(geometryOrdinal) =>
          if (!row.isNullAt(geometryOrdinal)) {
            consumeField(schema(i).name, i) {
              fieldWriters(i).apply(row, i)
            }
          }
        case None =>
          if (i < row.numFields && !row.isNullAt(i)) {
            consumeField(schema(i).name, i) {
              fieldWriters(i).apply(row, i)
            }
          }
      }
      i += 1
    }
  }

  private def maybeAutoGenerateCoveringColumns(): Unit = {
    if (!isAutoCoveringEnabled) {
      return
    }

    // If the user provided any explicit covering options, don't auto-generate for
    // the remaining geometry columns. Explicit options signal intentional configuration.
    if (geoParquetColumnCoveringMap.nonEmpty) {
      return
    }

    val generatedCoveringFields = mutable.ArrayBuffer.empty[StructField]
    val geometryColumns =
      geometryColumnInfoMap.keys.toSeq.sorted.map(ordinal => ordinal -> schema(ordinal).name)

    geometryColumns.foreach { case (geometryOrdinal, geometryColumnName) =>
      if (!geoParquetColumnCoveringMap.contains(geometryColumnName)) {
        val coveringColumnName = s"${geometryColumnName}_bbox"
        if (schema.fieldNames.contains(coveringColumnName)) {
          // Reuse an existing column if it is a valid covering struct; otherwise skip.
          try {
            val covering = createCoveringColumnMetadata(coveringColumnName, schema)
            geoParquetColumnCoveringMap.put(geometryColumnName, covering)
          } catch {
            case _: IllegalArgumentException =>
              logWarning(
                s"Existing column '$coveringColumnName' is not a valid covering struct " +
                  s"(expected struct<xmin, ymin, xmax, ymax> with float/double fields; " +
                  s"optional zmin/zmax fields are also supported). " +
                  s"Skipping automatic covering for geometry column '$geometryColumnName'.")
          }
        } else {
          val coveringStructType = StructType(
            Seq(
              StructField("xmin", DoubleType, nullable = false),
              StructField("ymin", DoubleType, nullable = false),
              StructField("xmax", DoubleType, nullable = false),
              StructField("ymax", DoubleType, nullable = false)))
          generatedCoveringFields +=
            StructField(coveringColumnName, coveringStructType, nullable = true)
          val generatedOrdinal = schema.length + generatedCoveringFields.length - 1
          generatedCoveringColumnOrdinals.put(generatedOrdinal, geometryOrdinal)
        }
      }
    }

    if (generatedCoveringFields.nonEmpty) {
      schema = StructType(schema.fields ++ generatedCoveringFields)
      generatedCoveringFields.foreach { generatedField =>
        val covering = createCoveringColumnMetadata(generatedField.name, schema)
        val geometryColumnName = generatedField.name.stripSuffix("_bbox")
        geoParquetColumnCoveringMap.put(geometryColumnName, covering)
      }
    }
  }

  private def isGeoParquet11: Boolean = {
    geoParquetVersion.contains(VERSION)
  }

  private def isAutoCoveringEnabled: Boolean = {
    geoParquetCoveringMode == GEOPARQUET_COVERING_MODE_AUTO && isGeoParquet11
  }

  private def makeGeneratedCoveringWriter(geometryOrdinal: Int): ValueWriter = {
    (row: SpecializedGetters, _: Int) =>
      val geom = GeometryUDT.deserialize(row.getBinary(geometryOrdinal))
      val envelope = geom.getEnvelopeInternal
      consumeGroup {
        consumeField("xmin", 0) {
          recordConsumer.addDouble(envelope.getMinX)
        }
        consumeField("ymin", 1) {
          recordConsumer.addDouble(envelope.getMinY)
        }
        consumeField("xmax", 2) {
          recordConsumer.addDouble(envelope.getMaxX)
        }
        consumeField("ymax", 3) {
          recordConsumer.addDouble(envelope.getMaxY)
        }
      }
  }

  private def makeWriter(dataType: DataType, rootOrdinal: Option[Int] = None): ValueWriter = {
    dataType match {
      case BooleanType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addBoolean(row.getBoolean(ordinal))

      case ByteType =>
        (row: SpecializedGetters, ordinal: Int) => recordConsumer.addInteger(row.getByte(ordinal))

      case ShortType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addInteger(row.getShort(ordinal))

      case DateType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addInteger(dateRebaseFunc(row.getInt(ordinal)))

      case IntegerType =>
        (row: SpecializedGetters, ordinal: Int) => recordConsumer.addInteger(row.getInt(ordinal))

      case LongType =>
        (row: SpecializedGetters, ordinal: Int) => recordConsumer.addLong(row.getLong(ordinal))

      case FloatType =>
        (row: SpecializedGetters, ordinal: Int) => recordConsumer.addFloat(row.getFloat(ordinal))

      case DoubleType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addDouble(row.getDouble(ordinal))

      case StringType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addBinary(
            Binary.fromReusedByteArray(row.getUTF8String(ordinal).getBytes))

      case TimestampType =>
        outputTimestampType match {
          case PortableSQLConf.ParquetOutputTimestampType.INT96 =>
            (row: SpecializedGetters, ordinal: Int) =>
              val micros = int96RebaseFunc(row.getLong(ordinal))
              val (julianDay, timeOfDayNanos) = DateTimeUtils.toJulianDay(micros)
              val buf = ByteBuffer.wrap(timestampBuffer)
              buf.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay)
              recordConsumer.addBinary(Binary.fromReusedByteArray(timestampBuffer))

          case PortableSQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS =>
            (row: SpecializedGetters, ordinal: Int) =>
              val micros = row.getLong(ordinal)
              recordConsumer.addLong(timestampRebaseFunc(micros))

          case PortableSQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
            (row: SpecializedGetters, ordinal: Int) =>
              val micros = row.getLong(ordinal)
              val millis = DateTimeUtils.microsToMillis(timestampRebaseFunc(micros))
              recordConsumer.addLong(millis)
        }

      case TimestampNTZType =>
        // For TimestampNTZType column, Spark always output as INT64 with Timestamp annotation in
        // MICROS time unit.
        (row: SpecializedGetters, ordinal: Int) => recordConsumer.addLong(row.getLong(ordinal))

      case BinaryType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addBinary(Binary.fromReusedByteArray(row.getBinary(ordinal)))

      case DecimalType.Fixed(precision, scale) =>
        makeDecimalWriter(precision, scale)

      case t: StructType =>
        val fieldWriters = t.map(_.dataType).map(makeWriter(_, None)).toArray[ValueWriter]
        (row: SpecializedGetters, ordinal: Int) =>
          consumeGroup {
            writeFields(row.getStruct(ordinal, t.length), t, fieldWriters)
          }

      case t: ArrayType => makeArrayWriter(t)

      case t: MapType => makeMapWriter(t)

      case GeometryUDT =>
        val geometryColumnInfo = rootOrdinal match {
          case Some(ordinal) =>
            geometryColumnInfoMap.getOrElseUpdate(ordinal, new GeometryColumnInfo())
          case None => null
        }
        (row: SpecializedGetters, ordinal: Int) => {
          val serializedGeometry = row.getBinary(ordinal)
          val geom = GeometryUDT.deserialize(serializedGeometry)
          val wkbWriter = GeomUtils.createWKBWriter(GeomUtils.getDimension(geom))
          recordConsumer.addBinary(Binary.fromReusedByteArray(wkbWriter.write(geom)))
          if (geometryColumnInfo != null) {
            geometryColumnInfo.update(geom)
          }
        }

      case t: UserDefinedType[_] => makeWriter(t.sqlType)

      // TODO Adds IntervalType support
      case _ => sys.error(s"Unsupported data type $dataType.")
    }
  }

  private def makeDecimalWriter(precision: Int, scale: Int): ValueWriter = {
    assert(
      precision <= DecimalType.MAX_PRECISION,
      s"Decimal precision $precision exceeds max precision ${DecimalType.MAX_PRECISION}")

    val numBytes = Decimal.minBytesForPrecision(precision)

    val int32Writer =
      (row: SpecializedGetters, ordinal: Int) => {
        val unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong
        recordConsumer.addInteger(unscaledLong.toInt)
      }

    val int64Writer =
      (row: SpecializedGetters, ordinal: Int) => {
        val unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong
        recordConsumer.addLong(unscaledLong)
      }

    val binaryWriterUsingUnscaledLong =
      (row: SpecializedGetters, ordinal: Int) => {
        // When the precision is low enough (<= 18) to squeeze the decimal value into a `Long`, we
        // can build a fixed-length byte array with length `numBytes` using the unscaled `Long`
        // value and the `decimalBuffer` for better performance.
        val unscaled = row.getDecimal(ordinal, precision, scale).toUnscaledLong
        var i = 0
        var shift = 8 * (numBytes - 1)

        while (i < numBytes) {
          decimalBuffer(i) = (unscaled >> shift).toByte
          i += 1
          shift -= 8
        }

        recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBuffer, 0, numBytes))
      }

    val binaryWriterUsingUnscaledBytes =
      (row: SpecializedGetters, ordinal: Int) => {
        val decimal = row.getDecimal(ordinal, precision, scale)
        val bytes = decimal.toJavaBigDecimal.unscaledValue().toByteArray
        val fixedLengthBytes = if (bytes.length == numBytes) {
          // If the length of the underlying byte array of the unscaled `BigInteger` happens to be
          // `numBytes`, just reuse it, so that we don't bother copying it to `decimalBuffer`.
          bytes
        } else {
          // Otherwise, the length must be less than `numBytes`.  In this case we copy contents of
          // the underlying bytes with padding sign bytes to `decimalBuffer` to form the result
          // fixed-length byte array.
          val signByte = if (bytes.head < 0) -1: Byte else 0: Byte
          util.Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte)
          System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length)
          decimalBuffer
        }

        recordConsumer.addBinary(Binary.fromReusedByteArray(fixedLengthBytes, 0, numBytes))
      }

    writeLegacyParquetFormat match {
      // Standard mode, 1 <= precision <= 9, writes as INT32
      case false if precision <= Decimal.MAX_INT_DIGITS => int32Writer

      // Standard mode, 10 <= precision <= 18, writes as INT64
      case false if precision <= Decimal.MAX_LONG_DIGITS => int64Writer

      // Legacy mode, 1 <= precision <= 18, writes as FIXED_LEN_BYTE_ARRAY
      case true if precision <= Decimal.MAX_LONG_DIGITS => binaryWriterUsingUnscaledLong

      // Either standard or legacy mode, 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
      case _ => binaryWriterUsingUnscaledBytes
    }
  }

  def makeArrayWriter(arrayType: ArrayType): ValueWriter = {
    val elementWriter = makeWriter(arrayType.elementType)

    def threeLevelArrayWriter(repeatedGroupName: String, elementFieldName: String): ValueWriter =
      (row: SpecializedGetters, ordinal: Int) => {
        val array = row.getArray(ordinal)
        consumeGroup {
          // Only creates the repeated field if the array is non-empty.
          if (array.numElements() > 0) {
            consumeField(repeatedGroupName, 0) {
              var i = 0
              while (i < array.numElements()) {
                consumeGroup {
                  // Only creates the element field if the current array element is not null.
                  if (!array.isNullAt(i)) {
                    consumeField(elementFieldName, 0) {
                      elementWriter.apply(array, i)
                    }
                  }
                }
                i += 1
              }
            }
          }
        }
      }

    def twoLevelArrayWriter(repeatedFieldName: String): ValueWriter =
      (row: SpecializedGetters, ordinal: Int) => {
        val array = row.getArray(ordinal)
        consumeGroup {
          // Only creates the repeated field if the array is non-empty.
          if (array.numElements() > 0) {
            consumeField(repeatedFieldName, 0) {
              var i = 0
              while (i < array.numElements()) {
                elementWriter.apply(array, i)
                i += 1
              }
            }
          }
        }
      }

    (writeLegacyParquetFormat, arrayType.containsNull) match {
      case (legacyMode @ false, _) =>
        // Standard mode:
        //
        //   <list-repetition> group <name> (LIST) {
        //     repeated group list {
        //                    ^~~~  repeatedGroupName
        //       <element-repetition> <element-type> element;
        //                                           ^~~~~~~  elementFieldName
        //     }
        //   }
        threeLevelArrayWriter(repeatedGroupName = "list", elementFieldName = "element")

      case (legacyMode @ true, nullableElements @ true) =>
        // Legacy mode, with nullable elements:
        //
        //   <list-repetition> group <name> (LIST) {
        //     optional group bag {
        //                    ^~~  repeatedGroupName
        //       repeated <element-type> array;
        //                               ^~~~~ elementFieldName
        //     }
        //   }
        threeLevelArrayWriter(repeatedGroupName = "bag", elementFieldName = "array")

      case (legacyMode @ true, nullableElements @ false) =>
        // Legacy mode, with non-nullable elements:
        //
        //   <list-repetition> group <name> (LIST) {
        //     repeated <element-type> array;
        //                             ^~~~~  repeatedFieldName
        //   }
        twoLevelArrayWriter(repeatedFieldName = "array")
    }
  }

  private def makeMapWriter(mapType: MapType): ValueWriter = {
    val keyWriter = makeWriter(mapType.keyType)
    val valueWriter = makeWriter(mapType.valueType)
    val repeatedGroupName = if (writeLegacyParquetFormat) {
      // Legacy mode:
      //
      //   <map-repetition> group <name> (MAP) {
      //     repeated group map (MAP_KEY_VALUE) {
      //                    ^~~  repeatedGroupName
      //       required <key-type> key;
      //       <value-repetition> <value-type> value;
      //     }
      //   }
      "map"
    } else {
      // Standard mode:
      //
      //   <map-repetition> group <name> (MAP) {
      //     repeated group key_value {
      //                    ^~~~~~~~~  repeatedGroupName
      //       required <key-type> key;
      //       <value-repetition> <value-type> value;
      //     }
      //   }
      "key_value"
    }

    (row: SpecializedGetters, ordinal: Int) => {
      val map = row.getMap(ordinal)
      val keyArray = map.keyArray()
      val valueArray = map.valueArray()

      consumeGroup {
        // Only creates the repeated field if the map is non-empty.
        if (map.numElements() > 0) {
          consumeField(repeatedGroupName, 0) {
            var i = 0
            while (i < map.numElements()) {
              consumeGroup {
                consumeField("key", 0) {
                  keyWriter.apply(keyArray, i)
                }

                // Only creates the "value" field if the value if non-empty
                if (!map.valueArray().isNullAt(i)) {
                  consumeField("value", 1) {
                    valueWriter.apply(valueArray, i)
                  }
                }
              }
              i += 1
            }
          }
        }
      }
    }
  }

  private def consumeMessage(f: => Unit): Unit = {
    recordConsumer.startMessage()
    f
    recordConsumer.endMessage()
  }

  private def consumeGroup(f: => Unit): Unit = {
    recordConsumer.startGroup()
    f
    recordConsumer.endGroup()
  }

  private def consumeField(field: String, index: Int)(f: => Unit): Unit = {
    recordConsumer.startField(field, index)
    f
    recordConsumer.endField(field, index)
  }
}

object GeoParquetWriteSupport {
  class GeometryColumnInfo {
    val bbox: GeometryColumnBoundingBox = new GeometryColumnBoundingBox()

    // GeoParquet column metadata has a `geometry_types` property, which contains a list of geometry types
    // that are present in the column.
    val seenGeometryTypes: mutable.Set[String] = mutable.Set.empty

    def update(geom: Geometry): Unit = {
      bbox.update(geom)
      // In case of 3D geometries, a " Z" suffix gets added (e.g. ["Point Z"]).
      val hasZ = {
        val coordinate = geom.getCoordinate
        if (coordinate != null) !coordinate.getZ.isNaN else false
      }
      val geometryType = if (!hasZ) geom.getGeometryType else geom.getGeometryType + " Z"
      seenGeometryTypes.add(geometryType)
    }
  }

  class GeometryColumnBoundingBox(
      var minX: Double = Double.PositiveInfinity,
      var minY: Double = Double.PositiveInfinity,
      var maxX: Double = Double.NegativeInfinity,
      var maxY: Double = Double.NegativeInfinity) {
    def update(geom: Geometry): Unit = {
      val env = geom.getEnvelopeInternal
      minX = math.min(minX, env.getMinX)
      minY = math.min(minY, env.getMinY)
      maxX = math.max(maxX, env.getMaxX)
      maxY = math.max(maxY, env.getMaxY)
    }
  }

  private def getSparkSqlParquetRowMetadata(schema: StructType): String = {
    val fields = schema.fields.map { field =>
      field.dataType match {
        case _: GeometryUDT =>
          // Don't write the GeometryUDT type to the Parquet metadata. Write the type as binary for maximum
          // compatibility.
          field.copy(dataType = BinaryType)
        case _ => field
      }
    }
    StructType(fields).json
  }
}
