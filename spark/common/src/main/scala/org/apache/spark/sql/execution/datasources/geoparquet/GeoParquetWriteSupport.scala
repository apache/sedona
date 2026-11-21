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
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetMetaData.{GEOPARQUET_COVERING_KEY, GEOPARQUET_COVERING_MODE_AUTO, GEOPARQUET_COVERING_MODE_KEY, GEOPARQUET_COVERING_MODE_LEGACY, GEOPARQUET_CRS_KEY, GEOPARQUET_VERSION_KEY, VERSION, createCoveringColumnMetadata}
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetWriteSupport.GeometryColumnInfo
import org.apache.spark.sql.execution.datasources.geoparquet.internal.{DataSourceUtils, LegacyBehaviorPolicy, PortableSQLConf}
import org.apache.spark.sql.sedona_sql.types.{PublicWkbHeaderAdapter, SpatialTypeSupport}
import org.apache.spark.sql.types._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse
import org.locationtech.jts.geom.{CoordinateSequence, Geometry, LineString, Point, Polygon}
import org.locationtech.jts.io.ByteOrderValues
import org.datasyslab.jts.io.WKBWriter

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
  private var userExplicitlySetDefaultCrs: Boolean = false
  private val geoParquetColumnCrsMap: mutable.Map[String, Option[JValue]] = mutable.Map.empty
  // The CRS of the geometry columns for which it is known before the first row is written: the
  // columns of a native Spark type, and every geometry column of a GeoParquet 2 file.
  private var fixedColumnCrs: Map[Int, Option[JValue]] = Map.empty
  // The SRID that FirstGeometryOutputWriter found for a GeometryUDT column of a GeoParquet 2 file,
  // None when it found no geometry before it had to open the file.
  private val firstGeometrySrids: mutable.Map[Int, Option[Int]] = mutable.Map.empty
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
      if (SpatialTypeSupport.isGeometry(field.dataType)) {
        geometryColumnInfoMap.getOrElseUpdate(ordinal, new GeometryColumnInfo())
      }
    }

    // prepareWrite already checked this on the driver
    if (geometryColumnInfoMap.isEmpty) {
      GeoParquetWriteSupport.requireGeometryColumn(schema)
    }

    geoParquetVersion = configuration.get(GEOPARQUET_VERSION_KEY) match {
      case null => Some(VERSION)
      // The version decides the layout of the file with the spaces around it ignored, and the
      // metadata states it in the same form
      case version: String => Some(version.trim)
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
        // If no CRS is specified, we default to deriving CRS from the geometry SRID in finalizeWrite.
        // This JNull value is used as a fallback when SRID is 0 or SRID-to-PROJJSON conversion fails,
        // maintaining compatibility with geopandas 0.10.0 and earlier versions, which require a crs field.
        Some(org.json4s.JNull)
      case "" =>
        userExplicitlySetDefaultCrs = true
        None
      case crs: String =>
        userExplicitlySetDefaultCrs = true
        Some(parse(crs))
    }
    GeoParquetWriteSupport
      .columnCrsOptions(configuration, geometryColumnInfoMap.keys.map(schema(_).name).toSeq)
      .foreach {
        case (name, "") => geoParquetColumnCrsMap.put(name, None)
        case (name, crs) => geoParquetColumnCrsMap.put(name, Some(parse(crs)))
      }
    fixedColumnCrs = geometryColumnInfoMap.keys.flatMap { ordinal =>
      val field = schema(ordinal)
      val explicit = geoParquetColumnCrsMap.get(field.name).orElse {
        if (userExplicitlySetDefaultCrs) Some(defaultGeoParquetCrs) else None
      }
      SpatialTypeSupport.geometrySrid(field.dataType) match {
        case Some(srid) =>
          Some(ordinal -> GeoParquetNativeCrs.writeCrs(field.name, srid, explicit))
        case None if writesGeospatialTypes =>
          // The GEOMETRY logical type states the CRS in the Parquet schema, and a GeometryUDT
          // column only has an SRID on each geometry. FirstGeometryOutputWriter looks it up.
          Some(ordinal -> explicit.getOrElse {
            val found =
              configuration.get(GeoParquetWriteSupport.FIRST_GEOMETRY_SRID_KEY + ordinal)
            if (found == null) {
              throw new IllegalArgumentException(
                s"GeoParquet ${geoParquetVersion.get} states the CRS of column '${field.name}' " +
                  "in the Parquet schema, which is fixed before the first row is written. Set " +
                  s"$GEOPARQUET_CRS_KEY or $GEOPARQUET_CRS_KEY.${field.name} to the CRS of the " +
                  "column.")
            }
            val srid =
              if (found == GeoParquetWriteSupport.NO_FIRST_GEOMETRY) None else Some(found.toInt)
            firstGeometrySrids.put(ordinal, srid)
            GeoParquetNativeCrs.writeCrs(field.name, srid.getOrElse(0), None)
          })
        case None => None
      }
    }.toMap
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

    val messageType = withGeospatialTypes(
      new internal.SparkToParquetSchemaConverter(configuration)
        .convert(GeoParquetSchemaConverter.physicalSchema(schema)))
    val sparkSqlParquetRowMetadata =
      GeoParquetWriteSupport.getSparkSqlParquetRowMetadata(schema, writesGeospatialTypes)
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
        // Empty geometries still contribute geometry_types but have no spatial extent.
        // Omit bbox when the file contains only empty/null geometries or no rows.
        // Omit it too when a bound is infinite or NaN. JSON has no number for such a bound, and
        // a bbox states the range of every dimension that it covers.
        val bbox =
          if (columnInfo.bbox.isEmpty) None
          else Some(columnInfo.bbox.bounds).filter(_.forall(b => !b.isNaN && !b.isInfinity))
        lazy val legacyCrs = geoParquetColumnCrsMap.getOrElse(
          columnName, {
            if (!userExplicitlySetDefaultCrs) {
              // No explicit CRS option was provided; try to derive from geometry SRID.
              // For SRID 4326 (OGC:CRS84), omit CRS entirely per GeoParquet spec default.
              columnInfo.observedSrid match {
                case Some(srid) if srid == GeoParquetMetaData.DEFAULT_SRID => None
                case Some(srid) if srid > 0 =>
                  GeoParquetMetaData.sridToProjJson(srid).orElse(defaultGeoParquetCrs)
                case _ => defaultGeoParquetCrs
              }
            } else {
              defaultGeoParquetCrs
            }
          })
        val crs = fixedColumnCrs.getOrElse(ordinal, legacyCrs)
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

  private def writesGeospatialTypes: Boolean =
    geoParquetVersion.exists(GeoParquetMetaData.usesGeospatialTypes)

  /**
   * Annotates the geometry columns of a GeoParquet 2 file with the GEOMETRY logical type, which
   * also makes the Parquet library collect geospatial statistics for their row groups.
   */
  private def withGeospatialTypes(messageType: MessageType): MessageType = {
    if (!writesGeospatialTypes) return messageType
    val fields = messageType.getFields.asScala.zipWithIndex.map { case (field, ordinal) =>
      fixedColumnCrs.get(ordinal) match {
        case Some(crs) =>
          val geometry = Types
            .primitive(PrimitiveTypeName.BINARY, field.getRepetition)
            .as(ParquetGeospatialTypes.geometry(ParquetGeospatialTypes.crsProperty(crs)))
          Option(field.getId)
            .map(id => geometry.id(id.intValue))
            .getOrElse(geometry)
            .named(field.getName)
        case None => field
      }
    }
    new MessageType(messageType.getName, fields.asJava)
  }

  private def isAutoCoveringEnabled: Boolean = {
    geoParquetCoveringMode == GEOPARQUET_COVERING_MODE_AUTO && isGeoParquet11
  }

  private def makeGeneratedCoveringWriter(geometryOrdinal: Int): ValueWriter = {
    (row: SpecializedGetters, _: Int) =>
      val geom =
        SpatialTypeSupport.readGeometry(row, geometryOrdinal, schema(geometryOrdinal).dataType)
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

      case dt if SpatialTypeSupport.isGeometry(dt) =>
        val geometryColumnInfo = rootOrdinal match {
          case Some(ordinal) =>
            geometryColumnInfoMap.getOrElseUpdate(ordinal, new GeometryColumnInfo())
          case None => null
        }
        val byteOrder = if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
          ByteOrderValues.BIG_ENDIAN
        } else ByteOrderValues.LITTLE_ENDIAN
        // Preserve declared dimensions even for empty geometries. GeoParquet 1.1 has no M
        // coordinate, so a 1.x file keeps X, Y and Z. GeoParquet 2.0 has all four.
        val geospatialTypes = writesGeospatialTypes
        val wkbWriter = new WKBWriter(if (geospatialTypes) 4 else 3, byteOrder, false)
        wkbWriter.setPreserveCoordinateDimensions(true)
        val firstGeometrySrid = rootOrdinal.flatMap(firstGeometrySrids.get)
        (row: SpecializedGetters, ordinal: Int) => {
          // A native geometry value of a 2.x file is stored as it is. It is ISO WKB of any of
          // the four coordinate layouts, and it keeps the layout that an empty multi-geometry
          // or collection declares, which geom cannot hold. geom is read from the same WKB, so
          // the value is converted once. A 1.x file has no M coordinate, so its geometries are
          // written again from geom.
          val (geom, heldWkb) =
            if (geospatialTypes) {
              SpatialTypeSupport.readGeometryWithWkb(
                row,
                ordinal,
                dataType,
                ByteOrder.nativeOrder())
            } else (SpatialTypeSupport.readGeometry(row, ordinal, dataType), None)
          SpatialTypeSupport.geometrySrid(dataType).foreach { expectedSrid =>
            GeoParquetNativeCrs.requireSrid(
              rootOrdinal.map(schema(_).name).getOrElse("<nested geometry>"),
              expectedSrid,
              geom.getSRID)
          }
          firstGeometrySrid.foreach { expected =>
            if (geom.getSRID != expected.getOrElse(0)) {
              throw GeoParquetWriteSupport.secondSrid(
                geoParquetVersion.get,
                schema(rootOrdinal.get).name,
                expected,
                geom.getSRID)
            }
          }
          val nativeWkb = heldWkb.map(PublicWkbHeaderAdapter.toIsoWkb)
          // The writer marks a Z coordinate with the EWKB flag bit. GeoParquet asks for the WKB
          // of the OGC and ISO standards, which uses the type codes 1001 to 1007 instead. The
          // writer returns a new array without an SRID, so the type words change in that array.
          val wkb =
            nativeWkb.getOrElse(PublicWkbHeaderAdapter.toIsoWkbInPlace(wkbWriter.write(geom)))
          recordConsumer.addBinary(Binary.fromReusedByteArray(wkb))
          if (geometryColumnInfo != null) {
            // The writer resolves each geometry's actual layout, including collection members.
            // Derive metadata from its WKB type code rather than the first coordinate.
            // ISO WKB adds 1000 to the type code for Z, 2000 for M and 3000 for both.
            val typeCode = ByteBuffer.wrap(wkb, 1, 4).order(ByteOrder.nativeOrder()).getInt
            val layout = typeCode / 1000
            geometryColumnInfo.update(geom, hasZ = layout % 2 == 1, hasM = layout >= 2)
            // The bbox of a 2.x file states the Z and M bounds too. A 1.x file keeps the X and
            // Y bounds that Sedona has always written for it.
            if (geospatialTypes) geometryColumnInfo.bbox.updateZAndM(geom)
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

  /**
   * Prefix of the configuration keys under which [[FirstGeometryOutputWriter]] passes the SRID of
   * the first geometry of a column, followed by the ordinal of the column. The value is
   * [[NO_FIRST_GEOMETRY]] when it found none.
   */
  private[geoparquet] val FIRST_GEOMETRY_SRID_KEY =
    "org.apache.sedona.geoparquet.first-geometry-srid."
  private[geoparquet] val NO_FIRST_GEOMETRY = "none"

  /**
   * Configuration key for the bytes of rows that a GeoParquet 2 write holds back while it waits
   * for the first geometry of a GeometryUDT column. It is not under geoparquet.crs, where every
   * key that goes on names a column.
   */
  val GEOPARQUET_SRID_LOOKAHEAD_KEY = "geoparquet.srid.lookahead.bytes"
  val DEFAULT_CRS_LOOKAHEAD_BYTES: Long = 16L * 1024 * 1024

  /**
   * The ordinals of the geometry columns whose CRS a GeoParquet 2 write takes from the data:
   * GeometryUDT columns that no CRS option covers. A native Spark geometry type names its SRID,
   * and a GeoParquet 1 file states the CRS in metadata that is written after the last row.
   */
  def columnsWithoutCrs(schema: StructType, configuration: Configuration): Seq[Int] = {
    val version = Option(configuration.get(GEOPARQUET_VERSION_KEY)).getOrElse(VERSION)
    if (!GeoParquetMetaData.usesGeospatialTypes(version) ||
      configuration.get(GEOPARQUET_CRS_KEY) != null) {
      Seq.empty
    } else {
      val candidates = schema.fields.toSeq.zipWithIndex.filter { case (field, _) =>
        SpatialTypeSupport.isGeometry(field.dataType) &&
        SpatialTypeSupport.geometrySrid(field.dataType).isEmpty
      }
      val stated = columnCrsOptions(configuration, candidates.map(_._1.name))
      candidates.collect { case (field, ordinal) if !stated.contains(field.name) => ordinal }
    }
  }

  /** The CRS option of each of `columns` that has one, under the key [[setWriteOptions]] set. */
  private[geoparquet] def columnCrsOptions(
      configuration: Configuration,
      columns: Seq[String]): Map[String, String] =
    columns.flatMap { column =>
      Option(configuration.get(GEOPARQUET_CRS_KEY + "." + column)).map(column -> _)
    }.toMap

  /** The keys of the options of a GeoParquet write that are complete as they are. */
  private val WRITE_OPTION_KEYS = Seq(
    GEOPARQUET_VERSION_KEY,
    GEOPARQUET_CRS_KEY,
    GEOPARQUET_COVERING_KEY,
    GEOPARQUET_COVERING_MODE_KEY,
    GEOPARQUET_SRID_LOOKAHEAD_KEY)

  /** The keys of the options of a GeoParquet write that go on with the name of a column. */
  private val COLUMN_WRITE_OPTION_KEYS =
    Seq(GEOPARQUET_CRS_KEY + ".", GEOPARQUET_COVERING_KEY + ".")

  /**
   * Sets the GeoParquet options of a write in `configuration` under the keys that the writer
   * reads there. Spark takes the keys of the options of a write without regard to case, and
   * copies them to the Hadoop configuration as they were written, where a key in another case is
   * another key. An option of the write replaces a value that the configuration has from
   * elsewhere.
   *
   * A key that goes on with the name of a geometry column, in the case of that column, is for
   * that column. This comes before anything else that the key is in another case:
   * `geoparquet.covering.MODE` is the covering of a column `MODE`, not the covering mode. When no
   * geometry column has the name in that case and the key is no other option either, the key is
   * for the column whose name differs from it in case only, and is rejected when there are
   * several of those.
   */
  def setWriteOptions(
      options: Map[String, String],
      schema: StructType,
      configuration: Configuration): Unit = {
    def lowerCase(text: String): String = text.toLowerCase(java.util.Locale.ROOT)
    val geometryColumns =
      schema.fields.toSeq
        .filter(field => SpatialTypeSupport.isGeometry(field.dataType))
        .map(_.name)
    // The map that Spark hands over lists its keys in lower case, which would take the case
    // from the column names in them
    val asWritten = options match {
      case insensitive: CaseInsensitiveMap[String @unchecked] => insensitive.originalMap
      case other => other
    }
    asWritten.foreach { case (key, value) =>
      // The name that the key goes on with, if it is the key of a column
      val ofColumn = COLUMN_WRITE_OPTION_KEYS
        .find(lowerCase(key).startsWith)
        .map(prefix => prefix -> key.substring(prefix.length))
      ofColumn match {
        case Some((prefix, name)) if geometryColumns.contains(name) =>
          configuration.set(prefix + name, value)
        case _ =>
          WRITE_OPTION_KEYS.find(_ == lowerCase(key)) match {
            case Some(complete) => configuration.set(complete, value)
            case None =>
              ofColumn.foreach { case (prefix, name) =>
                val columns = geometryColumns.filter(lowerCase(_) == lowerCase(name))
                if (columns.size > 1) {
                  throw new IllegalArgumentException(
                    s"Option $key names the geometry columns " +
                      s"${columns.mkString("'", "', '", "'")}, which differ in case only. " +
                      "Write the name in the key in the case of the column that the option " +
                      "is for.")
                }
                columns.foreach(column => configuration.set(prefix + column, value))
              }
          }
      }
    }
  }

  private def secondSrid(
      version: String,
      column: String,
      first: Option[Int],
      second: Int): IllegalArgumentException = {
    val found = first match {
      case Some(srid) => s"The first geometry of column '$column' has SRID $srid"
      case None =>
        s"Column '$column' had no geometry in the first rows of the file, so its CRS was " +
          "written as unknown"
    }
    new IllegalArgumentException(
      s"$found, but another geometry has SRID $second. A GeoParquet $version column has a " +
        "single CRS: give all its geometries the same SRID, or set " +
        s"$GEOPARQUET_CRS_KEY or $GEOPARQUET_CRS_KEY.$column to state the CRS.")
  }

  /** GeoParquet 2 needs the geospatial logical types of the Parquet library on the classpath. */
  def requireSupportedVersion(configuration: Configuration): Unit = {
    Option(configuration.get(GEOPARQUET_VERSION_KEY)).foreach { version =>
      if (GeoParquetMetaData.usesGeospatialTypes(version) && !ParquetGeospatialTypes.supported) {
        throw new IllegalArgumentException(
          s"GeoParquet $version stores geometries in the Parquet GEOMETRY logical type, which " +
            "needs Spark 4.1 or later (parquet-java 1.16.0). Set " +
            s"$GEOPARQUET_VERSION_KEY to $VERSION on this Spark version.")
      }
    }
  }

  /**
   * Fails unless `schema` has a geometry column, which GeoParquet requires. Called from
   * `prepareWrite` on the driver, so a write without one fails before any task is launched rather
   * than in every task.
   */
  def requireGeometryColumn(schema: StructType): Unit = {
    if (schema.exists(field =>
        !SpatialTypeSupport.isNativeGeometry(field.dataType) &&
          field.dataType.existsRecursively(SpatialTypeSupport.isNativeGeometry))) {
      throw new IllegalArgumentException(
        "GeoParquet native geometry columns must be at the root of the schema; " +
          "nested CRS metadata cannot be preserved")
    }
    if (schema.existsRecursively(dt =>
        SpatialTypeSupport.isNativeGeometry(dt) && SpatialTypeSupport.geometrySrid(dt).isEmpty)) {
      throw new IllegalArgumentException(
        "GeoParquet requires a fixed native geometry CRS, as Spark Parquet does; " +
          "cast GEOMETRY(ANY) columns to GEOMETRY(<srid>) before writing")
    }
    if (!schema.exists(field => SpatialTypeSupport.isGeometry(field.dataType))) {
      throw new IllegalArgumentException(
        "GeoParquet requires at least one geometry column, but the schema has none: " +
          schema.simpleString + ". Use format(\"parquet\") to write a table without " +
          "geometry columns.")
    }
  }

  class GeometryColumnInfo {
    val bbox: GeometryColumnBoundingBox = new GeometryColumnBoundingBox()

    // GeoParquet column metadata has a `geometry_types` property, which contains a list of geometry types
    // that are present in the column.
    val seenGeometryTypes: mutable.Set[String] = mutable.Set.empty

    // Track SRIDs seen in geometry values. A consistent SRID can be used to
    // auto-generate CRS (projjson) metadata when no explicit CRS is provided:
    // SRID 4326 results in omitted CRS (GeoParquet default), positive non-4326
    // SRIDs generate PROJJSON, and SRID 0 or mixed SRIDs result in null CRS.
    private var _srid: Int = -1 // -1 = no geometries seen yet
    private var _mixedSrids: Boolean = false

    /**
     * Returns the observed SRID if all geometries had the same SRID, or None if no geometries
     * were seen or if mixed SRIDs were encountered.
     */
    def observedSrid: Option[Int] = {
      if (_mixedSrids || _srid == -1) None
      else Some(_srid)
    }

    def update(geom: Geometry, hasZ: Boolean, hasM: Boolean): Unit = {
      bbox.update(geom)
      // Match the dimensionality encoded in WKB, including typed empty geometries.
      val layout = (if (hasZ) "Z" else "") + (if (hasM) "M" else "")
      val geometryType =
        if (layout.isEmpty) geom.getGeometryType else geom.getGeometryType + " " + layout
      seenGeometryTypes.add(geometryType)

      // Track SRID consistency across all geometries in this column
      if (!_mixedSrids) {
        val geomSrid = geom.getSRID
        if (_srid == -1) {
          _srid = geomSrid
        } else if (_srid != geomSrid) {
          _mixedSrids = true
        }
      }
    }
  }

  class GeometryColumnBoundingBox(
      var minX: Double = Double.PositiveInfinity,
      var minY: Double = Double.PositiveInfinity,
      var maxX: Double = Double.NegativeInfinity,
      var maxY: Double = Double.NegativeInfinity) {
    private var minZ = Double.PositiveInfinity
    private var maxZ = Double.NegativeInfinity
    private var minM = Double.PositiveInfinity
    private var maxM = Double.NegativeInfinity

    def isEmpty: Boolean = minX > maxX || minY > maxY

    def update(geom: Geometry): Unit = {
      val env = geom.getEnvelopeInternal
      if (!env.isNull) {
        minX = math.min(minX, env.getMinX)
        minY = math.min(minY, env.getMinY)
        maxX = math.max(maxX, env.getMaxX)
        maxY = math.max(maxY, env.getMaxY)
      }
    }

    /**
     * Takes the Z and M coordinates of `geom` into the bounds. A coordinate that is NaN is not a
     * bound, as in the bounding box of the Parquet statistics.
     */
    def updateZAndM(geom: Geometry): Unit = geom match {
      case point: Point => updateZAndM(point.getCoordinateSequence)
      case line: LineString => updateZAndM(line.getCoordinateSequence)
      case polygon: Polygon =>
        updateZAndM(polygon.getExteriorRing.getCoordinateSequence)
        var i = 0
        while (i < polygon.getNumInteriorRing) {
          updateZAndM(polygon.getInteriorRingN(i).getCoordinateSequence)
          i += 1
        }
      case _ =>
        var i = 0
        while (i < geom.getNumGeometries) {
          updateZAndM(geom.getGeometryN(i))
          i += 1
        }
    }

    private def updateZAndM(coordinates: CoordinateSequence): Unit = {
      val hasZ = coordinates.hasZ
      val hasM = coordinates.hasM
      if (hasZ || hasM) {
        var i = 0
        while (i < coordinates.size) {
          if (hasZ) {
            val z = coordinates.getZ(i)
            if (!z.isNaN) {
              minZ = math.min(minZ, z)
              maxZ = math.max(maxZ, z)
            }
          }
          if (hasM) {
            val m = coordinates.getM(i)
            if (!m.isNaN) {
              minM = math.min(minM, m)
              maxM = math.max(maxM, m)
            }
          }
          i += 1
        }
      }
    }

    /**
     * The bbox of the column metadata: the lower bound of every dimension and then the upper
     * bound of every dimension. X and Y come first, then Z, then M. A dimension without a
     * coordinate that is a number has no bounds and is left out. GeoParquet has no bbox with M
     * bounds and without Z bounds, so the M bounds are stated only next to the Z bounds.
     */
    def bounds: Seq[Double] = {
      val hasZ = minZ <= maxZ
      val hasM = minM <= maxM
      if (hasZ && hasM) Seq(minX, minY, minZ, minM, maxX, maxY, maxZ, maxM)
      else if (hasZ) Seq(minX, minY, minZ, maxX, maxY, maxZ)
      else Seq(minX, minY, maxX, maxY)
    }
  }

  /**
   * The Spark schema that the file records for Spark's own Parquet reader.
   *
   * @param keepNativeGeometry
   *   whether the file stores geometries in the GEOMETRY logical type. Spark's own writer records
   *   its native geometry type for such a column, and its reader needs that to return a geometry.
   */
  private def getSparkSqlParquetRowMetadata(
      schema: StructType,
      keepNativeGeometry: Boolean): String = {
    val fields = schema.fields.map { field =>
      field.dataType match {
        case dt if keepNativeGeometry && SpatialTypeSupport.isNativeGeometry(dt) => field
        case dt if SpatialTypeSupport.isGeometry(dt) =>
          // Don't write the spatial type to the Parquet metadata. Write the type as binary for maximum
          // compatibility.
          field.copy(dataType = BinaryType)
        case _ => field
      }
    }
    StructType(fields).json
  }
}
