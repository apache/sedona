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
package org.apache.spark.sql.sedona_sql.io.geojson

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.io.geojson.GeoJSONUtils.geometryToGeoJson
import org.apache.spark.sql.types._
import org.locationtech.jts.io.WKTWriter

import java.io.Writer

/**
 * `GeoJSONJacksonGenerator` is taken from [[JacksonGenerator]] with slight modifications to write
 * rows as GeoJSON Feature objects. It handles geometry columns properly and wrap all other
 * columns in `properties` column.
 */
private[sql] class GeoJSONJacksonGenerator(
    schema: StructType,
    geometryColumnName: String,
    writer: Writer,
    options: JSONOptions) {
  // A `ValueWriter` is responsible for writing a field of an `InternalRow` to appropriate
  // JSON data. Here we are using `SpecializedGetters` rather than `InternalRow` so that
  // we can directly access data in `ArrayData` without the help of `SpecificMutableRow`.
  private type ValueWriter = (SpecializedGetters, Int) => Unit

  // `ValueWriter`s for all fields of the schema
  private lazy val rootFieldWriters: Array[ValueWriter] =
    schema.map(field => makeWriter(field.dataType, field.name)).toArray

  // If there is a field named `properties` and it is a struct or map, we will write all fields to top-level
  // of the GeoJSON, otherwise we'll wrap all fields in `properties` field.
  private val hasPropertiesField = schema.fields.exists { field =>
    field.name == "properties" && (field.dataType.isInstanceOf[StructType] || field.dataType
      .isInstanceOf[MapType])
  }

  private val gen = {
    val generator = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
    if (options.pretty) {
      generator.setPrettyPrinter(new DefaultPrettyPrinter(""))
    }
    generator
  }

  private val lineSeparator: String = options.lineSeparatorInWrite

  private val timestampFormatter = SparkCompatUtil.constructTimestampFormatter(
    options,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
  private val dateFormatter = SparkCompatUtil.constructDateFormatter(
    options,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)

  private val wktWriter = new WKTWriter()
  private var currentGeoJsonString: Option[String] = None

  private def makeWriter(dataType: DataType, name: String): ValueWriter = dataType match {
    case NullType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNull()

    case BooleanType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeBoolean(row.getBoolean(ordinal))

    case ByteType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getByte(ordinal))

    case ShortType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getShort(ordinal))

    case IntegerType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getInt(ordinal))

    case LongType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getLong(ordinal))

    case FloatType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getFloat(ordinal))

    case DoubleType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeNumber(row.getDouble(ordinal))

    case StringType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeString(row.getUTF8String(ordinal).toString)

    case TimestampType =>
      (row: SpecializedGetters, ordinal: Int) =>
        val timestampString = timestampFormatter.format(row.getLong(ordinal))
        gen.writeString(timestampString)

    case DateType =>
      (row: SpecializedGetters, ordinal: Int) =>
        val dateString = dateFormatter.format(row.getInt(ordinal))
        gen.writeString(dateString)

    case CalendarIntervalType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeString(row.getInterval(ordinal).toString)

    case BinaryType =>
      (row: SpecializedGetters, ordinal: Int) => gen.writeBinary(row.getBinary(ordinal))

    case dt: DecimalType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getDecimal(ordinal, dt.precision, dt.scale).toJavaBigDecimal)

    case st: StructType =>
      val fieldWriters = st.map(field => makeWriter(field.dataType, name + "." + field.name))
      (row: SpecializedGetters, ordinal: Int) =>
        writeObject(writeFields(name, row.getStruct(ordinal, st.length), st, fieldWriters))

    case at: ArrayType =>
      val elementWriter = makeWriter(at.elementType, name)
      (row: SpecializedGetters, ordinal: Int) =>
        writeArray(writeArrayData(row.getArray(ordinal), elementWriter))

    case mt: MapType =>
      val valueWriter = makeWriter(mt.valueType, name)
      (row: SpecializedGetters, ordinal: Int) =>
        writeObject(writeMapData(row.getMap(ordinal), mt, valueWriter))

    case GeometryUDT =>
      // We'll only write non-primary geometry columns here, we'll write it as WKT to properties object.
      (row: SpecializedGetters, ordinal: Int) =>
        {
          val geom = GeometryUDT.deserialize(row.getBinary(ordinal))
          val wkt = wktWriter.write(geom)
          gen.writeString(wkt)
        }

      // For UDT values, they should be in the SQL type's corresponding value type.
      // We should not see values in the user-defined class at here.
      // For example, VectorUDT's SQL type is an array of double. So, we should expect that v is
    // an ArrayData at here, instead of a Vector.
    case t: UserDefinedType[_] =>
      makeWriter(t.sqlType, name)

    case _ =>
      (row: SpecializedGetters, ordinal: Int) =>
        val v = row.get(ordinal, dataType)
        throw new IllegalArgumentException(
          s"Unsupported dataType: ${dataType.catalogString} " +
            s"with value $v")
  }

  private def writeObject(f: => Unit): Unit = {
    gen.writeStartObject()
    f
    gen.writeEndObject()
  }

  private def writeFields(
      name: String,
      row: InternalRow,
      schema: StructType,
      fieldWriters: Seq[ValueWriter]): Unit = {
    var i = 0
    while (i < row.numFields) {
      val field = schema(i)
      val fullFieldName = if (name.isEmpty) field.name else s"$name.${field.name}"
      if (fullFieldName == geometryColumnName) {
        // Primary geometry field, don't need to write out the key. We save the geometry and will write it out as
        // geometry field after we've done with writing the properties field.
        if (!row.isNullAt(i)) {
          val geoJson = geometryToGeoJson(row.getBinary(i))
          currentGeoJsonString = Some(geoJson)
        } else {
          currentGeoJsonString = None
        }
      } else {
        // properties field, simply write it out
        if (hasPropertiesField && (fullFieldName == "type" || fullFieldName == "geometry")) {
          // Ignore top-level type and geometry fields
        } else {
          if (!row.isNullAt(i)) {
            gen.writeFieldName(field.name)
            fieldWriters(i).apply(row, i)
          } else if (!options.ignoreNullFields) {
            gen.writeFieldName(field.name)
            gen.writeNull()
          }
        }
      }
      i += 1
    }
  }

  private def writeArray(f: => Unit): Unit = {
    gen.writeStartArray()
    f
    gen.writeEndArray()
  }

  private def writeArrayData(array: ArrayData, fieldWriter: ValueWriter): Unit = {
    var i = 0
    while (i < array.numElements()) {
      if (!array.isNullAt(i)) {
        fieldWriter.apply(array, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }
  }

  private def writeMapData(map: MapData, mapType: MapType, fieldWriter: ValueWriter): Unit = {
    val keyArray = map.keyArray()
    val valueArray = map.valueArray()
    var i = 0
    while (i < map.numElements()) {
      gen.writeFieldName(keyArray.get(i, mapType.keyType).toString)
      if (!valueArray.isNullAt(i)) {
        fieldWriter.apply(valueArray, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }
  }

  def close(): Unit = gen.close()

  /**
   * Transforms a single `InternalRow` to JSON object using Jackson. This api calling will be
   * validated through accessing `rootFieldWriters`.
   *
   * @param row
   *   The row to convert
   */
  def write(row: InternalRow): Unit = {
    currentGeoJsonString = None

    gen.writeStartObject()
    gen.writeStringField("type", "Feature")

    if (hasPropertiesField) {
      // Write all fields as top-level fields
      writeFields(name = "", fieldWriters = rootFieldWriters, row = row, schema = schema)
    } else {
      // Wrap all fields in `properties` field
      gen.writeFieldName("properties")
      gen.writeStartObject()
      writeFields(name = "", fieldWriters = rootFieldWriters, row = row, schema = schema)
      gen.writeEndObject()
    }

    // write geometry
    gen.writeFieldName("geometry")
    currentGeoJsonString match {
      case Some(geoJson) => gen.writeRawValue(geoJson)
      case None => gen.writeNull()
    }

    // end of feature
    gen.writeEndObject()
  }

  def writeLineEnding(): Unit = {
    // Note that JSON uses writer with UTF-8 charset. This string will be written out as UTF-8.
    gen.writeRaw(lineSeparator)
  }
}
