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
package org.apache.sedona.sql

import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import java.io.File
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files
import java.util.Collections
import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupReadSupport}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.apache.sedona.common.utils.GeometryEquality
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.geoparquet.ParquetGeospatialTypes
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, StructField, StructType}
import org.json4s.{DefaultFormats, JNothing, JValue}
import org.json4s.jackson.parseJson
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.datasyslab.jts.io.WKBReader

/** File-level tests use hand-built ISO WKB, independently of the GeoParquet writer. */
class GeoParquetDimensionsSuite extends TestBaseScala {
  private case class Sample(name: String, wkb: Array[Byte])

  private def isoWkb(
      kind: Int,
      z: Boolean,
      coordinates: Seq[Double] = Seq.empty,
      children: Seq[Array[Byte]] = Seq.empty,
      order: ByteOrder = ByteOrder.LITTLE_ENDIAN,
      m: Boolean = false): Array[Byte] = {
    val dimension = 2 + (if (z) 1 else 0) + (if (m) 1 else 0)
    val payload =
      if (kind == 1) coordinates.size * 8
      else if (kind == 2) 4 + coordinates.size * 8
      else if (kind >= 4 && kind <= 7) 4 + children.map(_.length).sum
      else 4
    val buffer = ByteBuffer.allocate(5 + payload).order(order)
    buffer.put(if (order == ByteOrder.LITTLE_ENDIAN) 1.toByte else 0.toByte)
    buffer.putInt(kind + (if (z) 1000 else 0) + (if (m) 2000 else 0))
    if (kind == 1) coordinates.foreach(buffer.putDouble)
    else if (kind == 2) {
      require(coordinates.size % dimension == 0)
      buffer.putInt(coordinates.size / dimension)
      coordinates.foreach(buffer.putDouble)
    } else if (kind >= 4 && kind <= 7) {
      buffer.putInt(children.size)
      children.foreach(buffer.put)
    } else buffer.putInt(0)
    buffer.array()
  }

  private def samples: Seq[Sample] = {
    val primitives = for {
      order <- Seq(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN)
      z <- Seq(false, true)
      kind <- Seq(1, 2, 3)
    } yield Sample(
      s"empty-$kind-z=$z-$order",
      isoWkb(
        kind,
        z,
        if (kind == 1) Seq.fill(if (z) 3 else 2)(Double.NaN) else Seq.empty,
        order = order))
    val lines = Seq(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN).map { order =>
      Sample(
        s"later-finite-z-$order",
        isoWkb(2, true, Seq(1, 2, Double.NaN, 3, 4, 5), order = order))
    }
    primitives ++ lines ++ Seq(
      Sample("null", null),
      Sample("nonempty-xyz", isoWkb(1, true, Seq(10, 20, 30))),
      Sample("nan-z", isoWkb(1, true, Seq(10, 20, Double.NaN))),
      Sample("empty-collection", isoWkb(7, false)),
      Sample(
        "empty-multipoint",
        isoWkb(4, true, children = Seq(isoWkb(1, true, Seq.fill(3)(Double.NaN))))),
      Sample("empty-multilinestring", isoWkb(5, true, children = Seq(isoWkb(2, true)))),
      Sample("empty-multipolygon", isoWkb(6, true, children = Seq(isoWkb(3, true)))),
      Sample(
        "mixed-empty-children",
        isoWkb(7, true, children = Seq(isoWkb(2, false), isoWkb(3, true)))),
      Sample(
        "nested-empty-children",
        isoWkb(
          7,
          true,
          children = Seq(isoWkb(7, true, children = Seq(isoWkb(2, true))), isoWkb(3, false)))))
  }

  private def unsupportedNative(sample: Sample): Boolean =
    sample.name.startsWith("later-finite-z") ||
      sample.name == "mixed-empty-children" || sample.name == "nested-empty-children"

  private def supportedNativeSamples: Seq[Sample] = samples.filterNot(unsupportedNative) ++ Seq(
    Sample("finite-xyz-line", isoWkb(2, true, Seq(1, 2, 3, 3, 4, 5))),
    Sample(
      "uniform-empty-children",
      isoWkb(7, true, children = Seq(isoWkb(2, true), isoWkb(3, true)))),
    Sample(
      "uniform-nested-empty-children",
      isoWkb(
        7,
        true,
        children = Seq(isoWkb(7, true, children = Seq(isoWkb(2, true))), isoWkb(3, true)))))

  private val legacySchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("geometry", GeometryUDT(), nullable = true)))

  private def readFile(path: String, legacyUdt: Boolean): DataFrame = {
    val reader = sparkSession.read.format("geoparquet")
    val df = if (legacyUdt) reader.schema(legacySchema).load(path) else reader.load(path)
    assert(
      SpatialTypeSupport.isNativeGeometry(df.schema("geometry").dataType) ==
        (SpatialTypeSupport.usesNativeTypes && !legacyUdt))
    df
  }

  private def assertNativeRejected(action: => Any): Unit = {
    val error = intercept[Exception](action)
    val causes = Iterator.iterate(error: Throwable)(_.getCause).takeWhile(_ != null)
    assert(
      causes.exists(cause => Option(cause.getMessage).exists(_.contains("WKB_PARSE_ERROR"))),
      error.toString)
  }

  private def withDirectory(f: File => Unit): Unit = {
    val directory = Files.createTempDirectory("geoparquet-dimensions-").toFile
    try f(directory)
    finally FileUtils.deleteDirectory(directory)
  }

  private def writeFixture(file: File, dictionary: Boolean, values: Seq[Sample]): Unit = {
    val schema = MessageTypeParser.parseMessageType(
      "message fixture { required int32 id; optional binary geometry; }")
    val factory = new SimpleGroupFactory(schema)
    val metadata = Collections.singletonMap(
      "geo",
      """{"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":[]}}}""")
    val writer = ExampleParquetWriter
      .builder(new Path(file.toURI))
      .withConf(new Configuration())
      .withType(schema)
      .withExtraMetaData(metadata)
      .withDictionaryEncoding(dictionary)
      .build()
    try {
      // Repeated values exercise the dictionary decoder, while IDs retain stable ordering.
      for (repeat <- 0 until 4; (sample, index) <- values.zipWithIndex) {
        val row = factory.newGroup().append("id", repeat * values.size + index)
        if (sample.wkb != null) row.append("geometry", Binary.fromConstantByteArray(sample.wkb))
        writer.write(row)
      }
    } finally writer.close()
    val reader =
      ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI), new Configuration()))
    try {
      val columns = reader.getFooter.getBlocks.asScala
        .flatMap(_.getColumns.asScala)
        .filter(_.getPath.toDotString == "geometry")
      assert(columns.nonEmpty)
      val encodedWithDictionary = columns.exists { column =>
        column.getEncodings.contains(Encoding.RLE_DICTIONARY) ||
        column.getEncodings.contains(Encoding.PLAIN_DICTIONARY)
      }
      assert(encodedWithDictionary == dictionary)
    } finally reader.close()
  }

  private def assertFile(
      path: String,
      values: Seq[Sample],
      repetitions: Int = 1,
      expectedSrid: Int = 0,
      legacyUdt: Boolean = false): Unit = {
    val df = readFile(path, legacyUdt)
    assertGeometries(df, values, "file read", repetitions, expectedSrid)
  }

  private def assertGeometries(
      df: DataFrame,
      values: Seq[Sample],
      context: String,
      repetitions: Int = 1,
      expectedSrid: Int = 0): Unit = {
    val projected = df.select("id", "geometry")
    val rows = projected.queryExecution.toRdd.map(_.copy()).collect().sortBy(_.getInt(0))
    assert(rows.length == values.size * repetitions)
    rows.zipWithIndex.foreach { case (row, index) =>
      val sample = values(index % values.size)
      withClue(s"${sample.name}, $context: ") {
        if (sample.wkb == null) assert(row.isNullAt(1))
        else {
          val expected = WKBReader.forDeclaredDimensions().read(sample.wkb)
          val actual = SpatialTypeSupport.readGeometry(row, 1, projected.schema(1).dataType)
          assert(actual.getSRID == expectedSrid, "column metadata SRID was not preserved")
          assert(
            GeometryEquality.equalsIdentical(actual, expected),
            s"declared layout or ordinates changed: $actual")
        }
      }
    }
  }

  private def hasZ(wkb: Array[Byte]): Boolean = {
    val order = if (wkb(0) == 0) ByteOrder.BIG_ENDIAN else ByteOrder.LITTLE_ENDIAN
    val kind = ByteBuffer.wrap(wkb).order(order).getInt(1)
    (kind & 0x80000000) != 0 || ((kind & 0x0fffffff) / 1000) % 2 == 1
  }

  private def typeCode(wkb: Array[Byte]): Int = {
    val order = if (wkb(0) == 0) ByteOrder.BIG_ENDIAN else ByteOrder.LITTLE_ENDIAN
    ByteBuffer.wrap(wkb).order(order).getInt(1)
  }

  private def hex(wkb: Array[Byte]): String = org.locationtech.jts.io.WKBWriter.toHex(wkb)

  private def columnMetadata(file: File): JValue = {
    val reader =
      ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI), new Configuration()))
    try
      parseJson(reader.getFooter.getFileMetaData.getKeyValueMetaData.get("geo")) \
        "columns" \ "geometry"
    finally reader.close()
  }

  private def assertWrittenFile(path: String, expected: Seq[Sample], allEmpty: Boolean): Unit = {
    val rows = sparkSession.read
      .parquet(path)
      .select("id", "geometry")
      .collect()
      .sortBy(_.getInt(0))
    assert(rows.length == expected.size)
    rows.zip(expected).foreach { case (row, sample) =>
      withClue(s"written ${sample.name}: ") {
        if (sample.wkb == null) assert(row.isNullAt(1))
        else {
          val bytes = row.getAs[Array[Byte]](1)
          assert(hasZ(bytes) == hasZ(sample.wkb), "top-level WKB Z header changed")
          assert(typeCode(bytes) == typeCode(sample.wkb), "top-level type code is not ISO WKB")
          assert(
            GeometryEquality.equalsIdentical(
              WKBReader.forDeclaredDimensions().read(bytes),
              WKBReader.forDeclaredDimensions().read(sample.wkb)),
            "WKB coordinate layout or ordinates changed")
        }
      }
    }
    val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
    assert(files.length == 1)
    val metadata = columnMetadata(files.head)
    implicit val formats: org.json4s.Formats = DefaultFormats
    val expectedTypes = expected
      .filter(_.wkb != null)
      .map { sample =>
        WKBReader.forDeclaredDimensions().read(sample.wkb).getGeometryType +
          (if (hasZ(sample.wkb)) " Z" else "")
      }
      .toSet
    assert((metadata \ "geometry_types").extract[Seq[String]].toSet == expectedTypes)
    if (allEmpty) assert((metadata \ "bbox") == JNothing)
    else assert((metadata \ "bbox").extract[Seq[Double]] == Seq(1, 2, 10, 20))
  }

  private def writeAndRoundTrip(directory: File, allEmpty: Boolean, legacyUdt: Boolean): Unit = {
    val native = SpatialTypeSupport.usesNativeTypes && !legacyUdt
    val projectedMeasures = Seq(
      Sample("xym", isoWkb(1, false, Seq(10, 20, 99), m = true)) ->
        Sample("xym", isoWkb(1, false, Seq(10, 20))),
      Sample("xyzm", isoWkb(1, true, Seq(10, 20, 30, 99), m = true)) ->
        Sample("xyzm", isoWkb(1, true, Seq(10, 20, 30))))
    val emptyMeasures = for {
      z <- Seq(false, true)
      kind <- Seq(1, 2, 3)
    } yield {
      val name = s"empty-measured-$kind-z=$z"
      Sample(
        name,
        isoWkb(
          kind,
          z,
          if (kind == 1) Seq.fill(if (z) 4 else 3)(Double.NaN) else Seq.empty,
          m = true)) ->
        Sample(
          name,
          isoWkb(kind, z, if (kind == 1) Seq.fill(if (z) 3 else 2)(Double.NaN) else Seq.empty))
    }
    val measuredLine = Sample(
      "later-finite-z-measured",
      isoWkb(2, true, Seq(1, 2, Double.NaN, 90, 3, 4, 5, 99), m = true)) ->
      Sample("later-finite-z-measured", isoWkb(2, true, Seq(1, 2, Double.NaN, 3, 4, 5)))
    if (native && !allEmpty) {
      (samples.filter(unsupportedNative) :+ measuredLine._1).foreach { sample =>
        withClue(sample.name) {
          val hex = org.locationtech.jts.io.WKBWriter.toHex(sample.wkb)
          assertNativeRejected(
            sparkSession.sql(s"SELECT ST_GeomFromWKB(unhex('$hex'))").collect())
        }
      }
    }
    val trailingXY = Sample("xy-after-measured", isoWkb(1, false, Seq(10, 20)))
    val baseSamples = if (native) supportedNativeSamples else samples
    val candidates = baseSamples.map(s => s -> s) ++ projectedMeasures ++ emptyMeasures ++
      (if (native) Seq(trailingXY -> trailingXY) else Seq(measuredLine, trailingXY -> trailingXY))
    val selected = if (allEmpty) candidates.filter { case (sample, _) =>
      sample.wkb == null || WKBReader.forDeclaredDimensions().read(sample.wkb).isEmpty
    }
    else candidates
    val input = selected.map(_._1)
    val expected = selected.map(_._2)
    val rows = input.zipWithIndex.map { case (sample, index) => Row(index, sample.wkb) }
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("wkb", BinaryType, nullable = true)))
    val path = new File(directory, "written").getPath
    val df = if (legacyUdt) {
      val legacyRows = input.zipWithIndex.map { case (sample, index) =>
        val geometry =
          if (sample.wkb == null) null
          else WKBReader.forDeclaredDimensions().read(sample.wkb)
        Row(index, geometry)
      }
      sparkSession.createDataFrame(legacyRows.asJava, legacySchema)
    } else {
      sparkSession
        .createDataFrame(rows.asJava, schema)
        .selectExpr("id", "ST_GeomFromWKB(wkb) AS geometry")
    }
    assertGeometries(df, input, "before write")
    df.coalesce(1).write.format("geoparquet").save(path)
    assertWrittenFile(path, expected, allEmpty)
    assertFile(path, expected, legacyUdt = legacyUdt)
  }

  /**
   * Geometries that a native Spark geometry value can hold and a JTS geometry cannot: an empty
   * multi-geometry or collection has no coordinates to carry its Z and M dimensions.
   */
  private def emptyMembers(order: ByteOrder): Seq[Sample] = Seq(
    Sample("empty-multipoint-m", isoWkb(4, false, order = order, m = true)),
    Sample("empty-multipolygon-zm", isoWkb(6, true, order = order, m = true)),
    Sample("empty-collection-z", isoWkb(7, true, order = order)),
    Sample(
      "collection-m-with-empty-member",
      isoWkb(
        7,
        false,
        children = Seq(
          isoWkb(4, false, order = order, m = true),
          isoWkb(1, false, Seq(1, 2, 4), order = order, m = true)),
        order = order,
        m = true)),
    Sample(
      "collection-z-with-empty-member",
      isoWkb(
        7,
        true,
        children =
          Seq(isoWkb(5, true, order = order), isoWkb(1, true, Seq(1, 2, 3), order = order)),
        order = order)))

  /** The geometry type codes in the statistics of the first row group of a file. */
  private def rowGroupTypes(file: File): Set[Int] = {
    val reader =
      ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI), new Configuration()))
    try {
      val chunk = reader.getFooter.getBlocks
        .get(0)
        .getColumns
        .asScala
        .find(_.getPath.toDotString == "geometry")
        .get
      // Reflection, because the Parquet library of Spark 3.5 and 4.0 has no such statistics
      val geospatial = "org.apache.parquet.column.statistics.geospatial."
      val statistics =
        classOf[ColumnChunkMetaData].getMethod("getGeospatialStatistics").invoke(chunk)
      val types = Class
        .forName(geospatial + "GeospatialStatistics")
        .getMethod("getGeospatialTypes")
        .invoke(statistics)
      Class
        .forName(geospatial + "GeospatialTypes")
        .getMethod("getTypes")
        .invoke(types)
        .asInstanceOf[java.util.Set[Integer]]
        .asScala
        .map(_.intValue)
        .toSet
    } finally reader.close()
  }

  describe("GeoParquet declared coordinate dimensions") {
    val representations = if (SpatialTypeSupport.usesNativeTypes) Seq(false, true) else Seq(false)
    for (legacyUdt <- representations; dictionary <- Seq(false, true)) {
      it(s"reads ISO WKB from a file (dictionary=$dictionary, legacyUdt=$legacyUdt)") {
        withDirectory { directory =>
          val fixture = new File(directory, "fixture.parquet")
          // Omitted CRS defaults to EPSG:4326, overriding embedded EWKB SRIDs.
          val embeddedSrid = ByteBuffer
            .allocate(33)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put(1.toByte)
            .putInt(0xa0000001)
            .putInt(3857)
            .putDouble(10)
            .putDouble(20)
            .putDouble(30)
            .array()
          val baseSamples =
            if (SpatialTypeSupport.usesNativeTypes && !legacyUdt) supportedNativeSamples
            else samples
          val values = baseSamples :+ Sample("embedded-srid", embeddedSrid)
          writeFixture(fixture, dictionary, values)
          assertFile(
            fixture.getPath,
            values,
            repetitions = 4,
            expectedSrid = 4326,
            legacyUdt = legacyUdt)
          if (SpatialTypeSupport.usesNativeTypes && !legacyUdt) {
            samples.filter(unsupportedNative).zipWithIndex.foreach { case (sample, index) =>
              withClue(sample.name) {
                val rejected = new File(directory, s"rejected-$index.parquet")
                writeFixture(rejected, dictionary, Seq(sample))
                assertNativeRejected(readFile(rejected.getPath, legacyUdt = false).collect())
              }
            }
          }
        }
      }
    }
    for (legacyUdt <- representations; allEmpty <- Seq(false, true)) {
      it(
        s"preserves XYZ, projects M, and writes matching metadata (allEmpty=$allEmpty, legacyUdt=$legacyUdt)") {
        withDirectory(directory => writeAndRoundTrip(directory, allEmpty, legacyUdt))
      }
    }

    // GeoParquet asks for the WKB of the OGC and ISO standards, where a geometry with Z has a
    // type code from 1001 to 1007. EWKB sets a flag bit in the type code instead.
    val order = ByteOrder.nativeOrder()
    val pointZ = isoWkb(1, true, Seq(1, 2, 3), order = order)
    val lineStringZ = isoWkb(2, true, Seq(1, 2, 3, 4, 5, 6), order = order)
    val isoSamples = Seq(
      Sample("point", isoWkb(1, false, Seq(1, 2), order = order)),
      Sample("point-z", pointZ),
      Sample("line-z", lineStringZ),
      Sample("multipoint-z", isoWkb(4, true, children = Seq(pointZ), order = order)),
      Sample("collection-z", isoWkb(7, true, children = Seq(pointZ, lineStringZ), order = order)))

    it("stores geometries as ISO WKB") {
      withDirectory { directory =>
        val path = new File(directory, "iso").getPath
        val rows = isoSamples.zipWithIndex.map { case (sample, index) => Row(index, sample.wkb) }
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, nullable = false),
            StructField("wkb", BinaryType, nullable = false)))
        sparkSession
          .createDataFrame(rows.asJava, schema)
          .selectExpr("id", "ST_GeomFromWKB(wkb) AS geometry")
          .coalesce(1)
          .write
          .format("geoparquet")
          .save(path)
        val stored = sparkSession.read.parquet(path).collect().sortBy(_.getInt(0))
        assert(stored.length == isoSamples.size)
        stored.zip(isoSamples).foreach { case (row, sample) =>
          withClue(s"${sample.name}: ") {
            assert(hex(row.getAs[Array[Byte]](1)) == hex(sample.wkb))
          }
        }
      }
    }

    it("stores geometries nested in another column as ISO WKB") {
      withDirectory { directory =>
        val path = new File(directory, "nested").getPath
        // Spark's native geometry type is only written at the root of the schema, so the nested
        // columns hold GeometryUDT values on every Spark version.
        val geometry = WKBReader.forDeclaredDimensions().read(pointZ)
        val holder = StructType(Seq(StructField("g", GeometryUDT())))
        val schema = StructType(
          Seq(
            StructField("geometry", GeometryUDT()),
            StructField("members", ArrayType(GeometryUDT())),
            StructField("holder", holder)))
        sparkSession
          .createDataFrame(Seq(Row(geometry, Seq(geometry), Row(geometry))).asJava, schema)
          .coalesce(1)
          .write
          .format("geoparquet")
          .save(path)
        // Read the stored bytes with the Parquet library: Spark's own reader takes the nested
        // values for serialized geometries.
        val file = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
        val reader = ParquetReader.builder(new GroupReadSupport(), new Path(file.toURI)).build()
        val record =
          try reader.read()
          finally reader.close()
        val member = record.getGroup("members", 0).getGroup("list", 0).getBinary("element", 0)
        assert(hex(record.getBinary("geometry", 0).getBytes) == hex(pointZ))
        assert(hex(member.getBytes) == hex(pointZ))
        assert(hex(record.getGroup("holder", 0).getBinary("g", 0).getBytes) == hex(pointZ))
      }
    }

    // GeoParquet 2.0 has M coordinates, which GeoParquet 1.1 has no place for. It needs the
    // GEOMETRY logical type, which the Parquet library has since 1.16.0.
    if (ParquetGeospatialTypes.supported) {
      for (legacyUdt <- representations) {
        it(s"keeps M coordinates in a GeoParquet 2.0 file (legacyUdt=$legacyUdt)") {
          withDirectory { directory =>
            val order = ByteOrder.nativeOrder()
            val measured = Seq(
              Sample("xym", isoWkb(1, false, Seq(10, 20, 99), order = order, m = true)),
              Sample("xyzm", isoWkb(1, true, Seq(10, 20, 30, 99), order = order, m = true)),
              Sample(
                "line-xym",
                isoWkb(2, false, Seq(1, 2, 90, 3, 4, 99), order = order, m = true)),
              Sample(
                "line-xyzm",
                isoWkb(2, true, Seq(1, 2, 3, 90, 3, 4, 5, 99), order = order, m = true)),
              Sample("empty-line-xym", isoWkb(2, false, order = order, m = true)),
              Sample("empty-polygon-xyzm", isoWkb(3, true, order = order, m = true)),
              Sample("xy", isoWkb(1, false, Seq(10, 20), order = order)),
              Sample("xyz", isoWkb(1, true, Seq(10, 20, 30), order = order))) ++
              (if (SpatialTypeSupport.usesNativeTypes && !legacyUdt) emptyMembers(order)
               else Seq.empty)
            val path = new File(directory, "measured").getPath
            val sedonaType = legacyUdt || !SpatialTypeSupport.usesNativeTypes
            val df = if (legacyUdt) {
              val rows = measured.zipWithIndex.map { case (sample, index) =>
                Row(index, WKBReader.forDeclaredDimensions().read(sample.wkb))
              }
              sparkSession.createDataFrame(rows.asJava, legacySchema)
            } else {
              val rows = measured.zipWithIndex.map { case (sample, index) =>
                Row(index, sample.wkb)
              }
              val schema = StructType(
                Seq(
                  StructField("id", IntegerType, nullable = false),
                  StructField("wkb", BinaryType, nullable = false)))
              sparkSession
                .createDataFrame(rows.asJava, schema)
                .selectExpr("id", "ST_GeomFromWKB(wkb) AS geometry")
            }
            val writer =
              df.coalesce(1).write.format("geoparquet").option("geoparquet.version", "2.0.0")
            // A column of Sedona's own geometry type has no SRID to take the CRS from
            (if (sedonaType) writer.option("geoparquet.crs", "null") else writer).save(path)

            val file = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
            val reader =
              ParquetReader.builder(new GroupReadSupport(), new Path(file.toURI)).build()
            val stored =
              try Iterator.continually(reader.read()).takeWhile(_ != null).toList
              finally reader.close()
            assert(stored.size == measured.size)
            stored.sortBy(_.getInteger("id", 0)).zip(measured).foreach { case (record, sample) =>
              withClue(s"${sample.name}: ") {
                assert(hex(record.getBinary("geometry", 0).getBytes) == hex(sample.wkb))
              }
            }
            implicit val formats: org.json4s.Formats = DefaultFormats
            val metadata = columnMetadata(file)
            val layouts = Seq("", " Z", " M", " ZM")
            val kinds = Seq(
              "Point",
              "LineString",
              "Polygon",
              "MultiPoint",
              "MultiLineString",
              "MultiPolygon",
              "GeometryCollection")
            def typeName(code: Int): String = kinds(code % 1000 - 1) + layouts(code / 1000)
            assert(
              (metadata \ "geometry_types").extract[Seq[String]].toSet ==
                measured.map(sample => typeName(typeCode(sample.wkb))).toSet)
            // The Parquet library takes the types of the row-group statistics from the
            // geometries that have coordinates.
            val withCoordinates =
              measured.filterNot(s => WKBReader.forDeclaredDimensions().read(s.wkb).isEmpty)
            assert(rowGroupTypes(file) == withCoordinates.map(s => typeCode(s.wkb)).toSet)
            // The bounds of X, Y, Z and M, first the lower ones and then the upper ones. The M
            // bounds cover the XYM geometries too. A member of a collection of the native
            // column has the lowest M.
            val lowestM = if (SpatialTypeSupport.usesNativeTypes && !legacyUdt) 4 else 90
            assert(
              (metadata \ "bbox").extract[Seq[Double]] ==
                Seq(1, 2, 3, lowestM, 10, 20, 30, 99))
            assertFile(path, measured, legacyUdt = legacyUdt)
            if (SpatialTypeSupport.usesNativeTypes && !legacyUdt) {
              // The native values hold the stored WKB, empty members included
              val read = readFile(path, legacyUdt = false)
                .selectExpr("id", "hex(ST_AsBinary(geometry))")
                .collect()
                .sortBy(_.getInt(0))
              read.zip(measured).foreach { case (row, sample) =>
                withClue(s"${sample.name}, read: ") {
                  assert(row.getString(1) == hex(sample.wkb))
                }
              }
            }
          }
        }
      }
    }

    // The bbox of a GeoParquet 2.0 column lists the lower bounds of its dimensions and then the
    // upper bounds: X and Y, then Z, then M. It has no form with M bounds and without Z bounds.
    val nan = Double.NaN
    val xy = isoWkb(1, false, Seq(10, 20))
    val boundsCases = Seq[(String, String, Seq[Array[Byte]], Seq[Double])](
      (
        "the X and Y bounds of XYM geometries",
        "2.0.0",
        Seq(isoWkb(1, false, Seq(1, 2, 4), m = true), xy),
        Seq(1, 2, 10, 20)),
      (
        "the X and Y bounds of XYM geometries with an infinite M coordinate",
        "2.0.0",
        Seq(isoWkb(1, false, Seq(1, 2, Double.PositiveInfinity), m = true), xy),
        Seq(1, 2, 10, 20)),
      (
        "the X and Y bounds of XYZ geometries with an infinite Z coordinate in a GeoParquet 1.1 file",
        "1.1.0",
        Seq(isoWkb(1, true, Seq(1, 2, Double.PositiveInfinity)), xy),
        Seq(1, 2, 10, 20)),
      (
        "the Z bounds of XYZ geometries",
        "2.0.0",
        Seq(isoWkb(1, true, Seq(1, 2, 7)), isoWkb(1, true, Seq(3, 4, -5)), xy),
        Seq(1, 2, -5, 10, 20, 7)),
      (
        "the Z and M bounds of XYZ and XYM geometries",
        "2.0.0",
        Seq(isoWkb(1, true, Seq(1, 2, 7)), isoWkb(1, false, Seq(3, 4, 60), m = true), xy),
        Seq(1, 2, 7, 60, 10, 20, 7, 60)),
      (
        "the Z bounds without the Z coordinates that are NaN",
        "2.0.0",
        Seq(isoWkb(2, true, Seq(1, 2, nan, 3, 4, 5)), xy),
        Seq(1, 2, 5, 10, 20, 5)),
      (
        "the X and Y bounds of XYZ geometries without a Z coordinate that is a number",
        "2.0.0",
        Seq(isoWkb(1, true, Seq(1, 2, nan)), xy),
        Seq(1, 2, 10, 20)),
      (
        "the X and Y bounds of XYZM geometries without a Z coordinate that is a number",
        "2.0.0",
        Seq(isoWkb(1, true, Seq(1, 2, nan, 60), m = true), xy),
        Seq(1, 2, 10, 20)),
      (
        "the X, Y and Z bounds of XYZM geometries without an M coordinate that is a number",
        "2.0.0",
        Seq(isoWkb(1, true, Seq(1, 2, 7, nan), m = true), xy),
        Seq(1, 2, 7, 10, 20, 7)),
      (
        "the X and Y bounds of XYZ geometries in a GeoParquet 1.1 file",
        "1.1.0",
        Seq(isoWkb(1, true, Seq(1, 2, 7)), xy),
        Seq(1, 2, 10, 20)))
    def columnMetadataOf(version: String, geometries: Seq[Array[Byte]])(
        check: (JValue, String) => Unit): Unit = withDirectory { directory =>
      val path = new File(directory, "bounds").getPath
      val rows = geometries.zipWithIndex.map { case (wkb, index) =>
        Row(index, WKBReader.forDeclaredDimensions().read(wkb))
      }
      sparkSession
        .createDataFrame(rows.asJava, legacySchema)
        .coalesce(1)
        .write
        .format("geoparquet")
        .option("geoparquet.version", version)
        .option("geoparquet.crs", "null")
        .save(path)
      val file = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
      check(columnMetadata(file), path)
    }
    for ((name, version, geometries, expected) <- boundsCases
      if version == "1.1.0" || ParquetGeospatialTypes.supported) {
      it(s"writes $name") {
        columnMetadataOf(version, geometries) { (metadata, _) =>
          implicit val formats: org.json4s.Formats = DefaultFormats
          assert((metadata \ "bbox").extract[Seq[Double]] == expected)
        }
      }
    }

    // JSON has no number for a bound that is infinite or NaN, and the bbox has to state the range
    // of every dimension that it covers, so such a column has no bbox.
    val infinity = Double.PositiveInfinity
    val unboundedCases = Seq[(String, String, Seq[Array[Byte]])](
      (
        "an M coordinate is infinite",
        "2.0.0",
        Seq(isoWkb(1, true, Seq(1, 2, 7, infinity), m = true), xy)),
      ("a Z coordinate is infinite", "2.0.0", Seq(isoWkb(1, true, Seq(1, 2, -infinity)), xy)),
      ("an X coordinate is infinite", "2.0.0", Seq(isoWkb(1, false, Seq(infinity, 2)), xy)),
      ("a Y coordinate is infinite", "1.1.0", Seq(isoWkb(1, false, Seq(1, -infinity)), xy)),
      ("a Y coordinate is NaN", "1.1.0", Seq(isoWkb(1, false, Seq(1, nan)), xy)))
    for ((name, version, geometries) <- unboundedCases
      if version == "1.1.0" || ParquetGeospatialTypes.supported) {
      it(s"writes no bbox to a GeoParquet $version file when $name") {
        columnMetadataOf(version, geometries) { (metadata, path) =>
          assert(metadata \ "bbox" == JNothing)
          val read = readFile(path, legacyUdt = true)
          assert(read.count() == geometries.size)
          assert(read.where("ST_Intersects(geometry, ST_Point(10, 20))").count() == 1)
        }
      }
    }

    it("reads a geometry value and the WKB that it holds in one step") {
      val dataType = SpatialTypeSupport.geometryType(4326)
      val wkb = isoWkb(2, true, Seq(1, 2, 3, 4, 5, 6, 7, 8), m = true)
      val value = SpatialTypeSupport.serializeGeometry(
        WKBReader.forDeclaredDimensions(4326).read(wkb),
        dataType)
      val row = new GenericInternalRow(Array[Any](value))
      val (geometry, held) =
        SpatialTypeSupport.readGeometryWithWkb(row, 0, dataType, ByteOrder.LITTLE_ENDIAN)
      val expected = SpatialTypeSupport.readGeometry(row, 0, dataType)
      assert(GeometryEquality.equalsIdentical(geometry, expected))
      assert(geometry.getSRID == 4326)
      // Only a native value holds WKB
      assert(held.map(hex) == (if (SpatialTypeSupport.usesNativeTypes) Some(hex(wkb)) else None))
    }

    it("keeps the coordinate layout of each member of a geometry collection") {
      withDirectory { directory =>
        // The OGC grammar asks for Z members throughout a collection with a Z coordinate. A
        // geometry can hold members of both layouts, and the writer stores each one as it is.
        val order = ByteOrder.nativeOrder()
        val mixedSamples = Seq(
          Sample(
            "members",
            isoWkb(
              7,
              true,
              children = Seq(
                isoWkb(2, false, Seq(0, 0, 1, 1), order = order),
                isoWkb(1, true, Seq(1, 2, 3), order = order)),
              order = order)),
          Sample(
            "empty-members",
            isoWkb(
              7,
              true,
              children = Seq(isoWkb(2, false, order = order), isoWkb(3, true, order = order)),
              order = order)))
        val schema = StructType(Seq(StructField("geometry", GeometryUDT())))
        mixedSamples.foreach { sample =>
          val path = new File(directory, sample.name).getPath
          val geometry = WKBReader.forDeclaredDimensions().read(sample.wkb)
          sparkSession
            .createDataFrame(Seq(Row(geometry)).asJava, schema)
            .coalesce(1)
            .write
            .format("geoparquet")
            .save(path)
          val file = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
          val reader = ParquetReader.builder(new GroupReadSupport(), new Path(file.toURI)).build()
          val record =
            try reader.read()
            finally reader.close()
          withClue(s"${sample.name}: ") {
            assert(hex(record.getBinary("geometry", 0).getBytes) == hex(sample.wkb))
          }
        }
      }
    }
  }
}
