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

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files
import java.util.Collections
import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.apache.sedona.common.utils.GeometryEquality
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
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
      expectedSrid: Int = 0): Unit = {
    val df = sparkSession.read.format("geoparquet").load(path)
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
          val actual = GeometryUDT.deserialize(row.getBinary(1))
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

  private def writeAndRoundTrip(directory: File, allEmpty: Boolean): Unit = {
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
    val trailingXY = Sample("xy-after-measured", isoWkb(1, false, Seq(10, 20)))
    val candidates = samples.map(s => s -> s) ++ projectedMeasures ++ emptyMeasures ++
      Seq(measuredLine, trailingXY -> trailingXY)
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
    val df = sparkSession
      .createDataFrame(rows.asJava, schema)
      .selectExpr("id", "ST_GeomFromWKB(wkb) AS geometry")
    assertGeometries(df, input, "before write")
    df.coalesce(1).write.format("geoparquet").save(path)
    assertWrittenFile(path, expected, allEmpty)
    assertFile(path, expected)
  }

  describe("GeoParquet declared coordinate dimensions") {
    for (dictionary <- Seq(false, true)) {
      it(s"reads ISO WKB from a file (dictionary=$dictionary)") {
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
          val values = samples :+ Sample("embedded-srid", embeddedSrid)
          writeFixture(fixture, dictionary, values)
          assertFile(fixture.getPath, values, repetitions = 4, expectedSrid = 4326)
        }
      }
    }
    for (allEmpty <- Seq(false, true)) {
      it(s"preserves XYZ, projects M, and writes matching metadata (allEmpty=$allEmpty)") {
        withDirectory(directory => writeAndRoundTrip(directory, allEmpty))
      }
    }
  }
}
