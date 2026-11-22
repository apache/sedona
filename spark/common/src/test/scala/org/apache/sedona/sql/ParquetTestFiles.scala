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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files

import org.apache.parquet.format.{Util => ParquetFormatUtil}

/** Changes to Parquet files that the tests of the GeoParquet reader need. */
object ParquetTestFiles {

  /** Leaves the schema of a Parquet file readable, and its row groups unreadable. */
  def damageRowGroups(file: File): Unit = {
    val bytes = Files.readAllBytes(file.toPath)
    val length =
      ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt
    val start = bytes.length - 8 - length
    val metadata =
      ParquetFormatUtil.readFileMetaData(new ByteArrayInputStream(bytes, start, length))
    // The first column chunk now belongs to a column that the schema does not have
    val chunk = metadata.getRow_groups.get(0).getColumns.get(0)
    chunk.getMeta_data.setPath_in_schema(java.util.Arrays.asList("missing"))
    val damaged = new ByteArrayOutputStream()
    damaged.write(bytes, 0, start)
    ParquetFormatUtil.writeFileMetaData(metadata, damaged)
    val newLength = damaged.size - start
    damaged.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(newLength).array)
    damaged.write(bytes, bytes.length - 4, 4)
    Files.write(file.toPath, damaged.toByteArray)
    // The checksum that Hadoop keeps next to the file is for the bytes before
    new File(file.getParentFile, s".${file.getName}.crc").delete()
  }
}
