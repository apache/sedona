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
package org.apache.sedona.sql.datasources.osm

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.sedona.sql.datasources.osmpbf.{HeaderFinder, StartEndStream}
import org.apache.sedona.sql.datasources.osmpbf.iterators.PbfIterator
import org.apache.sedona.sql.datasources.osmpbf.model.OSMEntity
import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.net.URI
import java.util
import scala.collection.convert.ImplicitConversions.`iterator asScala`

case class OsmPartitionReader(
    sparkSession: SparkSession,
    requiredSchema: StructType,
    broadcastedHadoopConf: Broadcast[SerializableWritable[Configuration]],
    HEADER_SIZE_LENGTH: Int = 4)
    extends (PartitionedFile => Iterator[InternalRow]) {
  override def apply(file: PartitionedFile): Iterator[InternalRow] = {
    val path = new Path(new URI(file.filePath.toString()))
    val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
    val status = fs.getFileStatus(path)
    val f = fs.open(status.getPath)

    val offset = findOffset(fs, status, file.start)

    f.seek(file.start + offset)

    new PbfIterator(new StartEndStream(f, (file.length - offset) + HEADER_SIZE_LENGTH)).map(
      record => resolveEntity(record, requiredSchema))
  }

  def findOffset(fs: FileSystem, status: FileStatus, start: Long): Long = {
    val pbfIS = fs.open(status.getPath)

    try {
      pbfIS.seek(start)

      new HeaderFinder(pbfIS).find()
    } finally {
      if (pbfIS != null) pbfIS.close()
    }
  }

  private def resolveEntity(entity: OSMEntity, schema: StructType): InternalRow = {
    InternalRow.fromSeq(schema.map(field => {
      field.name match {
        case "id" => entity.getId
        case "kind" => UTF8String.fromString(entity.getKind)
        case "location" =>
          if (entity.getLongitude != null)
            InternalRow.fromSeq(Seq(entity.getLongitude, entity.getLatitude))
          else null
        case "tags" => transformTags(entity.getTags)
        case "refs" => if (entity.getRefs != null) ArrayData.toArrayData(entity.getRefs) else null
        case "ref_roles" =>
          if (entity.getRefRoles != null)
            ArrayData.toArrayData(entity.getRefRoles.map(x => UTF8String.fromString(x)))
          else null
        case "ref_types" =>
          if (entity.getRefTypes != null)
            ArrayData.toArrayData(entity.getRefTypes.map(x => UTF8String.fromString(x)))
          else null
      }
    }))
  }

  def transformTags(tags: util.Map[String, String]): ArrayBasedMapData = {
    var keys = Seq().map(UTF8String.fromString)
    var values = Seq().map(UTF8String.fromString)

    tags.forEach((k, v) => {
      keys :+= UTF8String.fromString(k)
      values :+= UTF8String.fromString(v)
    })

    val keyArray = ArrayData.toArrayData(Array(keys: _*))

    val valArray = ArrayData.toArrayData(Array(values: _*))

    new ArrayBasedMapData(keyArray, valArray)
  }
}
