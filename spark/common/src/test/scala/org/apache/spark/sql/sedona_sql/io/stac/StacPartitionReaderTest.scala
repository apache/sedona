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
package org.apache.spark.sql.sedona_sql.io.stac

import org.apache.hadoop.conf.Configuration
import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.SerializableConfiguration

import scala.jdk.CollectionConverters._

class StacPartitionReaderTest extends TestBaseScala {

  val TEST_DATA_FOLDER: String =
    System.getProperty("user.dir") + "/src/test/resources/datasource_stac"
  val JSON_STAC_ITEM_SIMPLE: String = s"file://$TEST_DATA_FOLDER/simple-item.json"
  val JSON_STAC_ITEM_CORE: String = s"file://$TEST_DATA_FOLDER/core-item.json"
  val JSON_STAC_ITEM_EXTENDED: String = s"file://$TEST_DATA_FOLDER/extended-item.json"
  val JSON_STAC_ITEM_FEATURES: String = s"file://$TEST_DATA_FOLDER/collection-items.json"
  val MOCK_STAC_ITEM_FEATURES: String =
    StacTestUtils.getFileUrlOfResource("stac/items/sentinel-2-items.json")

  it("StacPartitionReader should read feature files from local files") {
    val jsonFiles =
      Seq(JSON_STAC_ITEM_SIMPLE, JSON_STAC_ITEM_CORE, JSON_STAC_ITEM_EXTENDED).toArray
    val partition = StacPartition(0, jsonFiles, Map.empty[String, String].asJava)
    val reader =
      new StacPartitionReader(
        sparkSession.sparkContext.broadcast(new SerializableConfiguration(new Configuration())),
        partition,
        StacTable.SCHEMA_V1_1_0,
        Map.empty[String, String],
        None,
        None)

    assert(reader.next())
    (1 to 3).foreach { i =>
      val row: InternalRow = reader.get()
      assert(row != null)
      assert(reader.next() == (i < 3))
    }

    reader.close()
  }

  it("StacPartitionReader should read features collection file from local files") {
    val jsonFiles = Seq(JSON_STAC_ITEM_FEATURES).toArray
    val partition = StacPartition(0, jsonFiles, Map.empty[String, String].asJava)
    val reader =
      new StacPartitionReader(
        sparkSession.sparkContext.broadcast(new SerializableConfiguration(new Configuration())),
        partition,
        StacTable.SCHEMA_V1_1_0,
        Map.empty[String, String],
        None,
        None)

    assert(reader.next())
    (1 to 10).foreach { i =>
      val row: InternalRow = reader.get()
      assert(row != null)
      assert(reader.next() == (i < 10))
    }

    reader.close()
  }

  it("StacPartitionReader should read features collection file from mock endpoint") {
    val jsonFiles = Seq(MOCK_STAC_ITEM_FEATURES).toArray
    val partition = StacPartition(0, jsonFiles, Map.empty[String, String].asJava)
    val reader =
      new StacPartitionReader(
        sparkSession.sparkContext.broadcast(new SerializableConfiguration(new Configuration())),
        partition,
        StacTable.SCHEMA_V1_1_0,
        Map.empty[String, String],
        None,
        None)

    assert(reader.next())
    (1 to 10).foreach { i =>
      val row: InternalRow = reader.get()
      assert(row != null)
      assert(reader.next() == (i < 10))
    }

    reader.close()
  }
}
