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

import org.apache.sedona.common.geometryObjects.Box2D
import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.UDT.Box2DUDT
import org.apache.spark.sql.types.{DataType, IntegerType, StructType, UDTRegistration}
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfter

class Box2DUDTSuite extends TestBaseScala with BeforeAndAfter {

  val tempFolder: TemporaryFolder = new TemporaryFolder

  before {
    tempFolder.create()
  }

  after {
    tempFolder.delete()
  }

  describe("Box2DUDT") {
    it("registers Box2D via UdtRegistratorWrapper") {
      assert(UDTRegistration.exists(classOf[Box2D].getName))
    }

    it("renders and parses a JSON schema round-trip") {
      val schema = new StructType().add("box", new Box2DUDT())
      assert(DataType.fromJson(schema.json).asInstanceOf[StructType] == schema)
    }

    it("serializes and deserializes a Box2D round-trip") {
      val udt = new Box2DUDT()
      val box = new Box2D(-10.0, -20.0, 30.0, 40.0)
      assert(udt.deserialize(udt.serialize(box)) == box)
    }

    it("case object equals a fresh instance") {
      val instance = new Box2DUDT()
      assert(Box2DUDT == Box2DUDT)
      assert(instance.equals(instance))
      assert(instance.equals(Box2DUDT))
      assert(Box2DUDT.equals(instance))
      assert(instance.hashCode() == Box2DUDT.hashCode())
    }

    it("writes and reads a Box2D column via Parquet") {
      val box = new Box2D(1.0, 2.0, 3.0, 4.0)
      val schema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("bbox", new Box2DUDT(), nullable = false)
      val rdd = sparkSession.sparkContext.parallelize(Seq(Row(1, box)))
      val df = sparkSession.createDataFrame(rdd, schema)

      val path = tempFolder.getRoot.getPath + "/box2d-parquet"
      df.write.parquet(path)

      val read = sparkSession.read.parquet(path)
      val row = read.collect()(0)
      assert(row.getAs[Int]("id") == 1)
      assert(row.getAs[Box2D]("bbox") == box)
    }
  }
}
